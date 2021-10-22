import logging
import math
import time
from typing import List, Tuple
from queue import PriorityQueue

import numpy as np
from kubeflow.pytorchjob import PyTorchJobClient

from fltk.job_prediction.workload_predictor import JobWorkloadPredictor
from fltk.util.cluster.client import construct_job
from fltk.util.config import BareConfig
from fltk.util.task.task import ArrivalTask
from dateutil import parser


class Job:
    def __init__(self, id: str, group_id: str, created: int, started: int, busy_time: int):
        self.id = id
        self.group_id = group_id
        self.created = created
        self.started = started
        self.busy_time = busy_time


class Schedule:
    def __init__(self, client: PyTorchJobClient, config: BareConfig, workload_predictor: JobWorkloadPredictor):
        self.__logger = logging.getLogger('Scheduler')
        self.__client = client
        self._config = config
        self.workload_predictor = workload_predictor
        self.start_time = round(time.time() * 1000)

        self.pending_tasks: List[ArrivalTask] = []
        self.deployed_tasks: List[Tuple[int, ArrivalTask]] = []
        self.completed_tasks: List[str] = []

        self.history: List[List[Job]] = []
        self.schedule: List[List[ArrivalTask]] = []
        self.pipeline_busy: List[bool] = []
        self.n_pipelines = config.experiment.pipelines
        for i in range(self.n_pipelines):
            self.history.append([])
            self.schedule.append([])
            self.pipeline_busy.append(False)

        self.task_on_pipeline = dict()

    def reschedule(self):
        self.schedule = [[] for _ in range(self.n_pipelines)]

        if self._config.experiment.scheduler == "random":
            self.random_scheduler()
        if self._config.experiment.scheduler == "fifo":
            self.fifo_scheduler()
        if self._config.experiment.scheduler == "fair":
            self.fair_scheduler()

    def random_scheduler(self):
        for task in self.pending_tasks:
            self.schedule[np.random.randint(0, self.n_pipelines)].append(task)

    def fifo_scheduler(self):
        lengths = PriorityQueue()

        for pipe in range(self.n_pipelines):
            # length, pipe
            lengths.put((0, pipe))

        for task in self.pending_tasks:
            length, pipe = lengths.get()
            self.schedule[pipe].append(task)
            lengths.put((length + task.predicted_length, pipe))

    def fair_scheduler(self):
        group_delays = self.calculate_group_delays()
        task_order = PriorityQueue()

        for task in self.pending_tasks:
            delay = 1

            if task.group_id in group_delays:
                delay = 1 / group_delays[task.group_id]

            task_order.put((delay, task))

        lengths = PriorityQueue()

        for pipe in range(self.n_pipelines):
            # length, pipe
            lengths.put((0, pipe))

        while not task_order.empty():
            delay, task = task_order.get()
            length, pipe = lengths.get()
            self.schedule[pipe].append(task)
            lengths.put((length + task.predicted_length, pipe))

    def deploy_tasks(self):
        # check per pipe
        for i, pipe in enumerate(self.schedule):
            # if there is stuff scheduled in this pipe and it is not busy we deploy the job
            if len(pipe) != 0 and not self.pipeline_busy[i]:
                first = pipe.pop()

                for x, task in enumerate(self.pending_tasks):
                    if task.id == first.id:
                        del self.pending_tasks[x]

                self.__logger.info(f"Scheduling arrival of Arrival: {first.task_id} -> {first.id}")
                job_to_start = construct_job(self._config, first)

                # Hack to overcome limitation of KubeFlow version (Made for older version of Kubernetes)
                self.__logger.info(f"Deploying on cluster: {first.task_id} -> {first.id}")

                self.__client.create(job_to_start, namespace=self._config.cluster_config.namespace)

                self.deployed_tasks.append((i, first))
                self.history[i].append(
                        Job(f'{first.id}',
                        first.group_id,
                        first.created,
                        round(time.time() * 1000), # job started now
                        first.predicted_length)
                )

                self.__logger.info(f"{first.id} :::: {first.created} -> {round(time.time() * 1000)}, diff = {round(time.time() * 1000) - first.created}")
                self.pipeline_busy[i] = True

    def check_completed(self):
        for deployed in self.deployed_tasks:
            pipe, task = deployed
            job = self.__client.get(f"trainjob-{task.id}", namespace=self._config.cluster_config.namespace)

            # check the status of the job
            if 'status' in job and len(job['status']['conditions']) and (job['status']['conditions'][-1]['type'].lower() == 'succeeded' or job['status']['conditions'][-1]['type'].lower() == 'failed'):
                # job is done
                self.__logger.info(f'Job done: {task.id}')
                self.deployed_tasks.remove(deployed)
                self.completed_tasks.append(f"trainjob-{task.id}")
                start_time = job['status']['startTime']
                end_time = job['status']['conditions'][-1]['lastTransitionTime']
                start_time = parser.parse(start_time).timestamp()
                end_time = parser.parse(end_time).timestamp()
                length = (end_time - start_time) * 1000 # convert to ms

                # update the workload predictor with the timings
                self.workload_predictor.feedback(task, int(length))
                # update the history schedule with the timings
                self.history[pipe][-1].busy_time = length
                # update the busy variable of the pipe
                self.pipeline_busy[pipe] = False

    def calculate_utilization(self):
        total_time = round(time.time() * 1000) - self.start_time
        average_utilization = 0
        for pipeline in self.history:
            busy_time = 0
            for job in pipeline:
                busy_time += job.busy_time

            average_utilization += busy_time / total_time

        return average_utilization / len(self.schedule)

    def calculate_fairness(self):
        """
        Calculates the fairness which we defined as the variance between the total delay's of groups
        @return:
        """
        group_delays = self.calculate_group_delays()
        delay_in_secs = dict()

        for key, value in group_delays.items():
            delay_in_secs[key] = value / 1000

        avg = sum(delay_in_secs.values()) / len(delay_in_secs.values())
        self.__logger.info(f"Average delays: {avg}")
        std = 0
        for delay in delay_in_secs.values():
            std += (delay - avg) ** 2
        self.__logger.info(f"Standard deviation delays: {(std / len(delay_in_secs.values()))}")

        return std / len(delay_in_secs.values())

    def calculate_group_delays(self):
        group_delays = dict()
        for pipeline in self.history:
            for job in pipeline:
                if job.group_id not in group_delays:
                    group_delays[job.group_id] = 0
                # calculate the delay
                group_delays[job.group_id] += job.started - job.created

        # calculate delays in current pipeline
        for i, pipeline in enumerate(self.schedule):
            start_next = 0
            # calculate the moment the previous job will likely end
            if len(self.history[i]):
                start_next = self.history[i][-1].started + self.history[i][-1].busy_time
            for job in pipeline:
                if job.group_id not in group_delays:
                    group_delays[job.group_id] = 0
                # calculate the delay which is the predicted start time of this job which is the moment the previous job will likely end
                group_delays[job.group_id] += start_next - job.created
                # Add the predicted length to the start_next such that delay of the next job is also correct
                start_next += job.predicted_length
        return group_delays
