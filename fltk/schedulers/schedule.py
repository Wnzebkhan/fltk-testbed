import logging
import time
from typing import List
from queue import PriorityQueue

import numpy as np
from kubeflow.pytorchjob import PyTorchJobClient

from fltk.job_prediction.workload_predictor import JobWorkloadPredictor
from fltk.util.cluster.client import construct_job
from fltk.util.config import BareConfig
from fltk.util.task.task import ArrivalTask


class Job:
    def __init__(self, group_id: str, created: int, started: int, busy_time: int):
        self.group_id = group_id
        self.created = created
        self.started = started
        self.busy_time = busy_time


class Schedule:
    def __init__(self, client: PyTorchJobClient, config: BareConfig, workload_predictor: JobWorkloadPredictor):
        self.__logger = logging.getLogger('Orchestrator')
        self.__client = client
        self._config = config
        self.workload_predictor = workload_predictor
        self.start_time = 0
        self.end_time = 0
        self.pipelines: List[List[Job]] = []

        # Priority queue, requires an orderable object, otherwise a Tuple[int, Any] can be used to insert.
        self.pending_tasks: "PriorityQueue[ArrivalTask]" = PriorityQueue()
        self.deployed_tasks: List[ArrivalTask] = []
        self.completed_tasks: List[str] = []

    def reschedule(self):
        if self._config.experiment.scheduler == "random":
            self.random_scheduler()
        if self._config.experiment.scheduler == "fifo":
            self.fifo_scheduler()
        if self._config.experiment.scheduler == "fair":
            self.fair_scheduler()

    def random_scheduler(self):
        for task in self.pending_tasks.queue:
            task.priority = np.random.randint(0, 100)

    def fifo_scheduler(self):
        # TODO
        pass

    def fair_scheduler(self):
        # self.taint_free = dict()
        # for task in self.deployed_tasks:
        #     self.taint_free[task.taint] = task.started + task.predicted_length
        # TODO make use of taints

        group_delays = self.calculate_group_delays()

        for task in self.pending_tasks.queue:
            task.priority = group_delays[task.group_id] + (round(time.time() * 1000) - task.created)

    def deploy_tasks(self):
        # TODO while there is place and the pending tasks are not empty
        while not self.pending_tasks.empty():
            curr_task = self.pending_tasks.get()
            self.__logger.info(f"Scheduling arrival of Arrival: {curr_task.id}")
            job_to_start = construct_job(self._config, curr_task)

            # Hack to overcome limitation of KubeFlow version (Made for older version of Kubernetes)
            self.__logger.info(f"Deploying on cluster: {curr_task.id}")

            outputs = self.__client.create(job_to_start, namespace=self._config.cluster_config.namespace)
            print(outputs)
            self.__logger.info(outputs)
            # TODO no clue what outputs contains since i could not run it but i assume it contains some kind of timing sooooo

            self.deployed_tasks.append(curr_task)

    def check_completed(self):
        for deployed in self.deployed_tasks:
            # self.workload_predictor.feedback(curr_task, outputs['timing'])
            # TODO update the workload predictor with the timings

            # TODO put the completed tasks in the pipelines array
            pass

    # schedule should be a list of lists
    def calculate_utilization(self):
        total_time = self.end_time - self.start_time
        average_utilization = 0
        for pipeline in self.pipelines:
            busy_time = 0
            for job in pipeline:
                busy_time += job.busy_time

            average_utilization += busy_time / total_time

        return average_utilization / len(self.pipelines)

    def calculate_fairness(self):
        """
        Calculates the fairness which we defined as the variance between the total delay's of groups
        @return:
        """
        group_delays = self.calculate_group_delays()

        avg = sum(group_delays.values()) / len(group_delays.values())
        std = 0
        for delay in group_delays.values():
            std += (delay - avg) ** 2

        return std / len(group_delays.values())

    def calculate_group_delays(self):
        group_delays = dict()
        for pipeline in self.pipelines:
            for job in pipeline:
                if job.group_id not in group_delays:
                    group_delays[job.group_id] = 0
                # calculate the delay
                group_delays[job.group_id] += job.started - job.created
        return group_delays
