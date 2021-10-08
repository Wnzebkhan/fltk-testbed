import logging
import time
import uuid
from queue import PriorityQueue
from typing import List
import dropbox
import csv

from kubeflow.pytorchjob import PyTorchJobClient
from kubeflow.pytorchjob.constants.constants import PYTORCHJOB_GROUP, PYTORCHJOB_VERSION, PYTORCHJOB_PLURAL
from kubernetes import client

from fltk.job_prediction.workload_predictor import JobWorkloadPredictor
from fltk.schedulers.schedule import Schedule
from fltk.util.cluster.client import construct_job, ClusterManager
from fltk.util.config.base_config import BareConfig
from fltk.util.task.generator.arrival_generator import ArrivalGenerator, Arrival
from fltk.util.task.task import ArrivalTask


class Orchestrator(object):
    """
    Central component of the Federated Learning System: The Orchestrator

    The Orchestrator is in charge of the following tasks:
    - Running experiments
        - Creating and/or managing tasks
        - Keep track of progress (pending/started/failed/completed)
    - Keep track of timing

    Note that the Orchestrator does not function like a Federator, in the sense that it keeps a central model, performs
    aggregations and keeps track of Clients. For this, the KubeFlow PyTorch-Operator is used to deploy a train task as
    a V1PyTorchJob, which automatically generates the required setup in the cluster. In addition, this allows more Jobs
    to be scheduled, than that there are resources, as such, letting the Kubernetes Scheduler let decide when to run
    which containers where.
    """
    _alive = False

    def __init__(self, cluster_mgr: ClusterManager, arv_gen: ArrivalGenerator, config: BareConfig):
        self.__logger = logging.getLogger('Orchestrator')
        self.__logger.debug("Loading in-cluster configuration")
        self.__cluster_mgr = cluster_mgr
        self.__arrival_generator = arv_gen
        self._config = config

        # API to interact with the cluster.
        self.__client = PyTorchJobClient()

        self.workload_predictor = JobWorkloadPredictor()
        self.schedule = Schedule(self.__client, self._config, self.workload_predictor)

    def stop(self) -> None:
        """
        Stop the Orchestrator.
        @return:
        @rtype:
        """
        self.__logger.info("Received stop signal for the Orchestrator.")
        self._alive = False

    def run(self, clear: bool = True) -> None:
        """
        Main loop of the Orchestartor.
        @param clear: Boolean indicating whether a previous deployment needs to be cleaned up (i.e. lingering jobs that
        were deployed by the previous run).

        @type clear: bool
        @return: None
        @rtype: None
        """
        self._alive = True
        start_time = time.time()
        if clear:
            self.__clear_jobs()
        while self._alive and time.time() - start_time < self._config.get_duration():
            # 1. Check arrivals
            # If new arrivals, store them in arrival list
            while not self.__arrival_generator.arrivals.empty():
                arrival: Arrival = self.__arrival_generator.arrivals.get()
                unique_identifier: uuid.UUID = uuid.uuid4()
                task = ArrivalTask(priority=arrival.get_priority(),
                                   id=unique_identifier,
                                   network=arrival.get_network(),
                                   dataset=arrival.get_dataset(),
                                   sys_conf=arrival.get_system_config(),
                                   param_conf=arrival.get_parameter_config(),
                                   group_id=arrival.group_id,
                                   task_id=arrival.task_id,
                                   created=round(time.time() * 1000),
                                   predicted_length=self.workload_predictor.predict_length(arrival))

                self.__logger.info(f"Arrival of: {task.task_id} {unique_identifier}")
                self.schedule.pending_tasks.append(task)

            self.schedule.reschedule()

            self.schedule.deploy_tasks()

            self.schedule.check_completed()

            if len(self.schedule.completed_tasks) == self._config.experiment.number_of_groups * self._config.experiment.number_of_jobs_per_group:
                self._alive = False

            self.__logger.debug("Still alive...")
            time.sleep(1)


        logging.info(f'Experiment completed, currently does not support waiting.')

        # with open('./logging/statistics.csv', 'a+') as f:
        #     f.write(f'{self._config.experiment.scheduler} ; {self._config.experiment.cpu_per_job} ; {self._config.experiment.memory_per_job} ; {self._config.experiment.number_of_groups}  ; {self._config.experiment.number_of_jobs_per_group} ; {self._config.experiment.scheduler} ; {self.schedule.calculate_fairness()} ; {self.schedule.calculate_utilization()}')

        dpbx = dropbox.Dropbox("x8KMxPF9z50AAAAAAAAAAXrTe1JWjuMJ-vYm8OnFAGJeHfPpy5HndfMrhwnij8os")
        header = ['scheduler', 'pipeline', 'number_of_groups', 'jobs_per_group', 'fairness', 'utilization']
        data = [self._config.experiment.scheduler, self._config.experiment.pipelines, self._config.experiment.number_of_groups,self._config.experiment.number_of_jobs_per_group, self.schedule.calculate_fairness(), self.schedule.calculate_utilization ]
        #print(dpbx.users_get_current_account()) #Make sure we have access
        with open('./statistics.csv', 'w+') as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerow(data)
        with open('./statistics.csv', 'rb') as f2:
            dpbx.files_upload(f2.read(), '/{}-{}-{}-{}-{}-{}-{}-{}-{}.csv'.format(self._config.experiment.scheduler, self._config.experiment.static, self._config.experiment.nodes, self._config.experiment.pipelines, self._config.experiment.number_of_groups,self._config.experiment.number_of_jobs_per_group, self._config.experiment.repetition, self.schedule.calculate_fairness(), self.schedule.calculate_utilization()), mute = True)


        self.stop()
        return

    def __clear_jobs(self):
        """
        Function to clear existing jobs in the environment (i.e. old experiments/tests)
        @return: None
        @rtype: None
        """
        namespace = self._config.cluster_config.namespace
        self.__logger.info(f'Clearing old jobs in current namespace: {namespace}')

        for job in self.__client.get(namespace=self._config.cluster_config.namespace)['items']:
            job_name = job['metadata']['name']
            self.__logger.info(f'Deleting: {job_name}')
            try:
                self.__client.custom_api.delete_namespaced_custom_object(
                    PYTORCHJOB_GROUP,
                    PYTORCHJOB_VERSION,
                    namespace,
                    PYTORCHJOB_PLURAL,
                    job_name)
            except Exception as e:
                self.__logger.warning(f'Could not delete: {job_name}')
                print(e)
