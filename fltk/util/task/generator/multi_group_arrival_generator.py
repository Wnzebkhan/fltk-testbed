import logging
import multiprocessing
import time
from pathlib import Path
from random import choices
from typing import Dict, List, Union

import numpy as np

from fltk.util.config import BareConfig
from fltk.util.task.config.parameter import TrainTask, JobDescription, ExperimentParser, JobClassParameter
from fltk.util.task.generator.arrival_generator import ArrivalGenerator, Arrival


class MultiGroupArrivalGenerator(ArrivalGenerator):
    start_time: float = -1
    stop_time: float = -1
    job_dict: Dict[str, JobDescription] = None

    _tick_list: List[Arrival] = []
    _alive: bool = False
    _decrement = 10
    __default_config: Path = Path('configs/tasks/example_arrival_config.json')

    def __init__(self, config: BareConfig, custom_config: Path = None):
        super(MultiGroupArrivalGenerator, self).__init__(custom_config or self.__default_config)
        self.__config = config
        self.load_config()
        self.jobs_per_group = dict()

    def set_logger(self, name: str = None):
        """
        Set logging name of the ArrrivalGenerator object to a recognizable name. Needs to be called once, as otherwise
        the logger is Uninitialized, resulting in failed execution.
        @param name: Name to use, by default the name 'ArrivalGenerator' is used.
        @type name: str
        @return: None
        @rtype: None
        """
        logging_name = name or self.__class__.__name__
        self.logger = logging.getLogger(logging_name)

    def load_config(self, alternative_path: Path = None):
        """
        Load configuration from default path, if alternative path is not provided.
        @param alternative_path: Optional non-default location to load the configuration from.
        @type alternative_path: Path
        @return: None
        @rtype: None
        """
        parser = ExperimentParser(config_path=alternative_path or self.configuration_path)
        experiment_descriptions = parser.parse()
        self.job_dict = {}
        total_jobs = self.__config.experiment.number_of_jobs_per_group * self.__config.experiment.number_of_groups
        for i in range(total_jobs):
            random_experiment_description = experiment_descriptions[np.random.randint(0, len(experiment_descriptions))]
            self.job_dict[f'train_job_{i}'] = random_experiment_description

    def generate_arrival(self, task_id: str) -> Arrival:
        """
        Generate a training task for a JobDescription once the inter-arrival time has been 'deleted'.
        @param task_id: identifier for a training task corresponding to the JobDescription.
        @type task_id: str
        @return: generated arrival corresponding to the unique task_id.
        @rtype: Arrival
        """
        self.logger.info(f"Creating task for {task_id}")
        self.logger.info(f"1 task for {task_id}")

        job: JobDescription = self.job_dict[task_id]
        parameters: JobClassParameter = choices(job.job_class_parameters, [param.class_probability for param in job.job_class_parameters])[0]
        priority = choices(parameters.priorities, [prio.probability for prio in parameters.priorities], k=1)[0]
        self.logger.info(f"2 task for {task_id}")

        # parameters.hyper_parameters
        parameters.system_parameters.executor_memory = f"{(self.__config.experiment.memory_per_job / 100) * 2000}Mi"
        parameters.system_parameters.executor_cores = f"{(self.__config.experiment.cpu_per_job / 100) * 1000}m"

        self.logger.info(f"3 task for {task_id}")

        inter_arrival_ticks = np.random.poisson(lam=job.arrival_statistic)
        train_task = TrainTask(task_id, parameters, priority)
        self.logger.info(f"4 task for {task_id}")

        # randomly pick a group
        group = np.random.randint(0, self.__config.experiment.number_of_groups)
        self.logger.info(f"5 task for {task_id}")

        if group not in self.jobs_per_group:
            self.jobs_per_group[group] = 0

        x = 0
        while self.jobs_per_group[group] > self.__config.experiment.number_of_jobs_per_group:
            # TODO this can be done smarter
            self.logger.info(f"5.{x} task for {task_id}")
            x += 1
            group = np.random.randint(0, self.__config.experiment.number_of_groups)
        self.logger.info(f"6 task for {task_id}")

        group = f"group_{group}"

        return Arrival(inter_arrival_ticks, train_task, task_id, group)

    def start(self, duration: Union[float, int]):
        """
        Function to start arrival generator, requires to
        @return: None
        @rtype: None
        """
        if not self.logger:
            self.set_logger()
        self.logger.info("Starting execution of arrival generator...")
        self._alive = True
        self.run(duration)

    def stop(self) -> None:
        """
        Function to call when the generator needs to stop. By default the generator will run for 1 hour.
        @return: None
        @rtype: None
        """
        self.logger.info("Received stopping signal")
        self._alive = False

    def run(self, duration: float):
        """
        Run function to generate arrivals during existence of the Orchestrator. Accounts time-drift correction for
        long-term execution duration of the generator (i.e. for time taken by Python interpreter).
        @return: None
        @rtype: None
        """
        np.random.seed(42)
        self.start_time = time.time()
        self.logger.info("Populating tick lists with initial arrivals")
        for task_id in self.job_dict.keys():
            new_arrival: Arrival = self.generate_arrival(task_id)
            self._tick_list.append(new_arrival)
            self.logger.info(f"Arrival {new_arrival} arrives at {new_arrival.ticks} seconds")
        event = multiprocessing.Event()
        while self._alive and time.time() - self.start_time < duration:
            save_time = time.time()

            new_scheduled = []
            for entry in self._tick_list:
                entry.ticks -= self._decrement
                if entry.ticks <= 0:
                    self.arrivals.put(entry)
                    new_arrival = self.generate_arrival(entry.task_id)
                    new_scheduled.append(new_arrival)
                    self.logger.info(f"Arrival {new_arrival} arrives at {new_arrival.ticks} seconds")
                else:
                    new_scheduled.append(entry)
            self._tick_list = new_scheduled
            # Correct for time drift between execution, otherwise drift adds up, and arrivals don't generate correctly
            correction_time = time.time() - save_time
            event.wait(timeout=self._decrement - correction_time)
        self.stop_time = time.time()
        self.logger.info(f"Stopped execution at: {self.stop_time}, duration: {self.stop_time - self.start_time}/{duration}")
