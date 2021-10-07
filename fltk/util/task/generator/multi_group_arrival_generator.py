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

        for g in range(self.__config.experiment.number_of_groups):
            for j in range(self.__config.experiment.number_of_jobs_per_group):
                self.job_dict[f'train_job_{g}_{j}'] = experiment_descriptions[g]

    def generate_arrival(self, task_id: str) -> Arrival:
        """
        Generate a training task for a JobDescription once the inter-arrival time has been 'deleted'.
        @param task_id: identifier for a training task corresponding to the JobDescription.
        @type task_id: str
        @return: generated arrival corresponding to the unique task_id.
        @rtype: Arrival
        """
        self.logger.info(f"Creating task for {task_id}")

        job: JobDescription = self.job_dict[task_id]
        parameters: JobClassParameter = choices(job.job_class_parameters, [param.class_probability for param in job.job_class_parameters])[0]
        priority = choices(parameters.priorities, [prio.probability for prio in parameters.priorities], k=1)[0]

        if self.__config.experiment.static:
            job_number = int(task_id.split("_")[3])

            inter_arrival_ticks = [0, 30, 24, 40, 34, 27, 30, 44, 34][job_number]
        else:
            inter_arrival_ticks = np.random.poisson(lam=job.arrival_statistic)
        train_task = TrainTask(task_id, parameters, priority)

        group = task_id.split("_")[2]

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
        # schedule first job of each group
        for g in range(self.__config.experiment.number_of_groups):
            task_id = f'train_job_{g}_{0}'

            new_arrival: Arrival = self.generate_arrival(task_id)
            self._tick_list.append(new_arrival)
            self.logger.info(f"Arrival {task_id} arrives at {new_arrival.ticks} seconds")

        event = multiprocessing.Event()
        while self._alive and time.time() - self.start_time < duration:
            save_time = time.time()

            new_scheduled = []
            for entry in self._tick_list:
                entry.ticks -= self._decrement
                if entry.ticks <= 0:
                    self.arrivals.put(entry)
                    group = entry.task_id.split("_")[2]
                    job = int(entry.task_id.split("_")[3]) + 1
                    new_task_id = f'train_job_{group}_{job}'

                    if job >= self.__config.experiment.number_of_jobs_per_group:
                        continue

                    new_arrival = self.generate_arrival(new_task_id)
                    new_scheduled.append(new_arrival)
                    self.logger.info(f"Arrival {new_task_id} arrives at {new_arrival.ticks} seconds")
                else:
                    new_scheduled.append(entry)
            self._tick_list = new_scheduled

            if len(new_scheduled) == 0:
                self._alive = False
            # Correct for time drift between execution, otherwise drift adds up, and arrivals don't generate correctly
            correction_time = time.time() - save_time
            event.wait(timeout=self._decrement - correction_time)
        self.stop_time = time.time()
        self.logger.info(f"Stopped execution at: {self.stop_time}, duration: {self.stop_time - self.start_time}/{duration}")
