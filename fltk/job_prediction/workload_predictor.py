import uuid

from fltk.util.task.config.parameter import SystemParameters, HyperParameters
from fltk.util.task.task import ArrivalTask
import re


class JobWorkloadPredictor:
    def __init__(self):
        self.history = dict()

    def predict_length(self, task: ArrivalTask):
        task_vector = self.get_task_vector(task)

        if len(self.history.keys()) == 0:
            # this is basically a random guess based on the task vector
            return (task_vector[0] * task_vector[1]) / (task_vector[3])
        else:
            closest = None
            min_dist = 0
            for other_vec_id, (other_vec, time) in self.history.items():
                # pretty stupid algorithm which just searches for the closest vector using a distance metric
                dist = self.calc_vector_distance(task_vector, other_vec)
                if closest == None or min_dist < dist:
                    closest = other_vec_id
                    min_dist = dist

            return self.history[closest][1]


    def calc_vector_distance(self, vec_a, vec_b):
        # manhattan or euclidean distance would not work since the difference in magnitude between 2 vector dimensions would mess with the results
        # cosine distance would not work since we care about the magnitude of the vectors

        # so instead we use manhattan distance but we devide the difference by the largest one
        sum = 0
        for x_a, x_b in zip(vec_a, vec_b):
            sum += abs(x_a - x_b) / max(x_a, x_b)

        return sum

    def feedback(self, task: ArrivalTask, actual_length: int):
        task_vector = self.get_task_vector(task)

        self.history[task.id] = (task_vector, actual_length)

    def get_task_vector(self, task: ArrivalTask):
        return [
            int(task.param_conf.max_epoch),
            int(task.param_conf.bs),
            int(task.sys_conf.data_parallelism),
            self.cores_to_number(task.sys_conf.executor_cores),
            self.memory_to_number(task.sys_conf.executor_memory)
        ]

    def cores_to_number(self, cores):
        r = re.compile("([0-9]+)([a-zA-Z]+)")
        m = r.match(cores)
        # TODO assumes that we always use "XXXXm"
        return int(m.group(1))


    def memory_to_number(self, memory):
        r = re.compile("([0-9]+)([a-zA-Z]+)")
        m = r.match(memory)

        multiplier = 1
        if m.group(2) == 'Ki':
            multiplier = 1000
        elif m.group(2) == 'Mi':
            multiplier = 1000000
        elif m.group(2) == 'Gi':
            multiplier = 1000000000

        return int(m.group(1)) * multiplier




if __name__ == "__main__":
    # test the predictor
    predictor = JobWorkloadPredictor()

    tasks = [
        ArrivalTask(priority=1,
                    id=uuid.uuid4(),
                    network="",
                    dataset="",
                    sys_conf=SystemParameters(
                        data_parallelism=2,
                        executor_cores=2,
                        executor_memory="2Gi",
                        action=""
                    ),
                   param_conf=HyperParameters(
                       bs=128,
                       max_epoch=5,
                       lr="0.05",
                       lr_decay="0.001"
                   )),
        ArrivalTask(priority=1,
                    id=uuid.uuid4(),
                    network="",
                    dataset="",
                    sys_conf=SystemParameters(
                        data_parallelism=2,
                        executor_cores=2,
                        executor_memory="2Gi",
                        action=""
                    ),
                    param_conf=HyperParameters(
                        bs=256,
                        max_epoch=3,
                        lr="0.05",
                        lr_decay="0.001"
                    )),
        ArrivalTask(priority=1,
                    id=uuid.uuid4(),
                    network="",
                    dataset="",
                    sys_conf=SystemParameters(
                        data_parallelism=2,
                        executor_cores=5,
                        executor_memory="3Gi",
                        action=""
                    ),
                    param_conf=HyperParameters(
                        bs=128,
                        max_epoch=7,
                        lr="0.05",
                        lr_decay="0.001"
                    ))
    ]

    print(predictor.predict_length(tasks[0]))
    predictor.feedback(tasks[0], 10)
    assert predictor.predict_length(tasks[0]) == 10

    assert predictor.predict_length(tasks[1]) == 10
    predictor.feedback(tasks[1], 100)

    assert predictor.predict_length(tasks[2]) == 100
    predictor.feedback(tasks[2], 1000)


