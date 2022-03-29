import time
from typing import Callable
from time import time as currentTime

from multiprocessing import (
    Process, JoinableQueue, Pipe, Event,
    current_process, Value, SimpleQueue, Manager
)

from multiprocessing.managers import SyncManager

from queue import PriorityQueue

from ..models import TaskRec


class TaskManager:
    def __init__(self, *_, poolSize=4, ):
        self.__processPool = []
        while len(self.__processPool) < poolSize:
            self.__processPool.append(WorkerProcess(
                manager=self,
                runFunc=workerProcessFunc,
            ))

        self.__pid = current_process().pid
        self.activeTime = Value(int)


    @property
    def pid(self):
        return {
            'pid': self.__pid,
            'workerPid': [
                worker_.pid for worker_ in self.__processPool
            ],
        }


class WorkerProcess():
    def __init__(self, *_, manager: TaskManager, runFunc: Callable):
        if not isinstance(manager, TaskManager):
            raise BaseException('Invalid TaskManager.')
        self.__manager = manager
        self.__activeTime = currentTime()
        parentConn, childConn = Pipe()
        self.__pipe = parentConn

        self.__process = Process(
            target=runFunc,
            kwargs={
                'pipe': childConn,
            },
        )

    def ping(self):
        self.__pipe.send(currentTime())

    @property
    def pid(self):
        return self.__process.pid

    pass


def workerProcessFunc(*_, taskQueue: JoinableQueue):
    if __name__ == '__main__':
        while True:
            taskQueue.get()
            time.sleep(1)
        pass
