import signal

from multiprocessing import (
    Process, Pipe, parent_process,
    current_process,
)

from django.core.exceptions import AppRegistryNotReady
from django.apps.registry import apps

try:
    apps.check_apps_ready()
except AppRegistryNotReady:
    import django

    django.setup()

from .Component import *


class TaskManager:
    @staticmethod
    def getTask(*args, **kwargs):
        print(f'Manager getTask called by args: {args}, kwargs: {kwargs}.')
        return f'Task Data response {time.time():.3f}'


class WorkerGroup:
    def __init__(
            self, *_,
            taskManager: SyncManager = None,
            poolSize=4,
            processFunc: Callable,
    ):
        if taskManager is None:
            raise Exception('Invalid task manager.')

        self.__processPool = []
        self.__poolSize = poolSize
        self.__pid = current_process().pid

        self.__stopEvent = Event()
        self.__stopEvent.clear()
        self.__taskManager: SyncManager = taskManager
        self.__processFunc = processFunc

        def stopSignalHandler(*_, ):
            print(f'Group {self.pid} receive stop signal @ {currentTimeStr()}.')
            self.exit()

        signal.signal(signal.SIGINT, stopSignalHandler)
        signal.signal(signal.SIGTERM, stopSignalHandler)

        print(f'Group {self.pid} start to create worker process, pool size is {self.__poolSize}.')

        self.appendProcess()

    def appendProcess(self):
        if self.__stopEvent.is_set():
            return None
        if len(self.__processPool) == self.__poolSize:
            return None
        while len(self.__processPool) < self.__poolSize:
            process = SubProcess(
                stopEvent=self.__stopEvent,
                processFunc=self.__processFunc,
                taskManager=self.__taskManager,
            )
            self.__processPool.append(process)

    @property
    def pid(self):
        return current_process().pid

    @property
    def workerPid(self):
        return [
            process.pid for process in self.__processPool
        ]

    @property
    def running(self):
        return not self.__stopEvent.is_set()

    @property
    def stopEvent(self):
        return self.__stopEvent

    def exit(self):
        self.__stopEvent.set()
        while True:
            allStop = True
            for workerProcess in self.__processPool:
                if workerProcess.is_alive():
                    allStop = False
            if allStop:
                print(f'Group {self.pid} receive exit @ {currentTimeStr()}.')
                exit()
            time.sleep(1)

    def run(self):
        while True:
            time.sleep(1)
            for workerProcess in self.__processPool:
                workerProcess.checkAlive()
            self.appendProcess()


class SubProcess:
    __process = None

    def __init__(
            self, *_,
            taskManager: SyncManager,
            stopEvent,
            processFunc,
    ):
        self.__taskManager = taskManager
        self.__stopEvent = stopEvent
        self.__processFunc = processFunc

        self.__processAliveTime = time.time()
        self.__processPipe, self.__pipe = Pipe()

        self.createProcess()

    def createProcess(self):
        if not parent_process():
            self.__process = Process(
                target=self.__processFunc,
                args=(SubProcessConfig(
                    stopEvent=self.__stopEvent,
                    taskManager=self.__taskManager,
                    pipe=self.__processPipe,
                ),)
            )
            self.__process.start()
            self.__processAliveTime = time.time()
        else:
            print('This is a sub process.')

    def is_alive(self):
        if not self.__process:
            return False
        return self.__process.is_alive()

    def checkAlive(self):
        if self.__process is None:
            return None

        while self.__pipe.poll():
            self.__processAliveTime = self.__pipe.recv()

        if time.time() - self.__processAliveTime > 300:
            self.__process.terminate()

        if not self.__process.is_alive():
            self.createProcess()
            return None

    @property
    def pid(self):
        if self.__process is None:
            return None
        return self.__process.pid
