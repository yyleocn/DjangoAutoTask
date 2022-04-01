import time
import signal
from typing import Callable
from multiprocessing import (Process, Pipe, parent_process, Event, current_process, )
from multiprocessing.managers import SyncManager, BaseManager

from django.core.exceptions import AppRegistryNotReady
from django.apps.registry import apps

try:
    apps.check_apps_ready()
except AppRegistryNotReady:
    import django

    django.setup()

from .Component import *
from .Conf import *


# -------------------- Task manager --------------------
class TaskManager:
    @staticmethod
    def getTask(*args, **kwargs):
        print(f'Manager getTask called by\n    args: {args}, kwargs: {kwargs}.')
        return TaskConfig(
            sn=100001,
            func='test',
            args=('A', 'B', 'C'),
            kwargs={
                'a': 1,
                'b': 2,
            },
        )

    @staticmethod
    def ping(*_, **kwargs):
        print(f'Ping task manager @ {currentTimeStr()}')
        return True

    @staticmethod
    def taskFinish(*_, **kwargs):
        return True

    @staticmethod
    def taskError(*_, errorCode: int = 0, **kwargs):
        return True


# -------------------- process group --------------------
class ProcessGroup:
    def __init__(
            self, *_,
            managerCon: SyncManager = None,
            poolSize=CONFIG.poolSize,
            processFunc: Callable,
    ):
        if managerCon is None:
            raise Exception('Invalid task manager.')

        self.__processPool = []
        self.__poolSize = poolSize

        self.__exitEvent = Event()
        self.__exitEvent.clear()
        self.__managerCon: SyncManager = managerCon
        self.__processFunc = processFunc

        def stopSignalHandler(*_, ):
            print(f'Group {self.pid} receive stop signal @ {currentTimeStr()}.')
            self.exit()

        signal.signal(signal.SIGINT, stopSignalHandler)
        signal.signal(signal.SIGTERM, stopSignalHandler)

        initTime = time.time()
        while True:
            if self.__exitEvent.is_set():
                break
            try:
                self.__managerCon.connect()
                break
            except:
                print(f'Group {self.pid} connect task manager fail, waiting.')
                time.sleep(2)

        print(f'Group {self.pid} start to create process, pool size is {self.__poolSize}.')

    def appendProcess(self):
        if self.__exitEvent.is_set():
            return None
        if len(self.__processPool) == self.__poolSize:
            return None
        while len(self.__processPool) < self.__poolSize:
            process = SubProcess(
                stopEvent=self.__exitEvent,
                processFunc=self.__processFunc,
                manager=self.__managerCon,
            )
            self.__processPool.append(process)

    @property
    def pid(self):
        return current_process().pid

    @property
    def subPid(self):
        return [
            process.pid for process in self.__processPool
        ]

    @property
    def exitEvent(self):
        return self.__exitEvent

    def exit(self):
        self.__exitEvent.set()
        while True:
            allStop = True
            for subProcess in self.__processPool:
                if subProcess.is_alive():
                    allStop = False
            if allStop:
                print(f'Group {self.pid} exit @ {currentTimeStr()}.')
                exit()
            time.sleep(0.2)

    def run(self):
        serverCheckTime = time.time()
        while True:
            if self.__exitEvent.is_set():
                break
            try:
                self.__managerCon.ping()
            except Exception as err_:
                print(f'Group {self.pid} connect task manager fail: {err_}')
                time.sleep(2)
                continue

            serverCheckTime = time.time()
            self.appendProcess()
            for subProcess in self.__processPool:
                subProcess.checkAlive()

            time.sleep(1)
        self.exit()


# -------------------- sub process --------------------
class SubProcess:
    __process = None

    def __init__(
            self, *_,
            manager: SyncManager,
            stopEvent,
            processFunc,
    ):
        self.__taskManager = manager
        self.__stopEvent = stopEvent
        self.__processFunc = processFunc

        self.__processAliveTime = time.time()
        self.__processPipe, self.__pipe = Pipe()

        self.createProcess()

    def createProcess(self):
        if not parent_process():
            self.__process = Process(
                target=self.__processFunc,
                args=(
                    SubProcessConfig(
                        stopEvent=self.__stopEvent,
                        taskManager=self.__taskManager,
                        pipe=self.__processPipe,
                        localName=CONFIG.name,
                    ),
                ),
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

        if time.time() - self.__processAliveTime > CONFIG.taskTimeLimit:
            self.__process.terminate()

        if not self.__process.is_alive():
            self.createProcess()

    @property
    def pid(self):
        if self.__process is None:
            return None
        if not self.__process.is_alive():
            return None
        return self.__process.pid


# -------------------- sync manager --------------------

class TaskManagerServer(SyncManager):
    pass


class TaskManagerClient(SyncManager):
    pass


taskManager = TaskManager()
syncManagerConfig = {
    'getTask': taskManager.getTask,
    'ping': taskManager.ping,
}

for name_, func_ in syncManagerConfig.items():
    TaskManagerClient.register(name_, )
    TaskManagerServer.register(name_, func_, )
