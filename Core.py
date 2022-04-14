import time
import signal
from typing import Callable
from multiprocessing import (Process, Pipe, parent_process, Event, current_process, )
from multiprocessing.managers import SyncManager

from django.core.exceptions import AppRegistryNotReady
from django.apps.registry import apps

try:
    apps.check_apps_ready()
except AppRegistryNotReady:
    import django

    django.setup()

from .Component import *
from .Handler import AutoTaskHandler


# -------------------- Task manager --------------------
class TaskManager:
    def __init__(self, *_, handler: AutoTaskHandler, **kwargs):
        print(f'Task manager {current_process().pid} init.')
        self.__lock = False
        self.__taskList = []
        if not isinstance(handler, AutoTaskHandler):
            raise Exception('Invalid auto task handler.')
        self.__handler = handler

    def lock(self):
        self.__lock = True

    def unlock(self):
        self.__lock = False

    def appendTask(self):
        self.__taskList.append('Task')

    def getTask(self, *args, **kwargs):
        print(f'Manager getTask called by\n    args: {args}, kwargs: {kwargs}.')
        if self.__lock:
            return None
        return TaskConfig(
            sn=100001,
            func='test',
            args=self.__handler.serialize(
                ('A', 'B', 'C')
            ),
            kwargs=self.__handler.serialize(
                {
                    'taskCount': len(self.__taskList),
                }
            ),
        )

    def ping(self, *_, **kwargs):
        print(f'Ping task manager @ {currentTimeStr()}')
        return True

    def taskFinish(self, *_, taskSn: int = None, **kwargs):
        if not isinstance(taskSn, int):
            return None
        return True

    def taskError(self, *_, errorCode: int = 0, **kwargs):
        return True


# -------------------- process group --------------------
class ExecutorGroup:
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

        self.__processCounter = 0

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
            except Exception as err_:
                print(f'Group {self.pid} connect task manager fail, waiting.')
                time.sleep(2)

        print(f'Group {self.pid} start to create process, pool size is {self.__poolSize}.')

    def appendProcess(self):
        if self.__exitEvent.is_set():
            return None
        if len(self.__processPool) == self.__poolSize:
            return None
        while len(self.__processPool) < self.__poolSize:
            self.__processCounter += 1
            process = SubProcess(
                sn=self.__processCounter,
                stopEvent=self.__exitEvent,
                processFunc=self.__processFunc,
                manager=self.__managerCon,
            )
            self.__processPool.append(process)
            time.sleep(0.1)

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
        # serverCheckTime = time.time()
        while True:
            if self.__exitEvent.is_set():
                break
            try:
                pingRes = self.__managerCon.ping()._getvalue()
            except Exception as err_:
                print(f'Group {self.pid} connect task manager fail: {err_}')
                time.sleep(2)
                continue

            self.appendProcess()
            for subProcess in self.__processPool:
                subProcess.checkAlive()

            time.sleep(0.2)
        self.exit()


# -------------------- sub process --------------------
class SubProcess:
    __process = None

    def __init__(
            self, *_,
            sn: int,
            manager: SyncManager,
            stopEvent,
            processFunc,
    ):
        self.__sn = sn
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
                        sn=self.__sn,
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

    @property
    def sn(self):
        return self.__sn


# -------------------- sync manager --------------------

class ManagerServer(SyncManager):
    pass


class ManagerAdmin(SyncManager):
    pass


def managerServerRegister():
    taskManager = TaskManager(handler=AutoTaskHandler())

    syncManagerConfig = {
        'getTask': taskManager.getTask,
        'ping': taskManager.ping,
    }

    for prop_ in taskManager.__dir__():
        if prop_[0] == '_':
            continue
        func_ = getattr(taskManager, prop_)
        if callable(func_):
            ManagerServer.register(prop_, func_, )
            ManagerAdmin.register(prop_, )


class ManagerClient(SyncManager):
    pass


managerClientFunc = ('getTask', 'ping',)

for name_ in managerClientFunc:
    ManagerClient.register(name_)
