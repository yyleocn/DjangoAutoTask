import time
import signal
from operator import attrgetter
from typing import Callable
from multiprocessing import (Process, Pipe, Event, parent_process, current_process, )
from multiprocessing.managers import BaseManager

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
    __pid: int = None

    def __init__(self, *_, **kwargs):
        self.__taskQueueLock: bool = False
        self.__taskQueue: list[TaskState] = []
        self.__taskDict: dict[int, TaskState] = {}
        self.__exit: bool = False
        self.__pid = current_process().pid

        print(f'Task manager {self.pid} init.')
        if CONFIG.handler_class:
            try:
                handlerClass = importComponent(CONFIG.handler_class)
            except:
                raise Exception('Handler class not exist.')
            if not issubclass(handlerClass, AutoTaskHandler):
                raise Exception('Invalid handler class.')
            self.__handler = handlerClass()

        else:
            self.__handler = AutoTaskHandler()

        def exitSignalHandler(*_, ):
            print(f'Manager {self.pid} receive stop signal @ {currentTimeStr()}.')
            self.exit()

        signal.signal(signal.SIGINT, exitSignalHandler)
        signal.signal(signal.SIGTERM, exitSignalHandler)

        self.__taskSnCounter = 1000000

    @property
    def pid(self):
        return self.__pid

    def exit(self):
        self.__exit = True
        time.sleep(10)
        exit()

    # def lock(self):
    #     self.__taskQueueLock = True
    #
    # def unlock(self):
    #     self.__taskQueueLock = False

    def refreshTaskQueue(self):
        self.__taskQueueLock = True

        currentTime = time.time()
        for taskState in self.__taskQueue:
            if taskState.done:
                continue
            if taskState.overTime is None:
                continue
            if taskState.overTime + 5 < currentTime:
                taskState.overTime = None
                self.taskTimeout(taskSn=taskState.taskSn)
                self.removeTask(taskSn=taskState.taskSn)

        appendTask: list[TaskState] = [
            TaskState(
                taskSn=taskRec['taskSn'], combine=taskRec['combine'], priority=taskRec['priority'],
                config=TaskConfig(
                    sn=taskRec['taskSn'], combine=taskRec['combine'],
                    timeout=taskRec['timeout'] or CONFIG.processTimeLimit,
                    func=taskRec['func'], callback=taskRec['callback'],
                    args=taskRec['args'], kwargs=taskRec['kwargs'],
                ),
            ) for taskRec in self.__handler.getTaskQueue(limit=CONFIG.queueSize)
            if taskRec['taskSn'] not in self.__taskDict
        ]
        appendTask.sort(key=attrgetter('priority', 'taskSn'))

        newQueue = [
                       taskRec for taskRec in [*self.__taskQueue, *appendTask, ] if not taskRec.done
                   ][:CONFIG.queueSize]

        newQueue.sort(key=attrgetter('priority', 'taskSn'))

        self.__taskDict = {taskRec.taskSn: taskRec for taskRec in newQueue}
        self.__taskQueue = newQueue

        print(f'TaskManager refreshed queue, current count is {len(self.__taskQueue)}')

        self.__taskQueueLock = False

    def getTask(self, *args, workerName: str = None, combine: int = None, **kwargs) -> TaskConfig | int:
        if self.__taskQueueLock or self.__exit:
            return -1

        selectTaskRec = None

        if combine:
            for taskRec in self.__taskQueue:
                if taskRec.combine == combine and not taskRec.done and not taskRec.getBy:
                    selectTaskRec = taskRec
                    break

        if not selectTaskRec:
            for taskRec in self.__taskQueue:
                if not taskRec.done and not taskRec.getBy:
                    selectTaskRec = taskRec
                    break
        if not selectTaskRec:
            return 1

        selectTaskRec.getBy = workerName
        selectTaskRec.overTime = time.time() + selectTaskRec.config.timeout

        print(f'Worker {workerName} get task {selectTaskRec.taskSn}.')

        return selectTaskRec.config

    def ping(self, *_, **kwargs):
        self.redundantKwargs(**kwargs)
        print(f'Ping task manager @ {currentTimeStr()}')
        return True

    def removeTask(self, taskSn: int):
        taskState = self.__taskDict.get(taskSn)
        if taskState is None:
            return False
        self.__taskDict.pop(taskSn)
        self.__taskQueue.remove(taskState)
        return True

    def taskRunning(self, *_, taskSn: int):
        self.__handler.taskRunning(taskSn=taskSn)

    def taskSuccess(self, *_, taskSn: int = None, result: any = None, **kwargs):
        self.redundantKwargs(**kwargs)
        print(f'    Task {taskSn} success, result is {result}.')

        self.removeTask(taskSn)
        return self.__handler.taskSuccess(taskSn=taskSn, result=result, )

    def taskError(self, *_, taskSn: int, errorText: str, **kwargs):
        self.redundantKwargs(**kwargs)

        return self.__handler.taskError(taskSn=taskSn, errorText=errorText)

    def taskTimeout(self, *_, taskSn: int, **kwargs):
        self.redundantKwargs(**kwargs)

        print(f'    Task {taskSn} timeout.')
        return self.__handler.taskTimeout(taskSn=taskSn)

    def invalidConfig(self, *_, taskSn: int, errorText: str, **kwargs):
        self.redundantKwargs(**kwargs)

        return self.__handler.taskInvalidConfig(taskSn=taskSn, errorText=errorText, )

    @staticmethod
    def redundantKwargs(**kwargs):
        pass
        if kwargs:
            print(f'Redundant kwargs {kwargs}.')


# -------------------- worker cluster --------------------
class WorkerCluster:
    def __init__(
            self, *_,
            managerCon: BaseManager = None,
            poolSize=CONFIG.poolSize,
            processFunc: Callable,
    ):
        if managerCon is None:
            raise Exception('Invalid task manager.')

        self.__processPool = []
        self.__poolSize = poolSize

        self.__exitEvent = Event()
        self.__exitEvent.clear()
        self.__managerCon: BaseManager = managerCon
        self.__processFunc = processFunc

        self.__processCounter = 0

        def exitSignalHandler(*_, ):
            print(f'Cluster {self.pid} receive stop signal @ {currentTimeStr()}.')
            self.exit()

        signal.signal(signal.SIGINT, exitSignalHandler)
        signal.signal(signal.SIGTERM, exitSignalHandler)

        initTime = time.time()
        while True:
            if self.__exitEvent.is_set():
                break
            try:
                self.__managerCon.connect()
                break
            except Exception as err_:
                print(f'Cluster {self.pid} connect task manager fail, waiting.')
                time.sleep(2)

        print(f'Cluster {self.pid} start to create process, pool size is {self.__poolSize}.')

    def appendProcess(self):
        if self.__exitEvent.is_set():
            return None
        if len(self.__processPool) == self.__poolSize:
            return None
        while len(self.__processPool) < self.__poolSize:
            self.__processCounter += 1
            process = WorkerProcess(
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
    def processPid(self):
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
                print(f'Cluster {self.pid} exit @ {currentTimeStr()}.')
                exit()
            time.sleep(0.2)

    def run(self):
        runCounter = 0
        while True:
            runCounter += 1
            if self.__exitEvent.is_set():
                break

            if runCounter > 9:
                try:
                    pingRes = self.__managerCon.ping()._getvalue()
                    runCounter = 0
                except Exception as err_:
                    print(f'Cluster {self.pid} connect task manager fail: {err_}')
                    time.sleep(2)
                    continue

            self.appendProcess()
            for subProcess in self.__processPool:
                subProcess.checkAlive()

            time.sleep(0.5)
        self.exit()


# -------------------- sub process --------------------
class WorkerProcess:
    __process = None

    def __init__(
            self, *_,
            sn: int,
            manager: BaseManager,
            stopEvent,
            processFunc,
    ):
        self.__sn = sn
        self.__taskManager = manager
        self.__stopEvent = stopEvent
        self.__processFunc = processFunc

        self.__processRefreshTime = time.time()
        self.__processOvertime = CONFIG.processTimeLimit
        self.__processPipe, self.__pipe = Pipe()

        self.createProcess()

    def createProcess(self):
        if not parent_process():
            while self.__pipe.poll():
                _ = self.__pipe.recv()
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
            self.__processRefreshTime = time.time()
        else:
            print('This is a sub process.')

    def is_alive(self):
        if not self.__process:
            return False
        return self.__process.is_alive()

    def checkAlive(self):
        if self.__process is None:
            self.createProcess()

        if not self.__process.is_alive():
            self.createProcess()

        while self.__pipe.poll():
            code, value = self.__pipe.recv()
            match code:
                case 'alive':
                    self.__processRefreshTime = value
                case 'overtime':
                    self.__processOvertime = value

        currentTime = time.time()

        if currentTime > self.__processOvertime or currentTime > self.__processRefreshTime + CONFIG.processTimeLimit:
            self.__process.terminate()
            time.sleep(0.5)

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

class ManagerServer(BaseManager):
    __methodBounded = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.methodBound()

    @classmethod
    def methodBound(cls):
        if cls.__methodBounded:
            return

        taskManager = TaskManager()
        for prop_ in taskManager.__dir__():
            if prop_[0] == '_':
                continue
            func_ = getattr(taskManager, prop_)
            if callable(func_):
                cls.register(prop_, func_, )

        cls.__methodBounded = True


class ManagerAdmin(BaseManager):
    __methodBounded = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.methodBound()

    @classmethod
    def methodBound(cls):
        if cls.__methodBounded:
            return

        taskManager = TaskManager()
        for prop_ in taskManager.__dir__():
            if prop_[0] == '_':
                continue
            func_ = getattr(taskManager, prop_)
            if callable(func_):
                cls.register(prop_, )

        cls.__methodBounded = True


class ManagerClient(BaseManager):
    __methodBounded = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.methodBound()

    @classmethod
    def methodBound(cls):
        if cls.__methodBounded:
            return

        managerClientFunc = (
            'ping', 'getTask',
            'configError', 'taskSuccess', 'taskError', 'taskTimeout',
        )
        for name_ in managerClientFunc:
            cls.register(name_)

        cls.__methodBounded = True


__all__ = (
    'TaskManager', 'WorkerProcess', 'WorkerCluster',
    'ManagerServer', 'ManagerClient', 'ManagerAdmin',
)
