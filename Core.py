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

        print(f'Task manager {self.pid} init')

        if CONFIG.handler_class:
            try:
                handlerClass = importComponent(CONFIG.handler_class)
            except:
                raise Exception('Handler class not exist')
            if not issubclass(handlerClass, AutoTaskHandler):
                raise Exception('Invalid handler class')
            self.__handler = handlerClass()

        else:
            self.__handler = AutoTaskHandler()

        def exitSignalHandler(*_, ):
            print(f'Manager {self.pid} receive stop signal @ {currentTimeStr()}')
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

    def refreshTaskQueue(self):
        # --------------- lock queue --------------------
        self.__taskQueueLock = True

        currentTime = time.time()

        # --------------- remove overtime task --------------------
        for taskState in self.__taskQueue:
            if taskState.done:
                continue
            if taskState.overTime is None:
                continue
            if taskState.overTime + 5 < currentTime:
                taskState.overTime = None
                self.taskTimeout(taskSn=taskState.taskSn)
                self.removeTask(taskSn=taskState.taskSn)

        # --------------- get executing task --------------------
        executingTask: list[TaskState] = [
            taskState for taskState in self.__taskQueue
            if taskState.executor is not None
        ]
        executingTaskSn: set[int] = {
            taskState.taskSn for taskState in executingTask
        }

        # --------------- get append task --------------------
        appendTask: list[TaskState] = [
            taskState for taskState in self.__handler.getTaskQueue(limit=CONFIG.queueSize)
            if taskState.taskSn not in executingTaskSn
        ]

        # --------------- sort by priority --------------------
        newQueue = [
                       taskRec for taskRec in [*executingTask, *appendTask, ] if not taskRec.done
                   ][:CONFIG.queueSize]

        # --------------- new queue sort --------------------
        newQueue.sort(key=attrgetter('priority', 'taskSn'))

        # --------------- set new queue --------------------
        self.__taskDict = {taskRec.taskSn: taskRec for taskRec in newQueue}
        self.__taskQueue = newQueue

        print(f'TaskManager refreshed queue, current count is {len(self.__taskQueue)}')

        # --------------- unlock queue --------------------
        self.__taskQueueLock = False

    def getTask(self, *args, workerName: str = None, combine: int = None, **kwargs) -> TaskConfig | int:
        # --------------- queue lock or exit return -1 --------------------
        if self.__taskQueueLock or self.__exit:
            return -1

        selectTask = None

        # --------------- search by combine --------------------
        if combine:
            for taskState in self.__taskQueue:
                if taskState.combine == combine and not taskState.executor:
                    selectTask = taskState
                    break

        # --------------- search all queue --------------------
        if selectTask is None:
            for taskState in self.__taskQueue:
                if not taskState.done and not taskState.executor:
                    selectTask = taskState
                    break

        # --------------- no task return 1 --------------------
        if selectTask is None:
            return 1

        # --------------- lock the task --------------------
        selectTask.executor = workerName
        selectTask.overTime = time.time() + selectTask.config.timeout

        # --------------- set task running to db --------------------
        self.__handler.taskRunning(
            taskSn=selectTask.taskSn,
            overTime=int(selectTask.overTime),
            executorName=selectTask.executor,
        )

        print(f'Worker {workerName} get task {selectTask.taskSn}')

        return selectTask.config

    def ping(self, *_, ):
        print(f'Ping task manager @ {currentTimeStr()}')
        return True

    def removeTask(self, taskSn: int):
        taskState = self.__taskDict.get(taskSn)
        if taskState is None:
            return False
        self.__taskDict.pop(taskSn)
        self.__taskQueue.remove(taskState)
        return True

    def taskSuccess(self, *_, taskSn: int = None, result: any = None, ):
        print(f'  Task {taskSn} success, result is {result}')
        self.removeTask(taskSn)
        return self.__handler.taskSuccess(taskSn=taskSn, result=result, )

    def taskError(self, *_, taskSn: int, errorText: str, ):
        print(f'  Task {taskSn} error: {errorText}')
        self.removeTask(taskSn)
        return self.__handler.taskError(taskSn=taskSn, errorText=errorText)

    def taskTimeout(self, *_, taskSn: int, ):
        print(f'  Task {taskSn} timeout')
        return self.__handler.taskTimeout(taskSn=taskSn)

    def invalidConfig(self, *_, taskSn: int, errorText: str, ):
        return self.__handler.taskInvalidConfig(taskSn=taskSn, errorText=errorText, )


# -------------------- worker cluster --------------------
class WorkerCluster:
    def __init__(
            self, *_,
            managerCon: BaseManager = None,
            poolSize=CONFIG.poolSize,
            processFunc: Callable,
    ):
        if managerCon is None:
            raise Exception('Invalid task manager')

        self.__processPool: list[WorkerProcess] = []
        self.__poolSize = poolSize

        self.__exitEvent = Event()
        self.__exitEvent.clear()
        self.__managerCon: BaseManager = managerCon
        self.__processFunc = processFunc

        self.__processCounter = 0

        def exitSignalHandler(*_, ):
            self.__exitEvent.set()
            print(f'Cluster {self.pid} receive stop signal @ {currentTimeStr()}')

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
                print(f'Cluster {self.pid} connect task manager fail, waiting')
                time.sleep(2)

        print(f'Cluster {self.pid} start to create process, pool size is {self.__poolSize}')

    def appendProcess(self):
        if self.__exitEvent.is_set():
            return None
        if len(self.__processPool) == self.__poolSize:
            return None
        while len(self.__processPool) < self.__poolSize:
            self.__processCounter += 1
            process = WorkerProcess(
                sn=self.__processCounter,
                exitEvent=self.__exitEvent,
                processFunc=self.__processFunc,
                manager=self.__managerCon,
            )
            self.__processPool.append(process)

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

    def run(self):
        runCounter = 0
        while True:
            runCounter += 1
            for subProcess in self.__processPool:
                subProcess.checkProcess()

            if self.__exitEvent.is_set():
                allStop = True
                for subProcess in self.__processPool:
                    if subProcess.isAlive():
                        allStop = False
                if allStop:
                    print(f'Cluster {self.pid} exit @ {currentTimeStr()}')
                    return None
                time.sleep(0.5)
                continue

            if runCounter > 20:
                try:
                    pingRes = self.__managerCon.ping()._getvalue()
                    runCounter = 0
                except Exception as err_:
                    print(f'Cluster {self.pid} connect task manager fail: {err_}')
                    time.sleep(2)
                    continue

            self.appendProcess()

            time.sleep(0.5)


# -------------------- sub process --------------------
class WorkerProcess:
    __process = None

    def __init__(
            self, *_,
            sn: int,
            manager: BaseManager,
            exitEvent,
            processFunc,
    ):
        self.__sn = sn
        self.__taskManager = manager
        self.__exitEvent = exitEvent
        self.__processFunc = processFunc

        self.__processOvertime = None
        self.__processPipe, self.__pipe = Pipe()

        self.createProcess()

    def createProcess(self):
        if not parent_process():
            while self.__pipe.poll():
                _ = self.__pipe.recv()

            self.__processOvertime = time.time() + CONFIG.taskTimeout * 2

            self.__process = Process(
                target=self.__processFunc,
                args=(
                    SubProcessConfig(
                        sn=self.__sn,
                        exitEvent=self.__exitEvent,
                        taskManager=self.__taskManager,
                        pipe=self.__processPipe,
                        localName=CONFIG.name,
                    ),
                ),
            )
            self.__process.start()
            time.sleep(0.5)

        else:
            print('This is a sub process')

    def isAlive(self):
        if not self.__process:
            return False
        return self.__process.is_alive()

    def checkProcess(self):
        if not self.isAlive() and not self.__exitEvent.is_set():
            self.createProcess()

        while self.__pipe.poll():
            code, value = self.__pipe.recv()
            match code:
                case 'alive':
                    self.__processOvertime = value + CONFIG.taskTimeout
                case 'overtime':
                    self.__processOvertime = value

        if self.__processOvertime < time.time():
            self.processTerminate()

    def processTerminate(self):
        if not self.isAlive():
            return True

        print(f'-- Worker {self.__sn}|{self.__process.pid} timeout, terminate')
        self.__process.terminate()
        time.sleep(0.5)

    @property
    def pid(self):
        if not self.isAlive():
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
