import time
import signal
from operator import attrgetter

from multiprocessing import (current_process, )
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
        self.__shutdown: bool = False
        self.__pid = current_process().pid
        self.__clusterDict = {}

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

        def shutdownHandler(*_, ):
            print(f'Manager {self.pid} receive stop signal @ {currentTimeStr()}')
            self.exit()

        for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGILL,):
            signal.signal(sig, shutdownHandler)

    def shutdownManager(self):
        self.__shutdown = True
        return 'The TaskManger is set to shutdown.'

    @property
    def pid(self):
        return self.__pid

    def isRunning(self):
        if self.__exit or self.__shutdown:
            return False
        return True

    def exit(self):
        self.__exit = True
        time.sleep(5)
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

        if not self.isRunning():
            return

            # --------------- get executing task --------------------
        runningTask: list[TaskState] = [
            taskState for taskState in self.__taskQueue
            if taskState.executor is not None
        ]

        executingTaskSn: set[int] = {
            taskState.taskSn for taskState in runningTask
        }

        # --------------- get append task --------------------
        appendTask: list[TaskState] = [
            taskState for taskState in self.__handler.getTaskQueue(limit=CONFIG.queueSize)
            if taskState.taskSn not in executingTaskSn
        ]

        # --------------- sort by priority --------------------
        newQueue = [
                       taskRec for taskRec in [*runningTask, *appendTask, ] if not taskRec.done
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
        if self.__taskQueueLock or not self.isRunning():
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

    def ping(self, status, *_, ):
        if isinstance(status, dict):
            clusterName = status.get('name')
            clusterPid = status.get('pid')
            clusterStatus = status.get('status')
            print(f'Cluster {clusterName} ping task manager')
            self.__clusterDict[clusterName] = status

        else:
            print('Invalid ping message.')
        if not self.isRunning():
            return -1
        return 0

    def status(self):
        return {
            'name': CONFIG.name,
            'status': 'running' if self.isRunning() else 'shutdown',
            'cluster': self.__clusterDict.values(),
            'runningTask': [
                {
                    'taskSn': taskState.taskSn,
                    'executor': taskState.executor,
                } for taskState in self.__taskQueue if taskState.executor
            ],
        }


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
        for name_ in taskManager.__dir__():
            if name_[0] == '_':
                continue
            func_ = getattr(taskManager, name_)
            if callable(func_):
                cls.register(name_, func_, )

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

        for name_, prop_ in TaskManager.__dict__.items():
            if name_[0] == '_':
                continue
            if callable(prop_):
                cls.register(name_, )

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
    'TaskManager', 'ManagerServer', 'ManagerClient', 'ManagerAdmin',
)
