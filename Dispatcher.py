from __future__ import annotations

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

from .Component import (getNowStamp, TaskInfo, CONFIG, importComponent, currentTimeStr, TaskConfig, )
from .Handler import AutoTaskHandler


#     #######                    #        #     #
#        #                       #        ##   ##
#        #      ######   #####   #   ##   # # # #   ######  # ####    ######   ######   #####    # ###
#        #     #     #  #        #  #     #  #  #  #     #  ##    #  #     #  #     #  #     #   ##
#        #     #     #   ####    ###      #     #  #     #  #     #  #     #  #     #  #######   #
#        #     #    ##       #   #  #     #     #  #    ##  #     #  #    ##   ######  #         #
#        #      #### #  #####    #   ##   #     #   #### #  #     #   #### #        #   #####    #
#                                                                              #####

class TaskDispatcher:
    __pid: int = None

    def __init__(self, *_, **kwargs):
        self.__taskQueueLock: bool = False
        self.__taskQueue: list[TaskInfo] = []
        self.__taskDict: dict[int, TaskInfo] = {}
        self.__exit: bool = False
        self.__shutdown: bool = False
        self.__pid = current_process().pid
        self.__clusterDict = {}

        print(f'Task dispatcher {self.pid} init')

        if CONFIG.handlerClass:
            try:
                handlerClass = importComponent(CONFIG.handlerClass)
            except:
                raise Exception('Handler class not exist')
            if not issubclass(handlerClass, AutoTaskHandler):
                raise Exception('Invalid handler class')
            self.__handler = handlerClass()

        else:
            self.__handler = AutoTaskHandler()

        def shutdownHandler(*_, ):
            print(f'Dispatcher {self.pid} receive stop signal @ {currentTimeStr()}')
            self.exit()

        for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGILL,):
            signal.signal(sig, shutdownHandler)

    def shutdownDispatcher(self):
        self.__shutdown = True
        return 'The TaskManger is set to shutdown'

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

        currentTime = getNowStamp()

        # --------------- remove overtime task --------------------
        for taskInfo in self.__taskQueue:
            if taskInfo.done:
                continue
            if taskInfo.timeout is None:
                continue
            if taskInfo.timeout + 5 < currentTime:
                self.taskTimeout(taskSn=taskInfo.taskSn)
                self.removeTask(taskSn=taskInfo.taskSn)

        if not self.isRunning():
            return

        # --------------- get executing task --------------------
        runningTask: list[TaskInfo] = [
            taskState for taskState in self.__taskQueue
            if taskState.executor is not None
        ]

        executingTaskSn: set[int] = {
            taskState.taskSn for taskState in runningTask
        }

        # --------------- get append task --------------------
        appendTask: list[TaskInfo] = [
            taskState for taskState in self.__handler.getTaskQueue(limit=CONFIG.queueSize)
            if taskState.taskSn not in executingTaskSn
        ]

        # --------------- sort by priority --------------------
        newQueue: list[TaskInfo] = [
                                       taskRec for taskRec in [*runningTask, *appendTask, ] if not taskRec.done
                                   ][:CONFIG.queueSize]

        # --------------- new queue sort --------------------
        newQueue.sort(key=attrgetter('priority', 'taskSn'))

        # --------------- set new queue --------------------
        self.__taskDict = {taskRec.taskSn: taskRec for taskRec in newQueue}
        self.__taskQueue = newQueue

        print(f'TaskDispatcher refreshed queue, current count is {len(self.__taskQueue)}')

        # --------------- unlock queue --------------------
        self.__taskQueueLock = False

    def getTask(self, *args, workerName: str = None, combine: int = None, **kwargs) -> TaskConfig | int:
        if not self.isRunning():
            return -1  # 已关闭返回 -1

        if self.__taskQueueLock:
            return 1  # 队列锁定返回 1

        selectTask: TaskInfo | None = None

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
            return 0  # 没有任务返回 0

        # --------------- lock the task --------------------
        selectTask.executor = workerName

        # --------------- set task running to db --------------------
        selectTask.timeout = self.__handler.setTaskRunning(
            taskSn=selectTask.taskSn,
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
        return self.__handler.setTaskRecSuccess(taskSn=taskSn, result=result, )

    def taskCrash(self, *_, taskSn: int, detail: str, ):
        print(f'  Task {taskSn} crash: {detail}')
        self.removeTask(taskSn)
        return self.__handler.setTaskRecCrash(taskSn=taskSn, detail=detail)

    def taskTimeout(self, *_, taskSn: int, ):
        print(f'  Task {taskSn} timeout')
        return self.__handler.setTaskTimeout(taskSn=taskSn)

    def invalidConfig(self, *_, taskSn: int):
        return self.__handler.setTaskRecInvalidConfig(taskSn=taskSn, detail='')

    def ping(self, state, *_, ):
        if isinstance(state, dict):
            clusterName = state.get('name')
            clusterPid = state.get('pid')
            clusterStatus = state.get('status')
            print(f'Cluster {clusterName} ping task dispatcher')
            self.__clusterDict[clusterName] = state

        else:
            print('Invalid ping message')
            return -1

        if not self.isRunning():
            return 0
        return 1

    def status(self):
        return {
            'name': CONFIG.name,
            'state': 'running' if self.isRunning() else 'shutdown',
            'cluster': self.__clusterDict.values(),
            'runningTask': [
                {
                    'taskSn': taskState.taskSn,
                    'executor': taskState.executor,
                } for taskState in self.__taskQueue if taskState.executor
            ],
        }


# -------------------- sync dispatcher --------------------

class DispatcherServer(BaseManager):
    __methodBounded = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.methodBound()

    @classmethod
    def methodBound(cls):
        if cls.__methodBounded:
            return

        taskDispatcher = TaskDispatcher()
        for name_ in taskDispatcher.__dir__():
            if name_[0] == '_':
                continue
            func_ = getattr(taskDispatcher, name_)
            if callable(func_):
                cls.register(name_, func_, )

        cls.__methodBounded = True


class DispatcherAdmin(BaseManager):
    __methodBounded = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.methodBound()

    @classmethod
    def methodBound(cls):
        if cls.__methodBounded:
            return

        for name_, prop_ in TaskDispatcher.__dict__.items():
            if name_[0] == '_':
                continue
            if callable(prop_):
                cls.register(name_, )

        cls.__methodBounded = True


class DispatcherClient(BaseManager):
    __methodBounded = False
    dispatcherClientFunc = (
        'ping', 'getTask',
        'configError', 'taskSuccess', 'taskError', 'taskExpire',
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.methodBound()

    @classmethod
    def methodBound(cls):
        if cls.__methodBounded:
            return

        for name_ in cls.dispatcherClientFunc:
            cls.register(name_)

        cls.__methodBounded = True


__all__ = (
    'TaskDispatcher', 'DispatcherServer', 'DispatcherClient', 'DispatcherAdmin',
)
