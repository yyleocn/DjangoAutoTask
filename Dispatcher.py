from __future__ import annotations

import time
import signal
from typing import Callable

from multiprocessing import (current_process, )
from multiprocessing.managers import BaseManager

from django.core.exceptions import AppRegistryNotReady
from django.apps.registry import apps

try:
    apps.check_apps_ready()
except AppRegistryNotReady:
    import django

    django.setup()

from . import Public, Handler

# from .Handler import AutoTaskHandler

if Public.TYPE_CHECKING:
    from .Public import (TaskState, )


#   #####       #                                  #               #
#   #    #                                         #               #
#   #     #   ###      #####   ######    ######  ######    #####   ######    #####    # ###
#   #     #     #     #        #     #  #     #    #      #        #     #  #     #   ##
#   #     #     #      ####    #     #  #     #    #      #        #     #  #######   #
#   #    #      #          #   #     #  #    ##    #      #        #     #  #         #
#   #####     #####   #####    ######    #### #     ###    #####   #     #   #####    #
#                              #

class TaskDispatcher:
    __pid: int = None

    def __init__(self, *_, **kwargs):
        self.__taskQueueLock: bool = False
        self.__taskQueue: list[TaskState] = []
        # self.__taskDict: dict[int, TaskState] = {}
        self.__exit: bool = False
        self.__shutdown: bool = False
        self.__pid = current_process().pid
        self.__clusterDict = {}
        self.__taskBlockSet: set[str] = set()

        print(f'Task dispatcher {self.pid} init')

        if Public.CONFIG.handlerClass:
            try:
                handlerClass = Public.importComponent(Public.CONFIG.handlerClass)
            except:
                raise Exception('Handler class not exist')
            if not issubclass(handlerClass, Handler.AutoTaskHandler):
                raise Exception('Invalid handler class')
            self.__handler = handlerClass()

        else:
            self.__handler = Handler.AutoTaskHandler()

        def shutdownHandler(*_, ):
            print(f'Dispatcher {self.pid} receive stop signal @ {Public.currentTimeStr()}')
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
        if not self.__shutdown:
            print('调度器未离线，请离线后再关闭')
            return

        self.__exit = True
        time.sleep(5)
        exit()

    def refreshTaskQueue(self):
        # --------------- lock queue --------------------
        self.__taskQueueLock = True

        if not self.isRunning():
            return

        runningBlockKeySet: set[str] = set(
            taskState.taskData.blockKey for taskState in self.__taskQueue
            if taskState.workerName is not None and taskState.taskData.blockKey
        )

        runningBlockKeySet.update(
            self.__handler.getRunningBlockKey()
        )

        # --------------- 查询获取新队列 --------------------
        newQueue: tuple[TaskState] = self.__handler.getTaskQueue(limit=Public.CONFIG.queueSize)

        # --------------- set new queue --------------------
        self.__taskQueue = newQueue
        self.__taskBlockSet = runningBlockKeySet

        print(f'调度器队列已刷新, 当前任务总数 {len(self.__taskQueue)}')

        # --------------- unlock queue --------------------
        self.__taskQueueLock = False

    def statusCode(self):
        if not self.isRunning():
            return -1  # 关闭状态为 -1

        for taskState in self.__taskQueue:
            if not taskState.workerName:
                return 1  # 正常状态为 1

        return 0  # 空闲状态为0

    def getTask(self, *args, workerName: str = None, combine: int = None, **kwargs) -> str | int:
        while True:
            state = self.statusCode()
            if state <= 0:
                return state  # 关闭/空闲 状态直接返回

            if self.__taskQueueLock:
                return 1  # 忙碌状态返回 1
            selectTask: TaskState | None = None

            # --------------- search all queue --------------------
            if selectTask is None:
                for taskState in self.__taskQueue:
                    if not taskState.done \
                            and taskState.workerName is None \
                            and taskState.taskData.blockKey not in self.__taskBlockSet:
                        selectTask = taskState
                        break

            # --------------- no task return 1 --------------------
            if selectTask is None:
                return 0  # 空闲状态返回 0

            # --------------- 锁定任务 --------------------
            selectTask.workerName = workerName
            if not selectTask.workerName == workerName:
                continue

            # --------------- set task running to db --------------------
            selectTask.endTime = self.__handler.setTaskRunning(
                taskSn=selectTask.taskData.taskSn,
                workerName=selectTask.workerName,
            )
            if not selectTask.endTime:
                continue

            if isinstance(selectTask.taskData.blockKey, str):
                self.__taskBlockSet.add(selectTask.taskData.blockKey)
            print(f'{workerName} 获取任务 {selectTask.taskData.taskSn}')
            return selectTask.exportToWorker()

    def taskSuccess(self, *_, taskSn: int = None, result: any = None, execWarn: str | None = None, ):
        print(f'{taskSn} 任务完成, 结果是 {result}')
        return self.__handler.setTaskRecSuccess(
            taskSn=taskSn, result=result, execWarn=execWarn,
        )

    def taskCrash(self, *_, taskSn: int, message: str, detail: str, execWarn: str | None = None, ):
        print(f'{taskSn} 任务失败: {message}')
        return self.__handler.setTaskRecCrash(
            taskSn=taskSn, message=message, detail=detail, execWarn=execWarn,
        )

    def invalidConfig(self, *_, runningWorkerName: str, detail: str = None):
        return self.__handler.setTaskRecInvalidConfig(runningWorkerName=runningWorkerName, detail=detail)

    def ping(self, state, *_, ):
        if isinstance(state, dict):
            clusterName = state.get('name')
            clusterPid = state.get('pid')
            clusterStatus = state.get('status')
            print(f'群集 {clusterName} ping 了一下')
            self.__clusterDict[clusterName] = state

        else:
            print('Ping 消息无效')
            return -99  # -99 表示错误信息

        return self.statusCode()  # 0 空闲

    def status(self):
        return {
            'name': Public.CONFIG.name,
            'state': 'running' if self.isRunning() else 'shutdown',
            'cluster': self.__clusterDict.values(),
            'runningTask': [
                {
                    'taskSn': taskState.taskData.taskSn,
                    'executor': taskState.workerName,
                } for taskState in self.__taskQueue if taskState.workerName
            ],
        }


#        #####
#       #     #
#       #         #####    # ###   ##   ##   #####    # ###
#        #####   #     #   ##       #   #   #     #   ##
#             #  #######   #         # #    #######   #
#       #     #  #         #         # #    #         #
#        #####    #####    #          #      #####    #

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


#          #           #              #
#          #           #
#         ###     ######  ### ##    ###     # ####
#         # #    #     #  #  #  #     #     ##    #
#        #####   #     #  #  #  #     #     #     #
#        #   #   #     #  #  #  #     #     #     #
#       ##   ##   ######  #     #   #####   #     #

class DispatcherAdmin(BaseManager):
    __methodBounded = False
    shutdownDispatcher: Callable
    status: Callable
    isRunning: Callable
    refreshTaskQueue: Callable

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


#         ####     ##        #                         #
#        #    #     #                                  #
#       #           #      ###      #####   # ####   ######
#       #           #        #     #     #  ##    #    #
#       #           #        #     #######  #     #    #
#        #    #     #        #     #        #     #    #
#         ####     ###     #####    #####   #     #     ###

class DispatcherClient(BaseManager):
    ping: Callable
    getTask: Callable

    taskSuccess: Callable
    taskCrash: Callable
    taskTimeout: Callable
    invalidConfig: Callable

    __methodBounded = False
    dispatcherClientFunc = (
        'ping', 'getTask',
        'taskSuccess', 'taskCrash', 'taskTimeout', 'invalidConfig',
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
