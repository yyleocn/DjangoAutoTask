from __future__ import annotations

import signal
import time
from typing import TYPE_CHECKING
from multiprocessing import Event, current_process, Pipe, parent_process, Process
from typing import Callable

from . import Public

if TYPE_CHECKING:
    from .Dispatcher import DispatcherClient


#        #####            #        ######
#       #     #           #        #     #
#       #        #     #  ######   #     #   # ###    #####    #####    #####    #####    #####
#        #####   #     #  #     #  ######    ##      #     #  #        #     #  #        #
#             #  #     #  #     #  #         #       #     #  #        #######   ####     ####
#       #     #  #    ##  #     #  #         #       #     #  #        #             #        #
#        #####    #### #  ######   #         #        #####    #####    #####   #####    #####

class WorkerProcess:
    def __init__(
            self, *_,
            sn: int, localName: str, workerFunc,
            dispatcher: DispatcherClient, clusterOffline,
    ):
        self.__sn = sn
        self.__workerProcess: Process | None = None
        self.__taskDispatcher: DispatcherClient = dispatcher
        self.__clusterOffline = clusterOffline
        self.__workerFunc = workerFunc
        self.__localName: str = localName

        self.__workerTimeLimit: int = 0
        self.__workerPipe, self.__pipe = Pipe()

    def __str__(self):
        pid = self.pid
        if pid:
            return f'作业器-{self.__sn:02d}-{pid}'

        return f'作业器-{self.__sn:02d}'

    def refreshWorkerTimeLimit(self, timeLimit=None):
        if timeLimit is None:
            self.__workerTimeLimit = Public.getNowStamp() + Public.CONFIG.execTimeLimit + 2
            return

        self.__workerTimeLimit = Public.getNowStamp() + timeLimit + 2

    def createProcess(self):
        if parent_process():
            print('只有主进程可以建立子进程')
            return

        while self.__pipe.poll():
            _ = self.__pipe.recv()

        self.refreshWorkerTimeLimit()

        self.__workerProcess = Process(
            target=self.__workerFunc,
            args=(
                Public.WorkerProcessConfig(
                    sn=self.__sn,
                    clusterOffline=self.__clusterOffline,
                    dispatcherClient=self.__taskDispatcher,
                    pipe=self.__workerPipe,
                    localName=self.__localName,
                ),
            ),
        )
        self.__workerProcess.start()

        time.sleep(0.5)

    def isAlive(self):
        if not self.__workerProcess:
            return False
        return self.__workerProcess.is_alive()

    def checkProcess(self) -> int:
        while self.__pipe.poll():
            code, value = self.__pipe.recv()
            if code == 'alive':
                self.refreshWorkerTimeLimit()
            if code == 'timeLimit':
                self.refreshWorkerTimeLimit(value)

        if self.__workerTimeLimit < time.time():
            self.workerTerminate()
            time.sleep(1)

        if self.__clusterOffline.is_set():
            return 0

        if not self.isAlive():
            self.createProcess()
            return 1

        return 0

    def workerTerminate(self):
        if not self.isAlive():
            return True

        self.__workerProcess.terminate()
        print(f'{self} 超时，已终止')
        time.sleep(0.5)

    @property
    def pid(self):
        if not self.isAlive():
            return None
        return self.__workerProcess.pid

    @property
    def sn(self):
        return self.__sn


#         ####     ##                         #
#        #    #     #                         #
#       #           #     #     #   #####   ######    #####    # ###
#       #           #     #     #  #          #      #     #   ##
#       #           #     #     #   ####      #      #######   #
#        #    #     #     #    ##       #     #      #         #
#         ####     ###     #### #  #####       ###    #####    #

class WorkerCluster:
    def __init__(
            self, *_, dispatcherConn: DispatcherClient, workerFunc: Callable,
            localName: str = Public.CONFIG.name, poolSize: int = Public.CONFIG.poolSize,
    ):
        if dispatcherConn is None:
            raise Exception('需要调度器连接')

        self.__dispatcherConn: DispatcherClient = dispatcherConn

        self.__workerFunc = workerFunc
        self.__localName: str = localName

        self.__clusterOffline = Event()
        self.__clusterOffline.set()

        self.__hasTask = Event()
        self.__hasTask.clear()

        self.__shutdown = False

        self.__poolSize: int = poolSize
        self.__processPool: tuple[WorkerProcess, ...] = tuple(
            WorkerProcess(
                sn=index + 1,
                clusterOffline=self.__clusterOffline,
                workerFunc=self.__workerFunc,
                dispatcher=self.__dispatcherConn,
                localName=self.__localName,
            )
            for index in range(poolSize)
        )

        def shutdownHandler(*_):
            print(f'{self} 接收到关闭信号 @ {Public.currentTimeStr()}')
            self.__shutdown = True
            for subProcess in self.__processPool:
                if not subProcess:
                    continue
                if subProcess.isAlive():
                    self.offline()
                    return

            if self.__shutdown:
                exit()

        for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGILL,):
            signal.signal(sig, shutdownHandler)

        initTime = time.time()

        print(f'{self} >>> 初始化, 子进程 {self.__poolSize} 个')

    def __str__(self):
        return f'{self.__localName}-群集-{self.pid}'

    def checkSubProcess(self):
        if self.__clusterOffline.is_set():
            return None

        for subProcess in self.__processPool:
            if subProcess.checkProcess():
                return

    @property
    def pid(self):
        return current_process().pid

    @property
    def workerPid(self):
        return [
            worker.pid for worker in self.__processPool if worker
        ]

    @property
    def isOnline(self):
        return not self.__clusterOffline.is_set()

    def run(self):
        dispatcherCheckTime = 0
        while True:
            if self.__clusterOffline.is_set():
                if self.__shutdown:
                    break

                print(f'{self} >>> 离线 @ {Public.currentTimeStr()}')
                time.sleep(5)

            if time.time() - dispatcherCheckTime > 10:
                try:
                    pingRes = self.__dispatcherConn.ping(
                        {
                            'name': self.__localName,
                            'pid': self.workerPid,
                            'status': 'offline' if self.__clusterOffline.is_set() else 'online',
                        }
                    )._getvalue()  # Ping 连接 dispatcher
                except Exception as err_:
                    print(f'{self} >>> 调度器连接失败: {err_}')
                    time.sleep(5)
                    raise Exception from err_

                if pingRes > 0:
                    dispatcherCheckTime = time.time()  # 正常状态更新调度器时间
                    if self.__clusterOffline.is_set():
                        print(f'{self} >>> 上线 @ {Public.currentTimeStr()}')
                        self.__clusterOffline.clear()

                if pingRes < 0:
                    if pingRes == -1:  # 结果等于 -1 表示调度器已关闭,执行关闭程序
                        self.offline()

                    if pingRes < -10:  # 结果小于 -10 表示请求错误，
                        print(f'{self} >>> 调度器通讯错误: {pingRes}')

                if pingRes == 0:
                    dispatcherCheckTime = time.time()  # 正常状态更新调度器时间
                    pass  # 空闲状态没有操作

                if time.time() - dispatcherCheckTime > Public.CONFIG.dispatcherTimeout:
                    self.offline()

            self.checkSubProcess()
            time.sleep(1)

    def offline(self):
        if self.__clusterOffline.is_set():
            return

        self.__clusterOffline.set()  # 激活关闭事件
        print(f'{self} >>> 准备下线')

        while True:
            subProcessAllExit = True
            for subProcess in self.__processPool:
                if not subProcess:
                    continue

                subProcess.checkProcess()
                if subProcess.isAlive():
                    subProcessAllExit = False

            if subProcessAllExit:
                print(f'{self} >>> 已下线')
                return None

            time.sleep(1)


__all__ = (
    'WorkerProcess', 'WorkerCluster',
)
