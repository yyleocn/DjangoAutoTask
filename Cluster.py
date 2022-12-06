import signal
import time
from typing import TYPE_CHECKING
from multiprocessing import Event, current_process, Pipe, parent_process, Process
from typing import Callable

from . import Public

if TYPE_CHECKING:
    from .Dispatcher import DispatcherClient


# -------------------- sub process --------------------
class WorkerProcess:
    def __init__(
            self, *_,
            sn: int, localName: str, workerFunc,
            dispatcher: DispatcherClient, shutdownEvent,
    ):
        self.__sn = sn
        self.__workerProcess: Process | None = None
        self.__taskDispatcher: DispatcherClient = dispatcher
        self.__shutdownEvent = shutdownEvent
        self.__workerFunc = workerFunc

        self.__workerTimeLimit: int = 0
        self.__workerPipe, self.__pipe = Pipe()

        self.__localName: str = localName

        self.createProcess()

    def refreshWorkerTimeLimit(self, timeLimit=None):
        if timeLimit is None:
            self.__workerTimeLimit = Public.getNowStamp() + Public.CONFIG.taskTimeLimit + 2
            return
        self.__workerTimeLimit = Public.getNowStamp() + timeLimit + 2

    def createProcess(self):
        if parent_process():
            print('Only cluster process can create worker.')
            return

        while self.__pipe.poll():
            _ = self.__pipe.recv()

        self.refreshWorkerTimeLimit()

        self.__workerProcess = Process(
            target=self.__workerFunc,
            args=(
                Public.WorkerProcessConfig(
                    sn=self.__sn,
                    shutdownEvent=self.__shutdownEvent,
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

    def checkProcess(self):
        while self.__pipe.poll():
            code, value = self.__pipe.recv()
            match code:
                case 'alive':
                    self.refreshWorkerTimeLimit()
                case 'timeLimit':
                    self.refreshWorkerTimeLimit(value)

        if not self.isAlive() and not self.__shutdownEvent.is_set():
            self.createProcess()

        if self.__workerTimeLimit < time.time():
            self.workerTerminate()

    def workerTerminate(self):
        if not self.isAlive():
            return True

        print(f'-- Worker {self.__sn}|{self.__workerProcess.pid} timeout, terminate')
        self.__workerProcess.terminate()
        time.sleep(0.5)

    @property
    def pid(self):
        if not self.isAlive():
            return None
        return self.__workerProcess.pid

    @property
    def sn(self):
        return self.__sn


# -------------------- worker cluster --------------------
class WorkerCluster:
    def __init__(
            self, *_, dispatcherConn: DispatcherClient = None, workerFunc: Callable,
            localName: str = Public.CONFIG.name, poolSize: int = Public.CONFIG.poolSize,
    ):
        if dispatcherConn is None:
            raise Exception('Invalid task dispatcher')

        self.__processPool: list[WorkerProcess] = []
        self.__poolSize: int = poolSize
        self.__localName: str = localName

        self.__shutdownEvent = Event()
        self.__shutdownEvent.clear()

        self.__dispatcherStatus = 0
        self.__shutdown = False
        # set the exitEvent() when dispatcherStatus < 0 or exit = True

        self.__dispatcherConn: DispatcherClient = dispatcherConn
        self.__workerFunc = workerFunc

        self.__processCounter = 0

        def shutdownHandler(*_):
            print(f'Cluster {self.pid} receive shutdown signal @ {Public.currentTimeStr()}')
            self.shutdown()

        for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGILL,):
            signal.signal(sig, shutdownHandler)

        initTime = time.time()
        while True:
            if self.__shutdownEvent.is_set():
                break
            try:
                print(f'Cluster {self.pid} connecting task dispatcher.')
                self.__dispatcherConn.connect()
                break
            except Exception as err_:
                print(f'Connect fail, waiting for retry.')
                time.sleep(5)

        print(f'Cluster {self.pid} start to create process, pool size is {self.__poolSize}')

    def appendProcess(self):
        if len(self.__processPool) >= self.__poolSize:
            return None
        if not self.running:
            return None

        while len(self.__processPool) < self.__poolSize:
            self.__processCounter += 1
            process = WorkerProcess(
                sn=self.__processCounter,
                shutdownEvent=self.__shutdownEvent,
                workerFunc=self.__workerFunc,
                dispatcher=self.__dispatcherConn,
                localName=self.__localName,
            )
            self.__processPool.append(process)

    @property
    def pid(self):
        return current_process().pid

    @property
    def workerPid(self):
        return [
            worker.pid for worker in self.__processPool if worker.isAlive()
        ]

    @property
    def running(self):
        if self.__shutdown or self.__dispatcherStatus < 0:
            return False
        return True

    def run(self):
        dispatcherCheckTime = 0
        while True:
            if not self.running:
                self.__shutdownEvent.set()
            else:
                self.__shutdownEvent.clear()

            if self.__shutdown:
                break

            if time.time() - dispatcherCheckTime > 10:
                try:
                    pingRes = self.__dispatcherConn.ping(
                        {
                            'name': self.__localName,
                            'pid': self.workerPid,
                            'status': 'running' if self.running else 'shutdown',
                        }
                    )._getvalue()
                    self.__dispatcherStatus = pingRes
                    dispatcherCheckTime = time.time()
                except Exception as err_:
                    print(f'Cluster {self.pid} connect task dispatcher fail: {err_}')
                    time.sleep(5)
                    continue

            for subProcess in self.__processPool:
                subProcess.checkProcess()

            self.appendProcess()
            time.sleep(1)

        exit()

    @property
    def exitEvent(self):
        return self.__shutdownEvent

    def shutdown(self):
        self.__shutdownEvent.set()  # 激活关闭事件

        while True:
            allStop = True
            for subProcess in self.__processPool:
                subProcess.checkProcess()
                if subProcess.isAlive():
                    allStop = False

            if allStop:
                print(f'Cluster {self.pid} ready to exit @ {Public.currentTimeStr()}')
                self.__shutdown = True

                return None

            time.sleep(0.5)


__all__ = (
    'WorkerProcess', 'WorkerCluster',
)
