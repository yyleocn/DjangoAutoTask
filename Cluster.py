import signal
import time
from multiprocessing import Event, current_process, Pipe, parent_process, Process
from multiprocessing.managers import BaseManager
from typing import Callable

from AutoTask.Component import CONFIG, currentTimeStr, SubProcessConfig


# -------------------- sub process --------------------
class WorkerProcess:
    __process = None

    def __init__(
            self, *_,
            sn: int,
            manager: BaseManager,
            shutdownEvent,
            processFunc,
            localName: str,
    ):
        self.__sn = sn
        self.__taskManager = manager
        self.__shutdownEvent = shutdownEvent
        self.__processFunc = processFunc

        self.__processOvertime: int = time.time() + CONFIG.taskTimeout * 2
        self.__processPipe, self.__pipe = Pipe()
        self.__localName: str = localName

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
                        shutdownEvent=self.__shutdownEvent,
                        taskManager=self.__taskManager,
                        pipe=self.__processPipe,
                        localName=self.__localName,
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
        if not self.isAlive() and not self.__shutdownEvent.is_set():
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


# -------------------- worker cluster --------------------
class WorkerCluster:
    def __init__(
            self, *_,
            managerCon: BaseManager = None,
            localName: str = CONFIG.name,
            poolSize: int = CONFIG.poolSize,
            processFunc: Callable,
    ):
        if managerCon is None:
            raise Exception('Invalid task manager')

        self.__processPool: list[WorkerProcess] = []
        self.__poolSize: int = poolSize
        self.__localName: str = localName

        self.__shutdownEvent = Event()
        self.__shutdownEvent.clear()

        self.__managerStatus = 0
        self.__shutdown = False
        # set the exitEvent() when managerStatus < 0 or exit = True

        self.__managerCon: BaseManager = managerCon
        self.__processFunc = processFunc

        self.__processCounter = 0

        def shutdownHandler(*_, ):
            print(f'Cluster {self.pid} receive shutdown signal @ {currentTimeStr()}')
            self.shutdown()

        for sig in [
            signal.SIGINT,
            # signal.SIGHUP,
            signal.SIGTERM,
            signal.SIGILL,
        ]:
            signal.signal(sig, shutdownHandler)

        initTime = time.time()
        while True:
            if self.__shutdownEvent.is_set():
                break
            try:
                self.__managerCon.connect()
                break
            except Exception as err_:
                print(f'Cluster {self.pid} connect task manager fail, waiting')
                time.sleep(5)

        print(f'Cluster {self.pid} start to create process, pool size is {self.__poolSize}')

    def appendProcess(self):
        if not self.running:
            return None

        if len(self.__processPool) >= self.__poolSize:
            return None
        while len(self.__processPool) < self.__poolSize:
            self.__processCounter += 1
            process = WorkerProcess(
                sn=self.__processCounter,
                shutdownEvent=self.__shutdownEvent,
                processFunc=self.__processFunc,
                manager=self.__managerCon,
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
        if self.__shutdown or self.__managerStatus < 0:
            return False
        return True

    def run(self):
        managerCheckTime = 0
        while True:
            if not self.running:
                self.__shutdownEvent.set()
            else:
                self.__shutdownEvent.clear()

            if self.__shutdown:
                break

            if time.time() - managerCheckTime > 10:
                try:
                    pingRes = self.__managerCon.ping(
                        {
                            'name': self.__localName,
                            'pid': self.workerPid,
                            'status': 'running' if self.running else 'shutdown',
                        }
                    )._getvalue()
                    self.__managerStatus = pingRes
                    managerCheckTime = time.time()
                except Exception as err_:
                    print(f'Cluster {self.pid} connect task manager fail: {err_}')
                    time.sleep(2)
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
                print(f'Cluster {self.pid} ready to exit @ {currentTimeStr()}')
                self.__shutdown = True

                return None

            time.sleep(0.5)


__all__ = (
    'WorkerProcess', 'WorkerCluster',
)
