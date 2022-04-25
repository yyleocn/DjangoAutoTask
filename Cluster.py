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


__all__ = (
    'WorkerProcess', 'WorkerCluster',
)
