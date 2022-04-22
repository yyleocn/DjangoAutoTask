import time
import random
import signal
from multiprocessing import current_process, Event

from .Component import SubProcessConfig, currentTimeStr, TaskConfig, CONFIG
from .Handler import AutoTaskHandler


def workerFunc(workerConfig: SubProcessConfig, *args, **kwargs):
    initTime = time.time()
    pid = current_process().pid
    workerStopEvent = Event()
    workerStopEvent.clear()

    processID = f'{workerConfig.sn}|{pid}'
    workerConfig.taskManager.methodBound()

    print(f'* Worker {processID} start @ {currentTimeStr()}.')

    def stopSignalHandler(*_, ):
        print(f'Worker {processID} receive stop signal @ {currentTimeStr()}.')
        workerStopEvent.set()

    signal.signal(signal.SIGINT, stopSignalHandler)
    signal.signal(signal.SIGTERM, stopSignalHandler)

    managerCheckTime = time.time()

    def pipePing(processTimeout):
        workerConfig.pipe.send((
            int(time.time()), processTimeout,
        ))

    while True:
        currentTime = time.time()
        if currentTime - initTime > CONFIG.processLifeTime:
            print(f'Worker {processID} life end, exit for next.')
            exit()
        # -------------------- check event status --------------------
        if workerConfig.stopEvent.is_set() or workerStopEvent.is_set():
            print(f'Worker {processID} exit @ {currentTimeStr()}.')
            exit()

        # -------------------- task manager timeout --------------------
        if currentTime - managerCheckTime > CONFIG.managerTimeout:
            print(f'Task manager timeout , process {processID} exit.')
            exit()

        # -------------------- send alive time --------------------

        # -------------------- get task config --------------------
        timeout = None
        try:
            taskConfig = workerConfig.taskManager.getTask(
                processor=f'{workerConfig.localName}-{processID}'
            )._getvalue()
            if not isinstance(taskConfig, TaskConfig):
                print(f'Task manager busy, process {processID} ----------')
                time.sleep(0.2)
                continue
            timeout = taskConfig.sn
            managerCheckTime = currentTime
        except BaseException as err_:
            print(f'Worker {pid} error', err_)
            pipePing(None)
            time.sleep(2)
            continue

        pipePing(taskConfig.timeout)

        try:
            runConfig = AutoTaskHandler.configUnpack(taskConfig)
        except:
            runConfig = None
            workerConfig.taskManager.configError(taskSn=taskConfig.sn)

        # -------------------- function content --------------------
        if runConfig:
            print(f'Process {pid} get task {taskConfig.sn}:\n    {taskConfig}')
            taskFunc = runConfig['func']
            taskArgs = runConfig['args']
            taskKwargs = runConfig['kwargs']

            result = taskFunc(*taskArgs, **taskKwargs)

            workerConfig.taskManager.taskSuccess(taskSn=taskConfig.sn, result=result)

        time.sleep(5 + random.random() * 5)
