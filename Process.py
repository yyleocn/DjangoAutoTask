import time
import random
import signal
from multiprocessing import current_process, Event

from .Component import SubProcessConfig, currentTimeStr, TaskConfig, CONFIG
from .Handler import AutoTaskHandler


def processFunc(processConfig: SubProcessConfig, *args, **kwargs):
    initTime = time.time()
    pid = current_process().pid
    stopEvent = Event()
    stopEvent.clear()

    processID = f'{processConfig.sn}|{pid}'

    print(f'* Process {processID} start @ {currentTimeStr()}.')

    def stopSignalHandler(*_, ):
        print(f'Process {processID} receive stop signal @ {currentTimeStr()}.')
        stopEvent.set()

    signal.signal(signal.SIGINT, stopSignalHandler)
    signal.signal(signal.SIGTERM, stopSignalHandler)

    systemCheckTime = time.time()

    while True:
        if time.time() - initTime > CONFIG.processLifeTime:
            print(f'Process {processID} life end, exit for next.')
            exit()
        # -------------------- check event status --------------------
        if processConfig.stopEvent.is_set() or stopEvent.is_set():
            print(f'Process {processID} exit @ {currentTimeStr()}.')
            exit()

        # -------------------- task manager timeout --------------------
        if time.time() - systemCheckTime > CONFIG.taskManagerTimeout:
            print(f'Task manager timeout , process {processID} exit.')
            exit()

        # -------------------- send alive time --------------------
        processConfig.pipe.send(time.time())

        # -------------------- get task config --------------------
        try:
            taskConfig = processConfig.taskManager.getTask(
                processor=f'{processConfig.localName}-{processID}'
            )._getvalue()
            if not isinstance(taskConfig, TaskConfig):
                print(f'Task manager busy, process {processID} ----------')
                time.sleep(0.2)
                continue
            systemCheckTime = time.time()
        except BaseException as err_:
            print(err_)
            time.sleep(2)
            continue

        try:
            runConfig = AutoTaskHandler.configUnpack(taskConfig)
        except:
            runConfig = None
            processConfig.taskManager.configError(taskSn=taskConfig.sn)

        # -------------------- function content --------------------
        if runConfig:
            print(f'''Process {pid} get task {taskConfig.sn}:
    {taskConfig}''')
            taskFunc = runConfig['func']
            taskArgs = runConfig['args']
            taskKwargs = runConfig['kwargs']

            result = taskFunc(*taskArgs, **taskKwargs)

            processConfig.taskManager.taskSuccess(taskSn=taskConfig.sn, result=result)

        time.sleep(5 + random.random() * 5)
