import time
import signal

from multiprocessing import current_process, Event

from .Component import SubProcessConfig, currentTimeStr, CONFIG, proxyFunctionCall, ProxyTimeoutException, TaskConfig
from .Handler import AutoTaskHandler


def workerFunc(workerConfig: SubProcessConfig, *args, **kwargs):
    initTime = time.time()
    pid = current_process().pid
    workerStopEvent = Event()
    workerStopEvent.clear()

    processID = f'{workerConfig.sn}-{pid}'
    workerConfig.taskManager.methodBound()

    print(f'* Worker {processID} start @ {currentTimeStr()}')

    def stopSignalHandler(*_, ):
        print(f'Worker {processID} receive stop signal @ {currentTimeStr()}')
        workerStopEvent.set()

    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGILL,):
        signal.signal(sig, stopSignalHandler)

    managerCheckTime = time.time()

    while True:
        # -------------------- exit event check --------------------
        if workerConfig.shutdownEvent.is_set() or workerStopEvent.is_set():
            print(f'Worker {processID} exit @ {currentTimeStr()}')
            exit()

        currentTime = time.time()

        # -------------------- work process life time --------------------
        if currentTime - initTime > CONFIG.processLifeTime:
            print(f'Worker {processID} life end, exit for next')
            exit()

        # -------------------- heart beat --------------------
        workerConfig.pipe.send(('alive', currentTime))

        try:
            # -------------------- get task config --------------------
            taskConfig: TaskConfig = proxyFunctionCall(
                func=workerConfig.taskManager.getTask,
                workerName=f'{workerConfig.localName}-{processID}',
            )

            # -------------------- refresh manager check time --------------------
            managerCheckTime = currentTime

            match taskConfig:
                case 1:
                    # no task in manager waiting for a long time.
                    time.sleep(5)
                    continue
                case -1:
                    # task manager is busy, waiting for a short time.
                    time.sleep(0.5)
                    continue

            # -------------------- config check & unpack --------------------
            print(f'Process {processID} get task {taskConfig.sn}')
            try:
                runConfig = AutoTaskHandler.configUnpack(taskConfig)
            except:
                print(f'  Task  {taskConfig.sn} config invalid')
                proxyFunctionCall(
                    workerConfig.taskManager.invalidConfig,
                    taskSn=taskConfig.sn
                )
                continue

            # -------------------- send overtime --------------------
            workerConfig.pipe.send(('overtime', currentTime + taskConfig.timeout))

            try:
                # -------------------- executor task --------------------
                taskFunc = runConfig['func']
                result = taskFunc(
                    *runConfig['args'],
                    **runConfig['kwargs']
                )
            except Exception as err_:
                # -------------------- after crash --------------------
                print(f'  Task {taskConfig.sn} run error: {err_}')
                proxyFunctionCall(
                    workerConfig.taskManager.taskError,
                    taskSn=taskConfig.sn,
                    errorText=str(err_),
                )
                continue

            print(f'  Task {taskConfig.sn} success')

            # -------------------- send result --------------------
            proxyFunctionCall(
                workerConfig.taskManager.taskSuccess,
                taskSn=taskConfig.sn,
                result=result,
            )

        # -------------------- catch the manager timeout exception --------------------
        except ProxyTimeoutException as err_:
            print(f'Task manager timeout @ worker {processID}')

            # -------------------- task manager timeout --------------------
            if currentTime - managerCheckTime < CONFIG.managerTimeLimit:
                time.sleep(5)
            else:
                print(f'Task manager closed, worker {processID} exit')
                exit()

        except Exception as err_:
            print(f'* Worker {processID} crash @ {currentTimeStr()} : {err_}')
