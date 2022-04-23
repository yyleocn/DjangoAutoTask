import time
import signal

from multiprocessing import current_process, Event

from .Component import SubProcessConfig, currentTimeStr, CONFIG, proxyFunctionCall, ProxyTimeoutException
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

    while True:
        currentTime = time.time()
        workerConfig.pipe.send(
            ('alive', currentTime)
        )

        if currentTime - initTime > CONFIG.processLifeTime:
            print(f'Worker {processID} life end, exit for next.')
            exit()
        # -------------------- check event status --------------------
        if workerConfig.stopEvent.is_set() or workerStopEvent.is_set():
            print(f'Worker {processID} exit @ {currentTimeStr()}.')
            exit()

        try:
            # -------------------- get task config --------------------
            taskConfig = proxyFunctionCall(
                func=workerConfig.taskManager.getTask,
                workerName=f'{workerConfig.localName}-{processID}',
            )

            managerCheckTime = currentTime

            match taskConfig:
                case -1:
                    print(f'Task manager busy, worker {processID} ----------')
                    time.sleep(0.2)
                    continue
                case 1:
                    print(f'No task in manager, worker {processID} is waiting.')
                    time.sleep(5)
                    continue

            # -------------------- send alive time --------------------
            workerConfig.pipe.send(('overtime', currentTime + taskConfig.overTime))

            try:
                runConfig = AutoTaskHandler.configUnpack(taskConfig)
            except:
                runConfig = None
                counter = 0
                proxyFunctionCall(
                    workerConfig.taskManager.invalidConfig,
                    taskSn=taskConfig.sn
                )
                continue

            print(f'Process {pid} get task {taskConfig.sn}:\n    {taskConfig}')

            # -------------------- execute the function --------------------
            try:
                taskFunc = runConfig['func']
                result = taskFunc(
                    *runConfig['args'],
                    **runConfig['kwargs']
                )
            except Exception as err_:
                proxyFunctionCall(
                    workerConfig.taskManager.taskError,
                    taskSn=taskConfig.sn,
                    errorText=str(err_),
                )
                continue

            proxyFunctionCall(
                workerConfig.taskManager.taskSuccess,
                taskSn=taskConfig.sn,
                result=result,
            )

        except ProxyTimeoutException as err_:
            print(f'Task manager timeout @ worker {processID}.')
            # -------------------- task manager timeout --------------------
            if currentTime - managerCheckTime > CONFIG.managerTimeLimit:
                print(f'Task manager closed, worker {processID} exit.')
                exit()

            time.sleep(5)
        except Exception as err_:
            print(f'* Worker {processID} crash @ {currentTimeStr()} : {err_}')
