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
        if workerConfig.stopEvent.is_set() or workerStopEvent.is_set():
            print(f'Worker {processID} exit @ {currentTimeStr()}.')
            exit()

        currentTime = time.time()
        workerConfig.pipe.send(
            ('alive', currentTime)
        )

        if currentTime - initTime > CONFIG.processLifeTime:
            print(f'Worker {processID} life end, exit for next.')
            exit()
        # -------------------- check event status --------------------

        try:
            # -------------------- get task config --------------------
            taskConfig: TaskConfig = proxyFunctionCall(
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
            workerConfig.pipe.send(('overtime', currentTime + taskConfig.timeout))

            print(f'Process {processID} get task {taskConfig.sn}.')
            try:
                runConfig = AutoTaskHandler.configUnpack(taskConfig)
            except:
                print(f'  Task  {taskConfig.sn} config invalid.')
                proxyFunctionCall(
                    workerConfig.taskManager.invalidConfig,
                    taskSn=taskConfig.sn
                )
                continue

            # -------------------- execute the function --------------------
            try:
                taskFunc = runConfig['func']
                result = taskFunc(
                    *runConfig['args'],
                    **runConfig['kwargs']
                )
            except Exception as err_:
                print(f'  Task {taskConfig.sn} run error.')
                proxyFunctionCall(
                    workerConfig.taskManager.taskError,
                    taskSn=taskConfig.sn,
                    errorText=str(err_),
                )
                continue

            print(f'  Task {taskConfig.sn} success.')

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
