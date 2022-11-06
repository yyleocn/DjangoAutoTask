import time
import signal

from multiprocessing import current_process, Event

from .Component import WorkerProcessConfig, currentTimeStr, CONFIG, remoteProxyCall, ProxyTimeout, TaskConfig
from .models import TaskRec
from .Handler import AutoTaskHandler


def workerFunc(workerConfig: WorkerProcessConfig, *args, **kwargs):
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
        if currentTime - initTime > CONFIG.workerLifeTime:
            print(f'Worker {processID} life end, exit for next')
            exit()

        # -------------------- heart beat --------------------
        workerConfig.pipe.send(('alive', currentTime))

        try:
            # -------------------- get task config --------------------
            taskConfig: TaskConfig = remoteProxyCall(
                func=workerConfig.taskManager.getTask,
                workerName=f'{workerConfig.localName}-{processID}',
            )  # 从 manager 获取 taskConfig

            # -------------------- refresh manager check time --------------------
            managerCheckTime = currentTime

            if taskConfig == 1:  # 1 表示目前没用任务，暂停 5 秒
                time.sleep(5)
                continue

            if taskConfig == -1:  # -1 表示忙碌状态，暂停 0.5 秒
                time.sleep(0.5)
                continue

            # -------------------- config check & unpack --------------------
            print(f'Process {processID} get task {taskConfig.sn}')
            try:
                taskFunc, taskArgs, taskKwargs = taskConfig.unpack()  # 解析 taskConfig 的数据
            except:
                print(f'  Task  {taskConfig.sn} config invalid')
                remoteProxyCall(workerConfig.taskManager.invalidConfig, taskSn=taskConfig.sn)  # 发送 invalidConfig 错误
                continue

            # -------------------- send time limit --------------------
            workerConfig.pipe.send(('timeLimit', currentTime + taskConfig.timeLimit))

            try:
                # -------------------- executor task --------------------
                result = taskFunc(*taskArgs, **taskKwargs)
            except Exception as err_:
                # -------------------- after crash --------------------
                print(f'  Task {taskConfig.sn} run error: {err_}')
                remoteProxyCall(
                    workerConfig.taskManager.taskError,
                    taskSn=taskConfig.sn,
                    errorDetail=str(err_),
                    errorCode=TaskRec.ErrorCodeChoice.crash,
                )  # 发送 taskError 错误
                continue

            print(f'  Task {taskConfig.sn} success')

            # -------------------- send result --------------------
            remoteProxyCall(
                workerConfig.taskManager.taskSuccess,
                taskSn=taskConfig.sn,
                result=result,
            )  # 发送 taskSuccess

        # -------------------- catch the manager timeout exception --------------------
        except ProxyTimeout as err_:
            print(f'Task manager timeout @ worker {processID}')

            # -------------------- task manager timeout --------------------
            if currentTime - managerCheckTime < CONFIG.managerTimeLimit:
                time.sleep(5)
            else:
                print(f'Task manager timeout, worker {processID} exit')
                exit()

        except Exception as err_:
            print(f'* Worker {processID} crash @ {currentTimeStr()} : {err_}')
