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
    workerConfig.taskDispatcher.methodBound()

    print(f'* Worker {processID} start @ {currentTimeStr()}')

    def stopSignalHandler(*_, ):
        print(f'Worker {processID} receive stop signal @ {currentTimeStr()}')
        workerStopEvent.set()

    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGILL,):
        signal.signal(sig, stopSignalHandler)

    dispatcherCheckTime = time.time()

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
            taskConfig: TaskConfig | int = remoteProxyCall(
                func=workerConfig.taskDispatcher.getTask,
                workerName=f'{workerConfig.localName}-{processID}',
            )  # 从 dispatcher 获取 taskConfig

            # -------------------- refresh dispatcher check time --------------------
            dispatcherCheckTime = currentTime

            if taskConfig == 1:
                time.sleep(0.5)  # 1 表示忙碌状态，暂停 0.5 秒
                continue
            if taskConfig == 0:
                time.sleep(5)  # 0 表示目前没有任务，暂停 5 秒
                continue
            if taskConfig == -1:
                break  # 0 表示管理器进入关闭状态，退出循环

            # -------------------- config check & unpack --------------------
            print(f'Process {processID} get task {taskConfig.sn}')
            try:
                taskFunc, taskArgs, taskKwargs = taskConfig.unpack()  # 解析 taskConfig 的数据
            except:
                print(f'  Task  {taskConfig.sn} config invalid')
                remoteProxyCall(workerConfig.taskDispatcher.invalidConfig, taskSn=taskConfig.sn)  # 发送 invalidConfig 错误
                continue

            # -------------------- send time limit --------------------
            workerConfig.pipe.send(('timeLimit', taskConfig.timeLimit))

            try:
                # -------------------- executor task --------------------
                result = taskFunc(*taskArgs, **taskKwargs)
            except Exception as err_:
                # -------------------- after crash --------------------
                print(f'  Task {taskConfig.sn} run error: {err_}')
                remoteProxyCall(
                    workerConfig.taskDispatcher.taskCrash,
                    taskSn=taskConfig.sn,
                    detail=str(err_),
                )  # 发送 taskCrash 错误
                continue

            print(f'  Task {taskConfig.sn} success')

            # -------------------- send result --------------------
            remoteProxyCall(
                workerConfig.taskDispatcher.taskSuccess,
                taskSn=taskConfig.sn,
                result=result,
            )  # 发送 taskSuccess

        # -------------------- catch the dispatcher timeout exception --------------------
        except ProxyTimeout as err_:
            print(f'Task dispatcher timeout @ worker {processID}')

            # -------------------- task dispatcher timeout --------------------
            if currentTime - dispatcherCheckTime < CONFIG.dispatcherTimeout:
                time.sleep(5)
            else:
                print(f'Task dispatcher timeout, worker {processID} exit')
                exit()

        except Exception as err_:
            print(f'* Worker {processID} crash @ {currentTimeStr()} : {err_}')
