from __future__ import annotations

import time
import signal
import traceback

from multiprocessing import current_process, Event

from . import Public

if Public.TYPE_CHECKING:
    from .Public import WorkerProcessConfig, TaskInfo


def workerFunc(workerConfig: WorkerProcessConfig, *args, **kwargs):
    initTime = time.time()
    pid = current_process().pid
    workerStopEvent = Event()
    workerStopEvent.clear()

    processID = f'{workerConfig.sn}-{pid}'
    workerConfig.dispatcherClient.methodBound()

    print(f'* Worker {processID} start @ {Public.currentTimeStr()}')

    def stopSignalHandler(*_, ):
        print(f'Worker {processID} receive stop signal @ {Public.currentTimeStr()}')
        workerStopEvent.set()

    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGILL,):
        signal.signal(sig, stopSignalHandler)

    dispatcherCheckTime = time.time()

    while True:
        # -------------------- exit event check --------------------
        if workerConfig.shutdownEvent.is_set() or workerStopEvent.is_set():
            print(f'Worker {processID} exit @ {Public.currentTimeStr()}')
            exit()

        currentTime = time.time()

        # -------------------- work process life time --------------------
        if currentTime - initTime > Public.CONFIG.workerLifetime:
            print(f'Worker {processID} life end, exit for next')
            exit()

        # -------------------- heart beat --------------------
        workerConfig.pipe.send(('alive', currentTime))

        try:
            # -------------------- get task config --------------------
            taskConfig: TaskInfo | int = Public.remoteProxyCall(
                func=workerConfig.dispatcherClient.getTask,
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
                Public.remoteProxyCall(
                    workerConfig.dispatcherClient.invalidConfig,
                    taskSn=taskConfig.sn, detail=traceback.format_exc(),
                )  # 发送 invalidConfig 错误
                continue

            # -------------------- send time limit --------------------
            workerConfig.pipe.send(('timeLimit', taskConfig.timeLimit))

            # -------------------- executor task --------------------
            execWarn: str | None = None
            exception = None
            crashDetail = None
            with Public.catch_warnings(record=True) as warnArr:  # 捕获 warnings
                try:
                    result = taskFunc(*taskArgs, **taskKwargs)
                except Exception as exception_:  # 捕获 exception
                    print(f'  Task {taskConfig.sn} crash: {exception_}')
                    exception = exception_
                    crashDetail = traceback.format_exc()
                if warnArr:
                    execWarn = '\n'.join(
                        str(execWarn) for execWarn in warnArr
                    )

            if exception is not None:
                # -------------------- after crash --------------------
                Public.remoteProxyCall(
                    workerConfig.dispatcherClient.taskCrash,
                    taskSn=taskConfig.sn,
                    message=str(exception),
                    detail=crashDetail,
                    execWarn=execWarn,
                )  # 发送 taskCrash 错误
                continue

            print(f'  Task {taskConfig.sn} success')

            Public.remoteProxyCall(
                workerConfig.dispatcherClient.taskSuccess,  # 发送 taskSuccess
                taskSn=taskConfig.sn,
                result=result,
                execWarn=execWarn,
            )

            # -------------------- 捕获 TimeoutException --------------------
        except Public.ProxyTimeout as exception_:
            print(f'Task dispatcher timeout @ worker {processID}')

            if currentTime - dispatcherCheckTime < Public.CONFIG.dispatcherTimeout:
                # 没有超时等待 5 秒继续
                time.sleep(5)
            else:
                # 超时后 worker 退出
                print(f'Task dispatcher timeout, worker {processID} exit')
                exit()

        except Exception as exception_:
            print(f'* Worker {processID} crash @ {Public.currentTimeStr()} : {exception_}')
