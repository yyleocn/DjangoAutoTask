from __future__ import annotations

import time
import signal
import traceback

from multiprocessing import current_process, Event

from . import Public

if Public.TYPE_CHECKING:
    from .Public import WorkerProcessConfig, TaskData, TaskConfig


def workerFunc(workerConfig: WorkerProcessConfig, *args, **kwargs):
    initTime = time.time()
    pid = current_process().pid
    workerStopEvent = Event()
    workerStopEvent.clear()

    workerName = f'{Public.CONFIG.name}-worker-{workerConfig.sn:02d}-{pid:<5d}'
    workerConfig.dispatcherClient.methodBound()

    print(f'* {workerName} start @ {Public.currentTimeStr()}')

    def stopSignalHandler(*_, ):
        print(f'{workerName} receive stop signal @ {Public.currentTimeStr()}')
        workerStopEvent.set()

    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGILL,):
        signal.signal(sig, stopSignalHandler)

    dispatcherCheckTime = time.time()

    while True:
        # -------------------- exit event check --------------------
        if workerConfig.clusterOffline.is_set() or workerStopEvent.is_set():
            print(f'{workerName} >>> 进程关闭 {Public.currentTimeStr()}')
            exit()

        currentTime = time.time()

        # -------------------- work process life time --------------------
        if currentTime - initTime > Public.CONFIG.workerLifetime:
            print(f'{workerName} >>> 到达时限，等待重启')
            exit()

        # -------------------- heart beat --------------------
        workerConfig.pipe.send(('alive', currentTime))

        try:
            # -------------------- get task config --------------------
            getResult: str | int = Public.remoteProxyCall(
                func=workerConfig.dispatcherClient.getTask,
                workerName=f'{workerConfig.localName}-{workerName}',
            )  # 从 dispatcher 获取 taskInfo

            # -------------------- refresh dispatcher check time --------------------
            dispatcherCheckTime = currentTime

            if getResult == 1:
                time.sleep(0.5)  # -1 表示忙碌状态，暂停 0.5 秒
                continue
            if getResult == 0:
                print(f'{workerName} >>> 队列为空，等待中')
                time.sleep(5)  # 1 表示目前没有任务，暂停 5 秒
                continue
            if getResult == -1:
                break  # 0 表示管理器进入关闭状态，退出循环

            taskData: TaskData = Public.TaskData.from_json(getResult)
            taskConfig: TaskConfig = taskData.getTaskConfig()

            # -------------------- config check & unpack --------------------
            print(f'{workerName} >>> 已拉取任务 {taskData.taskSn}')
            try:
                taskFunc, taskArgs, taskKwargs = taskConfig.unpack()  # 解析 taskInfo 的数据
            except:
                print(f'{workerName} >>> 任务 {taskData.taskSn} 配置无效')
                Public.remoteProxyCall(
                    workerConfig.dispatcherClient.invalidConfig,
                    taskSn=taskData.taskSn, detail=traceback.format_exc(),
                )  # 发送 invalidConfig 错误
                continue

            # -------------------- send time limit --------------------
            workerConfig.pipe.send(('timeLimit', taskData.executeTimeLimit))

            # -------------------- executor task --------------------
            execWarn: str | None = None
            exception = None
            crashDetail = None
            with Public.catch_warnings(record=True) as warnArr:  # 捕获 warnings
                try:
                    result = taskFunc(*taskArgs, **taskKwargs)
                except Exception as exception_:  # 捕获 exception
                    print(f'{workerName} >>> 任务 {taskData.taskSn} 运行错误: {exception_}')
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
                    taskSn=taskData.taskSn,
                    message=str(exception),
                    detail=crashDetail,
                    execWarn=execWarn,
                )  # 发送 taskCrash 错误
                continue

            print(f'{workerName} >>> {taskData.taskSn} 运行完毕')

            Public.remoteProxyCall(
                workerConfig.dispatcherClient.taskSuccess,  # 发送 taskSuccess
                taskSn=taskData.taskSn,
                result=result,
                execWarn=execWarn,
            )

            # -------------------- 捕获 TimeoutException --------------------
        except Public.ProxyTimeout as exception_:
            print(f'{workerName} >>> dispatcher 连接失败')

            if currentTime - dispatcherCheckTime < Public.CONFIG.dispatcherTimeout:
                # 没有超时等待 5 秒继续
                time.sleep(5)
            else:
                # 超时后 worker 退出
                print(f'{workerName} >>> dispatcher 连接超时，进程退出')
                exit()

        except Exception as exception_:
            print(f'* {workerName} 运行错误 @ {Public.currentTimeStr()} : {exception_}')
