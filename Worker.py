from __future__ import annotations

import time
import signal
import traceback

from multiprocessing import current_process, Event

from . import Public

if Public.TYPE_CHECKING:
    from .Public import (WorkerProcessConfig, WorkerTaskData, )


def workerFunc(workerConfig: WorkerProcessConfig, *args, **kwargs):
    initTime = time.time()
    pid = current_process().pid
    workerStopEvent = Event()
    workerStopEvent.clear()

    workerName = f'{Public.CONFIG.name}-{workerConfig.sn:02d}-{pid:<5d}'
    workerNamePrint = f'{Public.CONFIG.name}-作业器-{workerConfig.sn:02d}-{pid:<5d}'
    workerConfig.dispatcherClient.methodBound()

    print(f'* {workerNamePrint} 启动 @ {Public.currentTimeStr()}')

    def stopSignalHandler(*_, ):
        print(f'{workerNamePrint} 收到关闭信号 @ {Public.currentTimeStr()}')
        workerStopEvent.set()

    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGILL,):
        signal.signal(sig, stopSignalHandler)

    dispatcherCheckTime = time.time()

    while True:
        # -------------------- exit event check --------------------
        if workerConfig.clusterOffline.is_set() or workerStopEvent.is_set():
            print(f'{workerNamePrint} >>> 进程关闭 {Public.currentTimeStr()}')
            exit()

        currentTime = time.time()

        # -------------------- work process life time --------------------
        if currentTime - initTime > Public.CONFIG.workerLifetime:
            print(f'{workerNamePrint} >>> 到达时限，等待重启')
            exit()

        # -------------------- heart beat --------------------
        workerConfig.pipe.send(('alive', currentTime))

        try:
            # -------------------- 获取任务配置 --------------------
            fetchData: str | int = Public.remoteProxyCall(
                func=workerConfig.dispatcherClient.getTask,
                workerName=workerName,
            )  # 从 dispatcher 获取 taskInfo

            # -------------------- refresh dispatcher check time --------------------
            dispatcherCheckTime = currentTime

            if fetchData == 1:  # 1 表示忙碌状态，暂停 0.5 秒
                time.sleep(0.5)
                continue
            if fetchData == 0:
                # print(f'{workerNamePrint} >>> 队列为空，等待中')  # 0 表示目前没有任务，暂停 5 秒
                time.sleep(10)
                continue
            if fetchData == -1:  # -1 表示管理器进入关闭状态，退出循环
                break

                # -------------------- config check & unpack --------------------

            try:
                taskData: WorkerTaskData = Public.CONFIG.handler.deserialize(fetchData)  # 解析 taskData 的数据
                taskSn = taskData['taskSn']
                execTimeLimit = taskData['execTimeLimit']
                funcPath = taskData['funcPath']
                taskName = taskData['name']

                taskFunc = Public.importFunction(funcPath)

                taskArgs = taskData['args']
                taskKwargs = taskData['kwargs']
            except:
                print(f'{workerNamePrint} >>> 任务配置无效')
                Public.remoteProxyCall(
                    workerConfig.dispatcherClient.invalidConfig,
                    detail=traceback.format_exc(),
                    runningWorkerName=workerName,
                )  # 发送 invalidConfig 错误
                continue

            # print(f'{workerNamePrint} >>> 拉取任务 {taskSn} - {taskName}')

            # -------------------- send time limit --------------------
            workerConfig.pipe.send(('timeLimit', execTimeLimit))

            # -------------------- executor task --------------------
            execWarn: str | None = None
            exception = None
            crashDetail = None
            startTime = time.time()
            with Public.catch_warnings(record=True) as warnMsgArr:  # 捕获 warnings
                try:
                    result = taskFunc(*taskArgs, **taskKwargs)
                except Exception as exception_:  # 捕获 exception
                    print(f'{workerNamePrint} 任务失败 {taskSn} - {taskName} \n  >>>  {exception_}')
                    exception = exception_
                    crashDetail = traceback.format_exc()
                if warnMsgArr:
                    execWarn = '\n'.join(
                        str(warnMsg.message) for warnMsg in warnMsgArr
                    )

            if exception is not None:
                # -------------------- after crash --------------------
                Public.remoteProxyCall(
                    workerConfig.dispatcherClient.taskCrash,
                    taskSn=taskSn,
                    message=str(exception),
                    detail=crashDetail,
                    execWarn=execWarn,
                )  # 发送 taskCrash 错误
                continue

            # print(f'{workerNamePrint} 任务完成 {taskSn} - {taskName} @ {time.time() - startTime:.03f}s')

            Public.remoteProxyCall(
                workerConfig.dispatcherClient.taskSuccess,  # 发送 taskSuccess
                taskSn=taskSn,
                result=result,
                execWarn=execWarn,
            )
            time.sleep(0.1)

            # -------------------- 捕获 TimeoutException --------------------
        except Public.ProxyTimeout as exception_:
            print(f'{workerNamePrint} >>> 调度器连接失败')

            if currentTime - dispatcherCheckTime < Public.CONFIG.dispatcherTimeout:
                # 没有超时等待 5 秒继续
                time.sleep(5)
            else:
                # 超时后 worker 退出
                print(f'{workerNamePrint} >>> 调度器连接超时，进程退出')
                exit()

        except Exception as exception_:
            print(f'* {workerNamePrint} 运行错误 @ {Public.currentTimeStr()} : {exception_}')
