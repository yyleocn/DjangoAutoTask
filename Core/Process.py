import time
import random
import signal
from multiprocessing import current_process, Event

from .Component import SubProcessConfig, currentTimeStr
from .Conf import CONFIG


def processFunc(processConfig: SubProcessConfig, *args, **kwargs):
    initTime = time.time()
    pid = current_process().pid
    stopEvent = Event()
    stopEvent.clear()
    print(f'Process {pid} start @ {currentTimeStr()}.')

    def stopSignalHandler(*_, ):
        print(f'Process {pid} receive stop signal @ {currentTimeStr()}.')
        stopEvent.set()

    signal.signal(signal.SIGINT, stopSignalHandler)
    signal.signal(signal.SIGTERM, stopSignalHandler)

    systemCheckTime = time.time()

    while True:
        if processConfig.stopEvent.is_set() or stopEvent.is_set():
            print(f'Process {pid} exit @ {currentTimeStr()}.')
            exit()

        processConfig.pipe.send(time.time())

        if time.time() - systemCheckTime > CONFIG.taskManagerTimeout:
            print(f'Task manager timeout , process {pid} exit.')
            exit()

        try:
            taskDataProxy = processConfig.taskManager.getTask(processor=f'{processConfig.localName}|{pid}')
            taskData = taskDataProxy._getvalue()
            systemCheckTime = time.time()
        except BaseException as err_:
            print(err_)
            time.sleep(2)
            continue

        print(f'Process {pid} get taskData:\n    {taskData}.')

        time.sleep(2 + random.random() * 5)
