import time
import random
import signal
from multiprocessing import current_process, Event

from .Component import SubProcessConfig, currentTimeStr
from .Conf import CONFIG


def workerProcessFunc(processConfig: SubProcessConfig, *args, **kwargs):
    initTime = time.time()
    pid = current_process().pid
    stopEvent = Event()
    stopEvent.clear()
    print(f'Worker {pid} start @ {currentTimeStr()}.')

    def stopSignalHandler(*_, ):
        print(f'Worker {pid} receive stop signal @ {currentTimeStr()}.')
        stopEvent.set()

    signal.signal(signal.SIGINT, stopSignalHandler)
    signal.signal(signal.SIGTERM, stopSignalHandler)

    systemCheckTime = time.time()

    while True:
        if processConfig.stopEvent.is_set() or stopEvent.is_set():
            print(f'Worker {pid} exit @ {currentTimeStr()}.')
            exit()

        processConfig.pipe.send(time.time())
        print(time.time() - systemCheckTime)
        if time.time() - systemCheckTime > CONFIG.taskManagerTimeout:
            print(f'TaskManager timeout , worker {pid} exit.')
            exit()

        try:
            taskData = processConfig.taskManager.getTask(worker=f'{processConfig.localName}|{pid}')
            systemCheckTime = time.time()
        except BaseException as err_:
            print(err_)
            time.sleep(1)
            continue

        print(f'TaskData is {taskData}.')
        print(f'Worker {pid} is running.', )

        time.sleep(2 + random.random() * 5)
