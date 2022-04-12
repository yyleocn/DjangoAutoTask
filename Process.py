import time
import random
import signal
from multiprocessing import current_process, Event

from .Component import SubProcessConfig, currentTimeStr, TaskConfig
from .Conf import CONFIG


def processFunc(processConfig: SubProcessConfig, *args, **kwargs):
    initTime = time.time()
    pid = current_process().pid
    stopEvent = Event()
    stopEvent.clear()
    print(f'* Process {pid} start @ {currentTimeStr()}.')

    def stopSignalHandler(*_, ):
        print(f'Process {pid} receive stop signal @ {currentTimeStr()}.')
        stopEvent.set()

    signal.signal(signal.SIGINT, stopSignalHandler)
    signal.signal(signal.SIGTERM, stopSignalHandler)

    systemCheckTime = time.time()

    while True:
        # -------------------- check event status --------------------
        if processConfig.stopEvent.is_set() or stopEvent.is_set():
            print(f'Process {pid} exit @ {currentTimeStr()}.')
            exit()

        # -------------------- task manager timeout --------------------
        if time.time() - systemCheckTime > CONFIG.taskManagerTimeout:
            print(f'Task manager timeout , process {pid} exit.')
            exit()

        # -------------------- send alive time --------------------
        processConfig.pipe.send(time.time())

        # -------------------- get task config --------------------
        try:
            taskConfig = processConfig.taskManager.getTask(
                processor=f'{processConfig.localName}|{pid}'
            )._getvalue()
            if not isinstance(taskConfig, TaskConfig):
                print(f'Task manager busy, process {pid} ----------')
                time.sleep(0.2)
                continue
            systemCheckTime = time.time()
        except BaseException as err_:
            print(err_)
            time.sleep(2)
            continue

        # -------------------- function content --------------------

        print(f'Process {pid} get task config:\n    {taskConfig}.')

        time.sleep(5 + random.random() * 5)
