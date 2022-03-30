import time
import random
import signal
from multiprocessing import current_process, Event

from .Component import SubProcessConfig, currentTimeStr


def workerProcessFunc(config: SubProcessConfig):
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

    while True:
        config.pipe.send(time.time())
        try:
            taskData = config.taskManager.getTask(
                time=time.time(),
                message='Hello',
            )
            print(taskData)
        except:
            pass
        print(f'Worker {pid} is running.', )

        if time.time() - initTime > 20:
            break

        time.sleep(3 + random.random() * 3)
        if config.stopEvent.is_set() or stopEvent.is_set():
            print(f'Worker {pid} exit @ {currentTimeStr()}.')
            exit()
