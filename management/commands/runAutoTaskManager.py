import time
import signal

from django.core.management.base import BaseCommand, no_translations

from AutoTask.Core.Core import TaskManagerServer
from AutoTask.Core.Conf import CONFIG
from AutoTask.Core.Component import currentTimeStr


def taskManagerInit():
    managerServer = TaskManagerServer(
        address=('', CONFIG.port),
        authkey=CONFIG.authKey,
    )

    def serverExit(*args):
        print(f'Task manager close @ {currentTimeStr()}')
        managerServer.shutdown()
        time.sleep(1)
        exit()

    signal.signal(signal.SIGINT, serverExit)
    signal.signal(signal.SIGTERM, serverExit)

    return managerServer


class Command(BaseCommand):
    help = "Starts AutoTask manager."

    @no_translations
    def handle(self, *args, **options):
        server = taskManagerInit()

        server.start()

        print(f'Task manager start @ {currentTimeStr()}')

        while True:
            print(f'Task manager is running @ {currentTimeStr()}')
            time.sleep(10)
