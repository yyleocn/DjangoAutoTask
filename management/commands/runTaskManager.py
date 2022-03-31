import time
import signal

from django.core.management.base import BaseCommand, no_translations

from AutoTask.Core.Core import TaskManagerServer
from AutoTask.Core.Conf import CONFIG


def taskManagerInit():
    server = TaskManagerServer(
        address=('', CONFIG.port),
        authkey=CONFIG.authKey,
    )

    def serverExit(*args):
        print('Task manager close.')
        server.shutdown()
        time.sleep(1)
        exit()

    signal.signal(signal.SIGINT, serverExit)
    signal.signal(signal.SIGTERM, serverExit)

    return server


class Command(BaseCommand):
    help = "Starts auto task manager."

    @no_translations
    def handle(self, *args, **options):
        server = taskManagerInit()

        server.start()

        print('Task manager start.')

        while True:
            print(time.time())
            time.sleep(10)
