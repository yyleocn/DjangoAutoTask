import time
import signal

from django.core.management.base import BaseCommand, no_translations

from AutoTask.Core.Core import TaskManagerServer
from AutoTask.Core.Conf import CONFIG


class Command(BaseCommand):
    help = "Starts auto task manager."

    @no_translations
    def handle(self, *args, **options):
        server = TaskManagerServer(
            address=('', CONFIG.port),
            authkey=CONFIG.authKey,
        )

        def serverExit(*argsF):
            print('Task manager close.')
            server.shutdown()
            time.sleep(1)
            exit()

        signal.signal(signal.SIGINT, serverExit)
        signal.signal(signal.SIGTERM, serverExit)

        server.start()

        print('Task manager start.')

        while True:
            print(time.time())
            time.sleep(10)
