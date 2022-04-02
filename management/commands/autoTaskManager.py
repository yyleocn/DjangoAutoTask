import time
import signal

from django.core.management.base import BaseCommand, no_translations

from AutoTask.Core.Core import ManagerServer, ManagerAdmin, managerServerRegister
from AutoTask.Core.Conf import CONFIG
from AutoTask.Core.Component import currentTimeStr


def managerServerInit():
    managerServerRegister()
    managerServer = ManagerServer(
        address=('', CONFIG.port),
        authkey=CONFIG.authKey,
    )

    managerAdmin = ManagerAdmin(
        address=('localhost', CONFIG.port),
        authkey=CONFIG.authKey,
    )

    def serverExit(*args):
        print(f'Task manager close @ {currentTimeStr()}')
        # managerServer.shutdown()
        # managerServer.shutdown()
        time.sleep(1)
        exit()

    signal.signal(signal.SIGINT, serverExit)
    signal.signal(signal.SIGTERM, serverExit)

    return managerServer, managerAdmin


class Command(BaseCommand):
    help = "Starts AutoTask manager."

    @no_translations
    def handle(self, *args, **options):
        managerServer, managerAdmin = managerServerInit()
        # taskManagerServer = managerServer.get_server()

        managerServer.start()
        time.sleep(1)

        managerAdmin.connect()

        print(f'Task manager start @ {currentTimeStr()}')

        while True:
            print(f'Task manager is running @ {currentTimeStr()}')
            managerAdmin.appendTask()
            time.sleep(2)
            managerAdmin.lock()
            time.sleep(0.2)
            managerAdmin.unlock()
