import signal
import time

from django.core.management.base import BaseCommand, no_translations

from ...Component import currentTimeStr, CONFIG
from ...Core import ExecutorGroup, ManagerClient, managerServerRegister, ManagerServer, ManagerAdmin
from ...Process import processFunc


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
        time.sleep(1)
        exit()

    signal.signal(signal.SIGINT, serverExit)
    signal.signal(signal.SIGTERM, serverExit)

    return managerServer, managerAdmin


def processGroupInit():
    taskManagerClient = ManagerClient(
        address=(CONFIG.host, CONFIG.port),
        authkey=CONFIG.authKey,
    )
    processGroup = ExecutorGroup(
        managerCon=taskManagerClient,
        processFunc=processFunc,
    )
    return processGroup


class Command(BaseCommand):
    help = "Start AutoTask process group."

    @no_translations
    def add_arguments(self, parser):
        parser.add_argument(
            'mode',
            help='Start mode, manager / group.',
        )

    @no_translations
    def runManager(self):
        managerServer, managerAdmin = managerServerInit()
        # taskManagerServer = managerServer.get_server()

        managerServer.start()
        time.sleep(1)

        managerAdmin.connect()

        print(f'Task manager start @ {currentTimeStr()}')

        while True:
            print(f'Task manager is running @ {currentTimeStr()}')
            managerAdmin.appendTask()._getvalue()
            time.sleep(2)
            managerAdmin.lock()._getvalue()
            time.sleep(0.2)
            managerAdmin.unlock()._getvalue()

    @no_translations
    def runGroup(self):
        processGroup = processGroupInit()
        try:
            processGroup.run()
        except KeyboardInterrupt as intP_:
            processGroup.exit()

    @no_translations
    def handle(self, *args, **options):
        if options.get('mode') == 'manager':
            self.runManager()
        if options.get('mode') == 'group':
            self.runGroup()
