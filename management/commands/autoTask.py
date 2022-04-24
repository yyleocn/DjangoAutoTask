import signal
import time

from django.core.management.base import BaseCommand, no_translations

from ...Component import currentTimeStr, CONFIG
from ...Core import (ManagerServer, ManagerAdmin, ManagerClient, WorkerCluster, )
from ...Worker import workerFunc


class Command(BaseCommand):
    help = "Start AutoTask manager & cluster."

    def managerServerInit(self):
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
            time.sleep(2)
            exit()

        signal.signal(signal.SIGINT, serverExit)
        signal.signal(signal.SIGTERM, serverExit)

        return managerServer, managerAdmin

    def workerClusterInit(self):
        managerClient = ManagerClient(
            address=(CONFIG.host, CONFIG.port),
            authkey=CONFIG.authKey,
        )

        workerCluster = WorkerCluster(
            managerCon=managerClient,
            processFunc=workerFunc,
        )
        return workerCluster

    @no_translations
    def add_arguments(self, parser):
        parser.add_argument(
            'mode',
            help='Start mode: manager / cluster.',
        )

    @no_translations
    def runManager(self):
        managerServer, managerAdmin = self.managerServerInit()
        # taskManagerServer = managerServer.get_server()

        managerServer.start()
        time.sleep(1)

        managerAdmin.connect()

        print(f'Task manager start @ {currentTimeStr()}')

        while True:
            # managerAdmin.appendTask()._getvalue()
            managerAdmin.refreshTaskQueue()._getvalue()
            time.sleep(10)

            # managerAdmin.lock()._getvalue()
            # managerAdmin.unlock()._getvalue()

    @no_translations
    def runCluster(self):
        workerCluster = self.workerClusterInit()
        workerCluster.run()

    @no_translations
    def handle(self, *args, **options):
        match options.get('mode'):
            case 'manager':
                self.runManager()
            case 'cluster':
                self.runCluster()
