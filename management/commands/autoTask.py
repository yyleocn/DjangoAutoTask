import signal
import time

from django.core.management.base import BaseCommand, no_translations

from ...Component import currentTimeStr, CONFIG
from ...Manager import (ManagerServer, ManagerAdmin, ManagerClient, )
from ...Cluster import WorkerCluster
from ...Worker import workerFunc
from ...Handler import AutoTaskHandler


class Command(BaseCommand):
    help = "Run AutoTask command."

    def managerServerInit(self):
        managerServer = ManagerServer(
            address=('', CONFIG.port),
            authkey=CONFIG.authKey,
        )

        managerAdmin = ManagerAdmin(
            address=('localhost', CONFIG.port),
            authkey=CONFIG.authKey,
        )

        def shutdownManager(*args):
            print(f'Task manager close @ {currentTimeStr()}')
            time.sleep(2)
            exit()

        signal.signal(signal.SIGINT, shutdownManager)
        signal.signal(signal.SIGTERM, shutdownManager)

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
    def runManager(self):
        managerServer, managerAdmin = self.managerServerInit()

        managerServer.start()
        time.sleep(1)
        managerAdmin.connect()

        print(f'Task manager start @ {currentTimeStr()}')
        while True:
            isRunning = managerAdmin.isRunning()._gevalue()
            if isRunning:
                AutoTaskHandler.taskSchemeAuto()
            managerAdmin.refreshTaskQueue()._getvalue()
            time.sleep(10)

    @no_translations
    def runCluster(self):
        workerCluster = self.workerClusterInit()
        workerCluster.run()

    @no_translations
    def add_arguments(self, parser):
        parser.add_argument(
            'command',
            help='Command: manager / cluster / shutdown / status.',
        )

    def shutdownManager(self):
        managerAdmin = ManagerAdmin(
            address=('localhost', CONFIG.port),
            authkey=CONFIG.authKey,
        )
        managerAdmin.connect()
        res = managerAdmin.shutdownManager()._getvalue()
        print(res)

    def status(self):
        managerAdmin = ManagerAdmin(
            address=('localhost', CONFIG.port),
            authkey=CONFIG.authKey,
        )
        managerAdmin.connect()
        managerStatus = managerAdmin.status()._getvalue()
        if not isinstance(managerStatus, dict):
            print(managerStatus)
            return

        print(f'''\n---------------- {managerStatus.get('name', '******')}: {managerStatus.get('status', '******')} ''')
        print(f'''---------------- {'Cluster'.center(20)} ----------------''')
        for cluster in managerStatus.get('cluster', []):
            print(f'''-- {cluster.get('name'):>10}:{cluster.get('pid', [])}''')

        print(f'''---------------- {'running task'.center(20)} ----------------''')
        for taskRec in managerStatus.get('runningTask', []):
            print(f'''--   {taskRec.get('taskSn')}:{taskRec.get('executor')}''')

    @no_translations
    def handle(self, *args, **options):
        match options.get('command'):
            case 'manager':
                self.runManager()
            case 'cluster':
                self.runCluster()
            case 'shutdown':
                self.shutdownManager()
            case 'status':
                self.status()
