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

        def shutdownManager(*args):
            print(f'Task manager close @ {currentTimeStr()}')
            time.sleep(2)
            exit()

        for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGILL,):
            signal.signal(sig, shutdownManager)

        return managerServer

    def managerAdminInit(self):
        managerAdmin = ManagerAdmin(
            address=('localhost', CONFIG.port),
            authkey=CONFIG.authKey,
        )

        return managerAdmin

    def workerClusterInit(self):
        managerClient = ManagerClient(
            address=(CONFIG.host, CONFIG.port),
            authkey=CONFIG.authKey,
        )

        workerCluster = WorkerCluster(
            managerConn=managerClient,
            processFunc=workerFunc,
        )
        return workerCluster

    @no_translations
    def runManager(self):
        managerServer = self.managerServerInit()
        managerAdmin = self.managerAdminInit()

        managerServer.start()
        time.sleep(1)
        managerAdmin.connect()

        print(f'Task manager start @ {currentTimeStr()}')

        checkTime = 0
        while True:
            if time.time() - checkTime > 10:
                isRunning = managerAdmin.isRunning()._getvalue()
                if isRunning:
                    AutoTaskHandler.taskSchemeAuto()
                managerAdmin.refreshTaskQueue()._getvalue()
                checkTime = time.time()
            time.sleep(0.2)

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

        print(f'''>-------------------------------------------------------''')
        print(f'''>          {managerStatus.get('name', '******')}: {managerStatus.get('status', '******')} ''')
        print(f'''>---------------   Cluster    --------------------------''')
        for cluster in managerStatus.get('cluster', []):
            print(f'''> {cluster.get('name'):>10}:{cluster.get('pid', [])}''')

        print(f'''>--------------- running task --------------------------''')
        for taskRec in managerStatus.get('runningTask', []):
            print(f'''> {taskRec.get('taskSn')}:{taskRec.get('executor')}''')

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
