import signal
import time

from django.core.management.base import BaseCommand, no_translations

from ...Public import currentTimeStr, CONFIG
from ...Dispatcher import (DispatcherServer, DispatcherAdmin, DispatcherClient, )
from ...Cluster import WorkerCluster
from ...Worker import workerFunc
from ...Handler import AutoTaskHandler


class Command(BaseCommand):
    help = 'Run AutoTask command'

    def dispatcherHostInit(self):
        dispatcherHost = DispatcherServer(
            address=('', CONFIG.port),
            authkey=CONFIG.authKey,
        )

        def shutdownDispatcher(*args):
            print(f'调度器关闭 @ {currentTimeStr()}')
            time.sleep(2)
            exit()

        for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGILL,):
            signal.signal(sig, shutdownDispatcher)

        return dispatcherHost

    def dispatcherAdminInit(self):
        dispatcherAdmin = DispatcherAdmin(
            address=('localhost', CONFIG.port),
            authkey=CONFIG.authKey,
        )

        return dispatcherAdmin

    def workerClusterInit(self):
        dispatcherClient = DispatcherClient(
            address=(CONFIG.host, CONFIG.port),
            authkey=CONFIG.authKey,
        )
        dispatcherClient.connect()

        workerCluster = WorkerCluster(
            dispatcherConn=dispatcherClient,
            workerFunc=workerFunc,
        )
        return workerCluster

    @no_translations
    def runDispatcher(self):
        dispatcherHost = self.dispatcherHostInit()
        dispatcherAdmin = self.dispatcherAdminInit()

        dispatcherHost.start()
        time.sleep(1)
        dispatcherAdmin.connect()

        print(f'调度器启动 @ {currentTimeStr()}')

        checkTime = 0
        while True:
            if time.time() - checkTime > 5:
                isRunning = dispatcherAdmin.isRunning()._getvalue()
                if isRunning:
                    AutoTaskHandler.taskSchemeAuto()
                dispatcherAdmin.refreshTaskQueue()._getvalue()
                checkTime = time.time()
            time.sleep(0.2)

    @no_translations
    def runCluster(self):
        print('Cluster init.')
        workerCluster = self.workerClusterInit()
        workerCluster.run()

    @no_translations
    def add_arguments(self, parser):
        parser.add_argument(
            'command',
            help='Command: dispatcher / cluster / shutdown / status',
        )

    def shutdownDispatcher(self):
        dispatcherAdmin = DispatcherAdmin(
            address=('localhost', CONFIG.port),
            authkey=CONFIG.authKey,
        )
        dispatcherAdmin.connect()
        res = dispatcherAdmin.shutdownDispatcher()._getvalue()
        print(res)

    def status(self):
        dispatcherAdmin = DispatcherAdmin(
            address=('localhost', CONFIG.port),
            authkey=CONFIG.authKey,
        )
        dispatcherAdmin.connect()
        dispatcherStatus = dispatcherAdmin.status()._getvalue()
        if not isinstance(dispatcherStatus, dict):
            print(dispatcherStatus)
            return

        print(f'''>-------------------------------------------------------''')
        print(f'''>          {dispatcherStatus.get('name', '******')}: {dispatcherStatus.get('state', '******')} ''')
        print(f'''>---------------   Cluster    --------------------------''')
        for cluster in dispatcherStatus.get('cluster', []):
            print(f'''> {cluster.get('name'):>10}:{cluster.get('pid', [])}''')

        print(f'''>--------------- 进行中任务 --------------------------''')
        for taskRec in dispatcherStatus.get('runningTask', []):
            print(f'''> {taskRec.get('taskSn')}:{taskRec.get('executor')}''')

    @no_translations
    def handle(self, *args, **options):
        match options.get('command'):
            case 'dispatcher':
                self.runDispatcher()
            case 'cluster':
                self.runCluster()
            case 'shutdown':
                self.shutdownDispatcher()
            case 'status':
                self.status()
