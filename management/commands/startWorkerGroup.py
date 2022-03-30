from django.core.management.base import BaseCommand, no_translations

from AutoTask.Core.Core import WorkerGroup

from AutoTask.Core.Manager import TaskManagerClient
from AutoTask.Core.Worker import workerProcessFunc


class Command(BaseCommand):
    help = "Start auto task worker group."

    @no_translations
    def handle(self, *args, **options):
        taskManagerClient = TaskManagerClient(address=('localhost', 33221), authkey=b'AutoTaskTestServer')
        taskManagerClient.connect()
        workerGroup = WorkerGroup(
            taskManager=taskManagerClient,
            processFunc=workerProcessFunc,
        )
        try:
            workerGroup.run()
        except KeyboardInterrupt as intP_:
            workerGroup.exit()
