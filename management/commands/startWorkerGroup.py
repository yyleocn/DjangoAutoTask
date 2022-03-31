from django.core.management.base import BaseCommand, no_translations

from AutoTask.Core.Conf import CONFIG
from AutoTask.Core.Core import WorkerGroup, TaskManagerClient
from AutoTask.Core.Worker import workerProcessFunc


class Command(BaseCommand):
    help = "Start auto task worker group."

    @no_translations
    def handle(self, *args, **options):
        taskManagerClient = TaskManagerClient(
            address=(CONFIG.host, CONFIG.port),
            authkey=CONFIG.authKey,
        )
        workerGroup = WorkerGroup(
            managerCon=taskManagerClient,
            processFunc=workerProcessFunc,
        )
        try:
            workerGroup.run()
        except KeyboardInterrupt as intP_:
            workerGroup.exit()
