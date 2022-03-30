from django.core.management.base import BaseCommand, no_translations

from AutoTask.Core.Core import WorkerGroup

from AutoTask.Core.Manager import TaskClientManager


class Command(BaseCommand):
    help = "Start auto task worker group."

    @no_translations
    def handle(self, *args, **options):
        workerGroup = WorkerGroup(
            taskManager=TaskClientManager(address=('localhost', 33221), authkey=b'112233AABBCC')
        )
        try:
            workerGroup.run()
        except KeyboardInterrupt as intP_:
            workerGroup.exit()
