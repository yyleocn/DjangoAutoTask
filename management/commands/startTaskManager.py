from multiprocessing.managers import SyncManager

from django.core.management.base import BaseCommand, no_translations

from AutoTask.Core.Manager import TaskManagerServer

import time


class Command(BaseCommand):
    help = "Starts auto task manager."

    @no_translations
    def handle(self, *args, **options):
        server = TaskManagerServer(
            address=('', 33221),
            authkey=b'AutoTaskTestServer',
        )

        server.start()

        while True:
            print(time.time())
            time.sleep(10)
