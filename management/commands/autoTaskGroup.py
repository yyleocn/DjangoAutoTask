from django.core.management.base import BaseCommand, no_translations

from AutoTask.Core.Conf import CONFIG
from AutoTask.Core.Core import ProcessGroup, ManagerClient
from AutoTask.Core.Process import processFunc


def processGroupInit():
    taskManagerClient = ManagerClient(
        address=(CONFIG.host, CONFIG.port),
        authkey=CONFIG.authKey,
    )
    processGroup = ProcessGroup(
        managerCon=taskManagerClient,
        processFunc=processFunc,
    )
    return processGroup


class Command(BaseCommand):
    help = "Start AutoTask process group."

    @no_translations
    def handle(self, *args, **options):
        processGroup = processGroupInit()
        try:
            processGroup.run()
        except KeyboardInterrupt as intP_:
            processGroup.exit()
