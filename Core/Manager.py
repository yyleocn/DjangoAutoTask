from multiprocessing.managers import SyncManager

from .Core import TaskManager


class TaskManagerServer(SyncManager):
    pass


class TaskManagerClient(SyncManager):
    pass


taskManager = TaskManager()

syncManagerConfig = {
    'getTask': taskManager.getTask,
}

for name_, func_ in syncManagerConfig.items():
    TaskManagerClient.register(name_, )
    TaskManagerServer.register(name_, func_, )
