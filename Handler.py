import json

from django.core.exceptions import AppRegistryNotReady
from django.apps.registry import apps

try:
    apps.check_apps_ready()
except AppRegistryNotReady:
    import django

    django.setup()

from . import Public

if Public.TYPE_CHECKING:
    from .Public import TaskState

from .models import TaskScheme, TaskRec, TaskRecQueueFields


class AutoTaskHandler:
    # -------------------- serialize --------------------
    @classmethod
    def serialize(cls, data: dict | list | tuple) -> str:
        return json.dumps(data)

    @classmethod
    def deserialize(cls, dataStr) -> dict | list:
        return json.loads(dataStr)

    # -------------------- TaskRec --------------------
    @classmethod
    def getTaskQueue(cls, *_, taskType: int | None = None, limit: int | None = None) -> list[TaskState]:

        queryRes = TaskRec.getTaskQueue(taskType=taskType, size=limit).values(*TaskRecQueueFields)
        return [
            Public.TaskState(
                taskSn=taskRec['taskSn'], combine=taskRec['combine'], priority=taskRec['priority'],
                config=Public.TaskInfo(
                    sn=taskRec['taskSn'],
                    timeLimit=taskRec['timeLimit'] or Public.CONFIG.taskTimeLimit,
                    taskConfig=Public.TaskConfig.from_json(taskRec['config']),
                ),
            ) for taskRec in queryRes
        ]

    @classmethod
    def setTaskRecSuccess(cls, *_, taskSn: int, result: any):
        taskRec = TaskRec.manageTaskRec(taskSn=taskSn)
        if taskRec is None:
            return False
        return taskRec.setSuccess(result=cls.serialize(result))

    @classmethod
    def setTaskRecInvalidConfig(cls, *_, taskSn: int, detail: str):
        taskRec = TaskRec.manageTaskRec(taskSn=taskSn)
        if taskRec is None:
            return False
        return taskRec.setError(
            errorCode=TaskRec.ErrorCodeChoice.invalidConfig,
            message='Invalid config', detail=detail,
        )

    @classmethod
    def setTaskRecCrash(cls, *_, taskSn: int, message: str, detail: str):
        taskRec = TaskRec.manageTaskRec(taskSn=taskSn)
        if taskRec is None:
            return False
        return taskRec.setError(
            errorCode=TaskRec.ErrorCodeChoice.crash,
            message=message,
            detail=detail,
        )

    @classmethod
    def setTaskTimeout(cls, *_, taskSn: int):
        taskRec = TaskRec.manageTaskRec(taskSn=taskSn)
        if taskRec is None:
            return False

        return taskRec.setError(
            errorCode=TaskRec.ErrorCodeChoice.timeout,
            message='Task timeout',
        )

    @classmethod
    def setTaskRunning(cls, *_, taskSn: int, workerName: str) -> int | None:
        """
        根据 taskSn 设置 TaskRec 为 running 状态
        """
        taskRec = TaskRec.manageTaskRec(taskSn=taskSn)
        if taskRec is None:
            return None
        return taskRec.setRunning(workerName=workerName, )

    @classmethod
    def taskSchemeAuto(cls):
        expireScheme = TaskScheme.queryDueScheme()
        for scheme in expireScheme:
            scheme.nextTaskCreate()
