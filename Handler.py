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
    from .Public import TaskInfo

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
    def getTaskQueue(cls, *_, taskType: int | None = None, limit: int | None = None) -> list[TaskInfo]:

        queryRes = TaskRec.getTaskQueue(taskType=taskType, size=limit).values(*TaskRecQueueFields)
        return [
            Public.TaskInfo(
                taskSn=taskRec['taskSn'], combine=taskRec['combine'], priority=taskRec['priority'],
                config=Public.TaskConfig(
                    sn=taskRec['taskSn'], combine=taskRec['combine'],
                    timeLimit=taskRec['timeLimit'] or Public.CONFIG.taskTimeLimit,
                    func=taskRec['func'], callback=taskRec['callback'],
                    args=taskRec['args'], kwargs=taskRec['kwargs'],
                ),
            ) for taskRec in queryRes
        ]

    # -------------------- TaskRec manage --------------------
    # @classmethod
    # def setTaskState(cls, *_, taskSn, state: int, ):
    #     taskRec = TaskRec.taskRecManage(taskSn=taskSn)
    #     if taskRec is None:
    #         return False
    #     taskRec.setState(state=state)
    #     return True

    @classmethod
    def setTaskRecSuccess(cls, *_, taskSn: int, result: any):
        taskRec = TaskRec.getTaskRec(taskSn=taskSn)
        if taskRec is None:
            return False
        return taskRec.setSuccess(result=cls.serialize(result))

    @classmethod
    def setTaskRecInvalidConfig(cls, *_, taskSn: int, detail: str):
        taskRec = TaskRec.getTaskRec(taskSn=taskSn)
        if taskRec is None:
            return False
        return taskRec.setError(
            errorCode=TaskRec.ErrorCodeChoice.invalidConfig,
            message='Invalid config', detail=detail,
        )

    @classmethod
    def setTaskRecCrash(cls, *_, taskSn: int, message: str, detail: str):
        taskRec = TaskRec.getTaskRec(taskSn=taskSn)
        if taskRec is None:
            return False
        return taskRec.setError(
            errorCode=TaskRec.ErrorCodeChoice.crash,
            message=message,
            detail=detail,
        )

    @classmethod
    def setTaskTimeout(cls, *_, taskSn: int):
        taskRec = TaskRec.getTaskRec(taskSn=taskSn)
        if taskRec is None:
            return False

        return taskRec.setError(
            errorCode=TaskRec.ErrorCodeChoice.timeout,
            message='Task timeout',
        )

    @classmethod
    def setTaskRunning(cls, *_, taskSn: int, executorName: str) -> int | None:
        """
        根据 taskSn 设置 TaskRec 为 running 状态
        """
        taskRec = TaskRec.getTaskRec(taskSn=taskSn)
        if taskRec is None:
            return None
        return taskRec.setRunning(executorName=executorName, )

    @classmethod
    def taskSchemeAuto(cls):
        expireScheme = TaskScheme.queryExpireScheme()
        for scheme in expireScheme:
            scheme.nextTaskCreate()
