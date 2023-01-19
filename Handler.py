from __future__ import annotations
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

from .models import TaskScheme, TaskRec


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
    def getTaskQueue(cls, *_, taskType: int | None = None, limit: int | None = None) -> tuple[TaskState, ...]:
        querySet = TaskRec.getTaskQueue(taskType=taskType, size=limit)
        taskDataArr = TaskRec.exportTaskData(querySet)
        return tuple(
            Public.TaskState(taskData=taskData)
            for taskData in taskDataArr
        )

    @classmethod
    def setTaskRecSuccess(
            cls, *_, taskSn: int,
            result: any, execWarn: str | None = None,
    ):
        taskRec = TaskRec.manageTaskRec(taskSn=taskSn)
        if taskRec is None:
            return False
        return taskRec.setSuccess(result=cls.serialize(result), execWarn=execWarn, )

    @classmethod
    def setTaskRecCrash(
            cls, *_, taskSn: int,
            message: str, detail: str, execWarn: str | None = None,
    ):
        taskRec = TaskRec.manageTaskRec(taskSn=taskSn)
        if taskRec is None:
            return False
        return taskRec.setError(
            errorCode=TaskRec.ErrorCodeChoice.crash,
            message=message,
            detail=detail,
            execWarn=execWarn,
        )

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
    def setTaskTimeout(cls, *_, taskSn: int):
        taskRec = TaskRec.manageTaskRec(taskSn=taskSn)
        if taskRec is None:
            return False

        return taskRec.setError(
            errorCode=TaskRec.ErrorCodeChoice.timeout,
            message='Task timeout',
        )

    @classmethod
    def setTaskRunning(cls, *_, taskSn: int, workerName: str) -> int:
        """
        根据 taskSn 设置 TaskRec 为 running 状态
        """
        taskRec = TaskRec.manageTaskRec(taskSn=taskSn)
        if taskRec is None:
            return 0
        return taskRec.setRunning(workerName=workerName, )

    @classmethod
    def taskSchemeAuto(cls):
        TaskScheme.dueSchemeApply()
        # for scheme in expireScheme:
        #     scheme.schemeApply()
