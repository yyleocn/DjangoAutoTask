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
        return tuple(Public.TaskState(taskData=taskData) for taskData in taskDataArr)

    @classmethod
    def setTaskRecSuccess(
            cls, *_, taskSn: int,
            result: any, execWarn: str | None = None,
    ):
        taskRec = TaskRec.manageTaskRec(taskSn=taskSn)
        if taskRec is None:
            return False
        return taskRec.setSuccess(
            result=cls.serialize(result),
            execWarn=execWarn,
        )

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
    def getRunningBlockKey(cls) -> set[str]:
        return TaskRec.getRunningBlockKey()

    @classmethod
    def setTaskRecInvalidConfig(cls, *_, runningWorkerName: str, detail: str):
        TaskRec.setInvalidConfig(
            runningWorkerName=runningWorkerName,
            detail=detail,
        )

    @classmethod
    def overtimeTaskProcess(cls):
        overtimeTaskArr = TaskRec.queryOvertimeTask()
        for overtimeTaskRec in overtimeTaskArr:
            overtimeTaskRec.setError(
                errorCode=TaskRec.ErrorCodeChoice.timeout,
                message='任务超时',
            )
            print(f'任务 {TaskRec.taskSn} 超时')

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
        TaskScheme.dueSchemeApply()
        # for scheme in expireScheme:
        #     scheme.schemeApply()

    @classmethod
    def expireTaskProcess(cls):
        pass
