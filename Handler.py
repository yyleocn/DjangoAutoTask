import json

from typing import Callable
from hashlib import md5

from pyDes import des, PAD_NORMAL, ECB

from django.core.exceptions import AppRegistryNotReady
from django.apps.registry import apps

try:
    apps.check_apps_ready()
except AppRegistryNotReady:
    import django

    django.setup()

from .Component import CONFIG, TaskConfig, importFunction, TaskInfo

from .models import TaskRec, TaskRecQueueFields, TaskScheme


class AutoTaskHandler:
    _desKey = bytes.fromhex(md5((CONFIG.dbSecretKey * 1024).encode('UTF-8')).hexdigest()[:16])

    desObj = des(_desKey, ECB, _desKey, padmode=PAD_NORMAL, pad=' ')

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

        queryRes = TaskRec.getTaskQueue(taskType=taskType, limit=limit).values(*TaskRecQueueFields)
        return [
            TaskInfo(
                taskSn=taskRec['taskSn'], combine=taskRec['combine'], priority=taskRec['priority'],
                config=TaskConfig(
                    sn=taskRec['taskSn'], combine=taskRec['combine'],
                    expire=taskRec['expire'] or CONFIG.taskExpire,
                    func=taskRec['func'], callback=taskRec['callback'],
                    args=taskRec['args'], kwargs=taskRec['kwargs'],
                ),
            ) for taskRec in queryRes
        ]

    # -------------------- TaskRec State --------------------
    @classmethod
    def setTaskState(cls, *_, taskSn, state: int, ):
        taskRec = TaskRec.initTaskRec(taskSn=taskSn)
        if taskRec is None:
            return False
        taskRec.setState(state=state)
        return True

    @classmethod
    def taskSuccess(cls, *_, taskSn: int, result: any):
        taskRec = TaskRec.initTaskRec(taskSn=taskSn)
        if taskRec is None:
            return False
        return taskRec.setSuccess(result=cls.serialize(result))

    @classmethod
    def taskInvalidConfig(cls, *_, taskSn: int, errorText: str, ):
        taskRec = TaskRec.initTaskRec(taskSn=taskSn)
        if taskRec is None:
            return False
        return taskRec.invalidConfig(errorText=errorText)

    @classmethod
    def taskError(cls, *_, taskSn: int, errorText: str, ):
        taskRec = TaskRec.initTaskRec(taskSn=taskSn)
        if taskRec is None:
            return False
        return taskRec.setError(errorText=errorText)

    @classmethod
    def taskExpire(cls, *_, taskSn: int):
        taskRec = TaskRec.initTaskRec(taskSn=taskSn)
        if taskRec is None:
            return False
        return taskRec.setError(errorText='Task expire', errorCode=-100)

    @classmethod
    def taskRunning(cls, *_, taskSn: int, expire: int, executorName: str = None, ):
        taskRec = TaskRec.initTaskRec(taskSn=taskSn)
        if taskRec is None:
            return False
        return taskRec.setRunning(
            expire=expire,
            executorName=executorName,
        )

    # -------------------- TaskConfig --------------------
    @classmethod
    def configUnpack(cls, config: TaskConfig):
        runConfig = {
            'func': '',
            'args': [],
            'kwargs': {},
        }
        try:
            runConfig['func']: Callable = importFunction(config.func)
        except:
            raise Exception('Invalid task function.')
        if not callable(runConfig['func']):
            raise Exception('Invalid task function.')

        try:
            if config.args:
                runConfig['args']: list = cls.deserialize(config.args)
        except:
            raise Exception('Invalid task args.')
        if not isinstance(runConfig['args'], list):
            raise Exception('Invalid task args.')

        try:
            if config.kwargs:
                runConfig['kwargs']: dict = cls.deserialize(config.kwargs)
        except:
            raise Exception('Invalid task kwargs.')
        if not isinstance(runConfig['kwargs'], dict):
            raise Exception('Invalid task kwargs.')

        return runConfig

    @classmethod
    def taskSchemeAuto(cls):
        expireScheme = TaskScheme.expireScheme()
        for scheme in expireScheme:
            scheme.nextTaskCreate()
