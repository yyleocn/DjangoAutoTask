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

from .Component import CONFIG, TaskConfig, importFunction, TaskState

from .models import TaskRec, TaskRecQueueFields


class AutoTaskHandler:
    _desKey = bytes.fromhex(md5((CONFIG.dbSecretKey * 1024).encode('UTF-8')).hexdigest()[:16])

    desObj = des(_desKey, ECB, _desKey, padmode=PAD_NORMAL, pad=' ')

    @classmethod
    def serialize(cls, data: dict | list | tuple):
        return data

    @classmethod
    def deserialize(cls, data) -> dict | list:
        return data

    @classmethod
    def getTaskQueue(cls, *_, taskType: int | None = None, limit: int | None = None) -> list[TaskState]:
        queryRes = TaskRec.getTaskQueue(taskType=taskType, limit=limit).values(*TaskRecQueueFields)
        return [
            TaskState(
                taskSn=taskRec['taskSn'], combine=taskRec['combine'], priority=taskRec['priority'],
                config=TaskConfig(
                    sn=taskRec['taskSn'], combine=taskRec['combine'],
                    timeout=taskRec['timeout'] or CONFIG.taskTimeout,
                    func=taskRec['func'], callback=taskRec['callback'],
                    args=taskRec['args'], kwargs=taskRec['kwargs'],
                ),
            ) for taskRec in queryRes
        ]

    @classmethod
    def setTaskStatus(cls, *_, taskSn, status: int, ):
        taskRec = TaskRec.initTaskRec(taskSn=taskSn)
        if taskRec is None:
            return False
        taskRec.setStatus(status=status)
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
    def taskTimeout(cls, *_, taskSn: int):
        taskRec = TaskRec.initTaskRec(taskSn=taskSn)
        if taskRec is None:
            return False
        return taskRec.setError(errorText='Task timeout', errorStatus=TaskRec.StatusChoice.timeout)

    @classmethod
    def taskRunning(cls, *_, taskSn: int, overTime: int, executorName: str = None, ):
        taskRec = TaskRec.initTaskRec(taskSn=taskSn)
        if taskRec is None:
            return False
        return taskRec.setRunning(
            overTime=overTime,
            executorName=executorName,
        )

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
