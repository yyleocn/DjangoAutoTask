import time
from dataclasses import dataclass, field
from typing import (
    Iterable,
    Union,
    Callable,
    Tuple, List,
)

from .Core.Component import importFunction

from django.db import models

# from AutoTask.config import agent
from AutoTask.Core.Component import TaskConfig


# task status
@dataclass(frozen=True)
class TaskStatus:
    invalidConfig: int = -999
    callbackError: int = -200
    fail: int = -100
    retry: int = -50
    normal: int = 0
    success: int = 100
    finish: int = 200


@dataclass(frozen=True)
class TaskType:
    normal: int = 0
    scheme: int = 1


@dataclass(frozen=True)
class TaskPriority:
    max: int = 10
    scheme: int = 50
    normal: int = 100
    idle: int = 200
    pause: int = 1000
    cancel: int = 10000


TASK_STATUS = TaskStatus()

TASK_TYPE = TaskType()

TASK_PRIORITY = TaskPriority()


class TaskPackage(models.Model):
    sn = models.BigAutoField(primary_key=True, )
    name = models.TextField(max_length=20, unique=True)  # 名称
    priority = models.SmallIntegerField(default=0)  # 优先级
    type = models.SmallIntegerField(default=0)  # 类型

    createTime = models.PositiveBigIntegerField()
    createUser = models.CharField(max_length=20, )

    count = models.SmallIntegerField(default=0, )
    success = models.SmallIntegerField(default=0, )
    fail = models.SmallIntegerField(default=0, )
    remain = models.SmallIntegerField(default=0, )

    finished = models.BooleanField(default=False, )


class TaskRec(models.Model):
    taskSn = models.BigAutoField(primary_key=True)  # task sn

    createTime = models.BigIntegerField()  # 创建时间
    createUser = models.CharField(max_length=20, null=False, blank=False)  # 创建人

    name = models.TextField(max_length=100, null=True, )  # 作业名称
    group = models.TextField(max_length=100, null=True, )  # 分组
    taskPackage = models.ForeignKey(to=TaskPackage, null=True, on_delete=models.SET_NULL)  # 作业包

    type = models.CharField(max_length=10, default='TASK')  # 类型
    priority = models.SmallIntegerField(default=100)  # 优先级，大值优先

    func = models.TextField(max_length=30, null=False, blank=False)  # func location string
    args = models.BinaryField()  # args
    kwargs = models.BinaryField()  # kwargs
    result = models.BinaryField(null=True, )  # return value
    callback = models.TextField(max_length=30, null=False, blank=False)  # callback func location string
    errorText = models.TextField(max_length=100, null=True, blank=False, default=None)  # 错误信息

    # status code:
    # -999  invalid config
    # -110  callback error
    #  -10  run fail
    #    0  init
    #  100  success
    #  110  callback finish
    status = models.SmallIntegerField(default=0, )  # 状态

    lockStamp = models.BigIntegerField(null=True, )  # 锁定时间
    lockSource = models.TextField(null=True, )  # 锁定源
    startStamp = models.BigIntegerField(null=True, )  # 开始时间
    endStamp = models.PositiveBigIntegerField(null=True, )  # 结束时间
    timeout = models.PositiveSmallIntegerField(default=30, )  # 超时
    delay = models.PositiveSmallIntegerField(default=10, )  # 间隔

    retry = models.PositiveSmallIntegerField(default=0)  # 重试
    execute = models.PositiveSmallIntegerField(default=0)  # 执行次数

    @classmethod
    def getTaskRec(cls, taskSn):
        if not isinstance(taskSn, int):
            return None
        if not cls.objects.filter(taskSn=taskSn).exists():
            return None
        return cls.objects.get(taskSn=taskSn)

    @classmethod
    def getTaskQueue(cls, taskType, limit: int = 100):
        taskQuery = cls.objects.filter(
            type=taskType, status__gt=TASK_STATUS.fail, lockStamp=None, priority__lt=TASK_PRIORITY.pause,
        ).orderBy('-priority', 'startStamp', 'createTime')[:limit]

        if taskQuery.count() < 1:
            return None

        return taskQuery.values()

    # def setStatus(self, status):
    #     self.status = status
    #     self.statusTime = time.time()
    #     self.save()
    #
    # def invalidConfig(self, errorText):
    #     self.errorText = str(errorText)
    #     self.setStatus(INVALID_CONFIG)
    #
    # def taskFail(self):
    #     self.setStatus(-1)
    #
    # def runFail(self, errorText):
    #     self.errorText = str(errorText)
    #     if self.execute > self.retry:
    #         self.taskFail()
    #         return
    #     self.setStatus(RUN_FAIL)
    #
    # def running(self):
    #     self.execute += 1
    #     self.startStamp = time.time()
    #     self.setStatus(RUNNING)
    #
    # def taskError(self, *_, errorText, errorStatus, ):
    #     self.errorText = errorText
    #     self.setStatus(errorStatus)
    #
    # def setResult(self, result_):
    #     self.result = agent.serialize(result_)
    #     self.setStatus(RUN_SUCCESS)
    #
    # def taskRun(self):
    #     if self.status < 0:
    #         return None
    #
    #     if self.execute > self.retry:
    #         self.taskFail()
    #         return None
    #
    #     try:
    #         func: Callable = importFunction(self.func)
    #         if not callable(func):
    #             self.taskError(
    #                 errorText='Task function',
    #                 errorStatus=INVALID_CONFIG,
    #             )
    #     except BaseException:
    #         self.invalidConfig('Invalid function')
    #         return None
    #
    #     try:
    #         args = agent.deserialize(self.args)
    #     except BaseException:
    #         self.invalidConfig('Invalid args')
    #         return None
    #
    #     try:
    #         kwargs = agent.deserialize(self.kwargs)
    #     except BaseException:
    #         self.invalidConfig('Invalid kwargs')
    #         return None
    #
    #     self.running()
    #     result = func(*args, **kwargs)
    #
    #     try:
    #         self.setResult(result)
    #     except BaseException:
    #         self.taskError(
    #             errorText='',
    #             errorStatus=CALLBACK_ERROR,
    #         )
    #
    #     if self.callback:
    #         try:
    #             callback: Callable = importFunction(self.callback)
    #             if not callable(callback):
    #                 self.taskError(
    #                     errorText='Callback is not a function.',
    #                     errorStatus=CALLBACK_ERROR,
    #                 )
    #
    #         except:
    #             self.taskError(
    #                 errorText='Callback run error.',
    #                 errorStatus=CALLBACK_ERROR,
    #             )
    #
    # def taskFinish(self, result):
    #     self.result = agent.serialize(result)
    #     self.setStatus(RUN_SUCCESS)
    #


@dataclass(frozen=True)
class TaskData:
    sn: int
    func: Callable
    args: [List, Tuple] = field(default_factory=list)
    kwargs: dict = field(default_factory=dict)
    callback: [Callable, None] = None
    sync: bool = False


TaskDataArrayType = Union[List[TaskData], Tuple[TaskData]]
