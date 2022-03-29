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

from AutoTask.config import agent
from AutoTask.Core.Component import TaskConfig

# task status
INVALID_CONFIG = -100
CALLBACK_ERROR = -3
RUN_FAIL = -1
RETRY = 1
RUNNING = 10
RUN_SUCCESS = 100

# task type
TASK_TYPE_NORMAL = 0
TASK_TYPE_SCHEME = 1

# task priority
TASK_PRIORITY_IDLE = 200
TASK_PRIORITY_NORMAL = 100
TASK_PRIORITY_SCHEME = 50
TASK_PRIORITY_MAX = 10


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

    status = models.SmallIntegerField(default=0, )  # 状态

    lockStamp = models.BigIntegerField(null=True, )  # 锁定时间
    lockSource = models.TextField(null=True, )  # 锁定源
    startStamp = models.BigIntegerField(null=True, )  # 开始时间
    endStamp = models.PositiveBigIntegerField(null=True, )  # 结束时间
    timeout = models.PositiveSmallIntegerField(default=30, )  # 超时
    delay = models.PositiveSmallIntegerField(default=10, )  # 间隔

    retry = models.PositiveSmallIntegerField(default=0)  # 重试
    execute = models.PositiveSmallIntegerField(default=0)  # 执行次数

    def setStatus(self, status):
        self.status = status
        self.statusTime = time.time()
        self.save()

    def invalidConfig(self, errorText):
        self.errorText = str(errorText)
        self.setStatus(INVALID_CONFIG)

    def taskFail(self):
        self.setStatus(-1)

    def runFail(self, errorText):
        self.errorText = str(errorText)
        if self.execute > self.retry:
            self.taskFail()
            return
        self.setStatus(RUN_FAIL)

    def running(self):
        self.execute += 1
        self.startStamp = time.time()
        self.setStatus(RUNNING)

    def taskError(self, *_, errorText, errorStatus, ):
        self.errorText = errorText
        self.setStatus(errorStatus)

    def setResult(self, result_):
        self.result = agent.serialize(result_)
        self.setStatus(RUN_SUCCESS)

    def taskRun(self):
        if self.status < 0:
            return None

        if self.execute > self.retry:
            self.taskFail()
            return None

        try:
            func: Callable = importFunction(self.func)
            if not callable(func):
                self.taskError(
                    errorText='Task function',
                    errorStatus=INVALID_CONFIG,
                )
        except BaseException:
            self.invalidConfig('Invalid function')
            return None

        try:
            args = agent.deserialize(self.args)
        except BaseException:
            self.invalidConfig('Invalid args')
            return None

        try:
            kwargs = agent.deserialize(self.kwargs)
        except BaseException:
            self.invalidConfig('Invalid kwargs')
            return None

        self.running()
        result = func(*args, **kwargs)

        try:
            self.setResult(result)
        except BaseException:
            self.taskError(
                errorText='',
                errorStatus=CALLBACK_ERROR,
            )

        if self.callback:
            try:
                callback: Callable = importFunction(self.callback)
                if not callable(callback):
                    self.taskError(
                        errorText='Callback is not a function.',
                        errorStatus=CALLBACK_ERROR,
                    )

            except:
                self.taskError(
                    errorText='Callback run error.',
                    errorStatus=CALLBACK_ERROR,
                )

    def taskFinish(self, result):
        self.result = agent.serialize(result)
        self.setStatus(RUN_SUCCESS)

    @staticmethod
    def getTaskRec(taskSn):
        if not isinstance(taskSn, int):
            return None
        if not TaskRec.objects.filter(taskSn=taskSn).exists():
            return None
        return TaskRec.objects.get(taskSn=taskSn)

    @staticmethod
    def getTaskQueue(taskType):
        taskQuery = TaskRec.objects.filter(
            type=taskType, status__gt=-100, lockStamp=None,
        ).orderBy('-priority', 'startStamp', 'createTime')
        if taskQuery.count() < 1:
            return None


@dataclass(frozen=True)
class TaskData:
    sn: int
    func: Callable
    args: [List, Tuple] = field(default_factory=list)
    kwargs: dict = field(default_factory=dict)
    callback: [Callable, None] = None
    sync: bool = False


TaskDataArrayType = Union[List[TaskData], Tuple[TaskData]]
