import time
from dataclasses import dataclass, field
from typing import Iterable, Callable, Union, Tuple, List
from pydoc import locate

from django.db import models
from django.utils.translation import gettext_lazy as _

from config import agent
from Core.Component import TaskConfig

INVALID_CONFIG = -100

CALLBACK_ERROR = -3
FAIL = -1
RETRY = 1
RUNNING = 10
SUCCESS = 100


class TaskPackage(models.Model):
    sn = models.BigAutoField(primary_key=True, )
    name = models.TextField(max_length=20, unique=True)  # 名称
    priority = models.IntegerField(default=0)  # 优先级
    type = models.CharField(max_length=10, default='TASK')  # 类型

    createTime = models.BigIntegerField()
    createUser = models.CharField(max_length=20, )

    count = models.IntegerField(default=0, )
    success = models.IntegerField(default=0, )
    fail = models.IntegerField(default=0, )
    remain = models.IntegerField(default=0, )

    finished = models.BooleanField(default=False, )


class TaskRec(models.Model):
    taskSn = models.BigAutoField(primary_key=True)  # task sn

    createTime = models.BigIntegerField()  # 创建时间
    createUser = models.CharField(max_length=20, null=False, blank=False)  # 创建人

    name = models.TextField(max_length=100, null=True, )  # 作业名称
    group = models.TextField(max_length=100, null=True, )  # 分组
    taskPackage = models.ForeignKey(to=TaskPackage, null=True, on_delete=models.SET_NULL)  # 作业包

    type = models.CharField(max_length=10, default='TASK')  # 类型
    priority = models.IntegerField(default=100)  # 优先级
    args = models.BinaryField()  # args
    kwargs = models.BinaryField()  # kwargs
    func = models.TextField(max_length=30, null=False, blank=False)  # func location string
    callback = models.TextField(max_length=30, null=False, blank=False)  # callback func location string
    result = models.BinaryField(null=True, )  # return value
    errorText = models.TextField(max_length=100, null=True, blank=False, default=None)  # 错误信息

    status = models.IntegerField(default=0, )  # 状态

    # -100 invalid config
    #   -3 callback error
    #   -1 fail
    #    0 default
    #    1 need to retry
    #   10 running
    #  100 finish

    @property
    def INVALID_CONFIG(self):
        return -100

    @property
    def CALLBACK_ERROR(self):
        return -3

    @property
    def FAIL(self):
        return -1

    @property
    def RETRY(self):
        return 1

    @property
    def RUNNING(self):
        return 10

    @property
    def SUCCESS(self):
        return 100

    schemeStamp = models.BigIntegerField(null=True, )  # 计划时间
    startStamp = models.BigIntegerField(null=True, )  # 开始时间
    endStamp = models.BigIntegerField(null=True, )  # 结束时间
    timeout = models.IntegerField(default=30, )  # 超时
    delay = models.IntegerField(default=10, )  # 间隔

    retry = models.IntegerField(default=0)  # 重试
    execute = models.IntegerField(default=0)  # 重试次数

    def setStatus(self, status_):
        self.status = status_
        self.statusTime = time.time()
        self.save()

    def invalidConfig(self, errorText_):
        self.errorText = str(errorText_)
        self.setStatus(INVALID_CONFIG)

    def taskFail(self):
        self.setStatus(-1)

    def runFail(self, errorText_):
        self.errorText = str(errorText_)
        if self.execute > self.retry:
            self.taskFail()
            return
        self.setStatus(FAIL)

    def taskStart(self):
        if self.status < 0:
            return None

        if self.execute > self.retry:
            self.taskFail()
            return None

        try:
            func = locate(self.func)
        except:
            self.invalidConfig('Invalid function.')
            return None

        try:
            args = agent.deserialize(self.args)
        except:
            self.invalidConfig('Invalid args.')
            return

        try:
            kwargs = agent.deserialize(self.kwargs)
        except BaseException as err_:
            self.invalidConfig('Invalid kwargs.')
            return

        self.execute += 1
        self.startStamp = time.time()
        self.setStatus(RUNNING)
        return {
            'function': func,
            'args': args,
            'kwargs': kwargs,
            'sn': self.taskSn,
        }

    def taskFinish(self, result_):
        self.result = agent.serialize(result_)
        try:
            callback = locate(self.callback)

        except BaseException as err_:
            self.errorText = ''
            self.setStatus(-3)

        self.setStatus(100)


@dataclass(frozen=True)
class TaskData:
    name: str
    func: Callable
    args: [List, Tuple] = field(default_factory=list)
    kwargs: dict = field(default_factory=dict)
    hook: Callable = None
    sync: bool = False


TaskDataArrayType = Union[List[TaskData], Tuple[TaskData]]
