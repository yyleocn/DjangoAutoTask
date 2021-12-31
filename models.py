import time
from dataclasses import dataclass, field
from typing import Iterable, Callable, Union, Tuple, List
from pydoc import locate

from django.db import models
from django.utils.translation import gettext_lazy as _

from config import agent
from Core.Component import TaskConfig


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

    name = models.TextField(max_length=100, null=True, )  # 作业名称
    group = models.TextField(max_length=100, null=True, )  # 分组
    taskPackage = models.ForeignKey(to=TaskPackage, null=True, on_delete=models.SET_NULL)  # 作业包
    priority = models.IntegerField(default=100)  # 优先级

    type = models.CharField(max_length=10, default='TASK')  # 类型
    args = models.BinaryField()
    kwargs = models.BinaryField()
    func = models.TextField(max_length=30, null=False, blank=False)
    result = models.BinaryField(null=True, )  # return value
    errorText = models.TextField(max_length=20, null=True, blank=False, default=None)  # 错误信息

    createTime = models.BigIntegerField()  # 创建时间
    createUser = models.CharField(max_length=20, null=False, blank=False)  # 创建人

    lock = models.BigIntegerField(null=True, )  # 锁定时间
    timeout = models.IntegerField(default=30, )  # 超时
    delay = models.IntegerField(default=10, )  # 间隔

    status = models.IntegerField(default=0, )  # 状态
    # -2 config error
    # -1 fail
    #  0 default
    #  1 success
    retryLimit = models.IntegerField(default=0)  # 重试
    retryCount = models.IntegerField(default=0)  # 重试次数
    endTime = models.BigIntegerField(null=True)  # 状态时间

    def setStatus(self, status_):
        self.status = status_
        self.statusTime = time.time()
        self.save()

    def startError(self, errorText_):
        self.errorText = str(errorText_)
        self.setStatus(-2)

    def finishError(self):
        self.setStatus(-1)

    def getConfig(self):
        if self.status < 0:
            return None

        if self.retryCount >= self.retryLimit:
            self.finishError()
            return None

        try:
            func = locate(self.func)
        except:
            self.startError('Invalid function.')
            return None

        try:
            args = agent.deserialize(self.args)
        except:
            self.startError('Load args fail.')
            return None

        try:
            kwargs = agent.deserialize(self.kwargs)
        except BaseException as err_:
            self.startError('Load kwargs fail.')
            return None

        return {
            'function': func,
            'args': args,
            'kwargs': kwargs,
            'sn': self.taskSn,
        }


@dataclass(frozen=True)
class TaskData:
    name: str
    func: Callable
    args: [List, Tuple] = field(default_factory=list)
    kwargs: dict = field(default_factory=dict)
    hook: Callable = None
    sync: bool = False


TaskDataArrayType = Union[List[TaskData], Tuple[TaskData]]
