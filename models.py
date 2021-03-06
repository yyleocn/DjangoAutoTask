import time
from time import time as getCurrentTime
from typing import Union

from croniter import croniter

from django.db import models
from django.db.models import Q
from django.db.models.query import QuerySet

TASK_TIMEOUT_DEFAULT = 30


def stamp2str(stamp: int | float):
    return time.strftime('%Y-%m-%d_%H:%M:%S', time.localtime(stamp))


class UserField(models.CharField):
    def __init__(self, *args, **kwargs):
        kwargs['max_length'] = 20
        kwargs['blank'] = False
        if kwargs.get('null'):
            kwargs['default'] = None
        super().__init__(*args, **kwargs)


class TimeStampField(models.BigIntegerField):
    pass


class TaskModelPublic(models.Model):
    # -------------------- create --------------------
    createTime = TimeStampField(null=False)
    createUser = UserField(null=False)

    name = models.CharField(max_length=50, null=True, )  # 作业名称
    tag = models.JSONField(max_length=50, null=True, )  # 标签

    planTime = TimeStampField(null=False, default=0)  # 计划时间

    timeout = models.SmallIntegerField(null=True)  # 运行时限
    delay = models.SmallIntegerField(default=10, null=False, )  # 延迟
    retry = models.SmallIntegerField(default=0)  # 重试

    # -------------------- type --------------------
    class TypeChoice(models.IntegerChoices):
        normal = 0
        scheme = 10

    type = models.SmallIntegerField(
        choices=TypeChoice.choices,
        default=TypeChoice.normal,
    )  # 类型

    # -------------------- priority --------------------

    class PriorityChoice(models.IntegerChoices):
        max = 10
        scheme = 50
        normal = 100
        idle = 200
        pause = 1000

    priority = models.SmallIntegerField(
        choices=PriorityChoice.choices,
        default=PriorityChoice.normal,
    )  # 优先级

    # -------------------- task config --------------------
    func = models.CharField(
        null=False, blank=False,
        max_length=50,
    )  # func location string
    callback = models.CharField(
        null=True, blank=False,
        max_length=50, default=None,
    )  # callback func location string

    args = models.TextField(null=True, blank=False, )  # args
    kwargs = models.TextField(null=True, blank=False, )  # kwargs
    combine = models.BigIntegerField(null=True)  # combine key

    result = models.TextField(null=True, blank=False, )  # return value

    pause = models.BooleanField(default=False)  # 暂停
    cancel = models.BooleanField(default=False)  # 取消

    class Meta:
        abstract = True


class TaskPackage(TaskModelPublic):
    sn = models.BigAutoField(primary_key=True, )

    count = models.PositiveIntegerField(default=0, )  # 总数
    success = models.PositiveIntegerField(default=0, )  # 成功
    fail = models.PositiveIntegerField(default=0, )  # 失败
    running = models.PositiveIntegerField(default=0, )  # running

    finished = models.BooleanField(default=False, )

    func = None
    args = None
    kwargs = None
    callback = None
    combine = None
    result = None


class TaskScheme(TaskModelPublic):
    schemeSn = models.AutoField(primary_key=True)  # scheme sn

    # -------------------- next --------------------

    cronStr = models.CharField(max_length=20, null=False, blank=False)  # crontab 配置
    interval = models.PositiveIntegerField(default=86400)  # 执行间隔

    currentTask = models.ForeignKey(
        to="TaskRec", null=True, on_delete=models.SET_NULL, related_name='currentScheme'
    )  # 当前任务

    retainDuration = models.PositiveIntegerField(null=False, default=86400 * 7)  # 任务保留时间

    priority = None
    result = None

    @classmethod
    def expireScheme(cls) -> QuerySet['TaskScheme']:
        return cls.objects.filter(
            planTime__lt=getCurrentTime(),
            cancel=False,
            pause=False,
        )

    def nextTaskCreate(self):
        currentTime = getCurrentTime()
        if currentTime < self.planTime:
            return False

        if self.cronStr:
            planTime = self.planTime
            cron = croniter(self.cronStr, currentTime)
            self.planTime = cron.next()
        else:
            if not self.interval:
                return False
            self.planTime = self.planTime + self.interval

        print(f'  TaskScheme {self.schemeSn} create next task.')

        nextTaskRec = self.taskRecCreate(planTime=self.planTime, scheme=self)

        self.currentTask = nextTaskRec

        self.save()

    def taskRecCreate(self, planTime: int | float, scheme: 'TaskScheme') -> 'TaskRec':
        planTimeStr = stamp2str(planTime)
        nextTask = TaskRec(
            createTime=time.time(),
            createUser=self.createUser,

            type=TaskRec.TypeChoice.scheme,
            name=f'{self.name}-{planTimeStr}',
            tag=self.tag,

            scheme=scheme,

            planTime=planTime,
            priority=TaskRec.PriorityChoice.scheme,

            func=self.func,
            args=self.args,
            kwargs=self.kwargs,

            timeout=self.timeout,
            delay=self.delay,
            retry=self.retry,

        )
        nextTask.save()
        return nextTask


TaskRecQueueFields = (
    'taskSn', 'type', 'priority',
    'func', 'args', 'kwargs', 'combine', 'callback', 'timeout', 'priority',
)


class TaskRec(TaskModelPublic):
    taskSn = models.BigAutoField(primary_key=True)  # task sn

    # --------------------  --------------------
    package = models.ForeignKey(to=TaskPackage, null=True, on_delete=models.SET_NULL, related_name='taskRec')  # 作业包
    scheme = models.ForeignKey(to=TaskScheme, null=True, on_delete=models.SET_NULL, related_name='taskRec')  # 作业计划

    # -------------------- status --------------------
    class StatusChoice(models.IntegerChoices):
        invalid_config = -999
        callback_error = -200
        fail = -100
        error = -50
        timeout = -30
        normal = 1
        running = 10
        success = 100
        finish = 200

    status = models.SmallIntegerField(null=False, choices=StatusChoice.choices, default=StatusChoice.normal, )  # 状态
    errorText = models.CharField(max_length=100, null=True, blank=False, default=None)  # 错误信息

    # -------------------- time stamp --------------------

    retryTime = TimeStampField(null=True)  # 重试时间
    overTime = TimeStampField(null=True)  # 超时时间

    startTime = TimeStampField(null=True)  # 开始时间
    endTime = TimeStampField(null=True)  # 结束时间

    executorName = models.CharField(null=True, max_length=50, )  # process name
    execute = models.SmallIntegerField(default=0)  # 执行次数

    @classmethod
    def initTaskRec(cls, taskSn: int) -> Union['TaskRec', None]:
        if not isinstance(taskSn, int):
            return None
        if not cls.objects.filter(taskSn=taskSn).exists():
            return None
        taskRec = cls.objects.get(taskSn=taskSn)
        if not cls.StatusChoice.fail < taskRec.status < cls.StatusChoice.success:
            return None
        return taskRec

    @classmethod
    def getTaskQueue(
            cls, *_,
            limit: int = None,
            status: int = None, priority: int = None, taskType: int = None,
            **kwargs
    ) -> QuerySet['TaskRec']:
        currentTime = getCurrentTime()

        queryLimit = 1000
        if isinstance(limit, int):
            queryLimit = limit

        queryConfig = [
            Q(
                status__gte=cls.StatusChoice.error, status__lt=cls.StatusChoice.success,
            ),
            ~(Q(status=cls.StatusChoice.error) & Q(retry__gt=currentTime)),
            Q(planTime__lte=currentTime),
            Q(retryTime__isnull=True) | Q(retryTime__lte=currentTime),
            Q(pause=False),
            Q(cancel=False),
        ]

        if isinstance(taskType, int):
            queryConfig.append(Q(type=taskType))

        if isinstance(priority, int):
            queryConfig.append(Q(priority__lte=priority))

        if isinstance(status, int):
            queryConfig.append(Q(status=status))

        taskQuery = cls.objects.filter(
            *queryConfig,
        ).order_by('priority', 'startTime', 'createTime')[:queryLimit]

        return taskQuery

    @classmethod
    def overtimeTask(cls, ) -> QuerySet['TaskRec']:
        currentTime = getCurrentTime()
        queryConfig = [
            Q(status=cls.StatusChoice.running),
            Q(overTime__lt=currentTime - 5),
        ]

        return cls.objects.filter(*queryConfig)

    def setStatus(self, status: int):
        self.status = status
        self.statusTime = getCurrentTime()
        self.save()

    def invalidConfig(self, errorText: str) -> bool:
        if self.status == self.StatusChoice.running:
            return False
        self.errorText = errorText[:100]
        self.setStatus(self.StatusChoice.invalid_config)
        return True

    def setError(self, errorText: str, errorStatus=StatusChoice.error) -> bool:
        if not self.status == self.StatusChoice.running:
            return False
        self.errorText = errorText[:100]
        if self.execute >= self.retry:
            self.setStatus(self.StatusChoice.fail)
            return True
        self.retryTime = getCurrentTime() + self.delay
        self.setStatus(errorStatus)
        return True

    def setRunning(self, overTime: int, executorName: str = None, ) -> bool:
        if self.status >= self.StatusChoice.success:
            return False
        self.execute += 1
        self.executorName = executorName
        self.startTime = getCurrentTime()
        self.overTime = getCurrentTime() + (self.timeout or TASK_TIMEOUT_DEFAULT)
        self.setStatus(self.StatusChoice.running)
        return True

    def setSuccess(self, result: str = None) -> bool:
        if not self.status == self.StatusChoice.running:
            return False
        if isinstance(result, str):
            self.result = result
        self.endTime = getCurrentTime()
        self.setStatus(self.StatusChoice.success)
        return True


TaskRec.objects.filter(
    retry__lte=getCurrentTime(),
)
