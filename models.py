from time import time as getCurrentTime

from croniter import croniter


from django.db import models
from django.db.models import Q

TASK_TIMEOUT = 30


class UserField(models.CharField):
    def __init__(self, *args, **kwargs):
        kwargs['max_length'] = 20
        kwargs['blank'] = False
        if kwargs.get('null'):
            kwargs['default'] = None
        super().__init__(*args, **kwargs)


class TimeStampField(models.BigIntegerField):
    pass


class TaskPriorityChoice(models.IntegerChoices):
    max = 10
    scheme = 50
    normal = 100
    idle = 200
    pause = 1000


class TaskTypeChoice(models.IntegerChoices):
    normal = 0
    scheme = 1


class TaskStatusChoice(models.IntegerChoices):
    invalid_config = -999
    callback_error = -200
    fail = -100
    error = -50
    timeout = -30
    normal = 1
    running = 10
    success = 100
    finish = 200


class TaskScheme(models.Model):
    schemeSn = models.AutoField(primary_key=True)  # scheme sn

    # -------------------- create --------------------
    createTime = TimeStampField(null=False)
    createUser = UserField(null=False)

    # -------------------- priority --------------------
    PriorityChoice = TaskPriorityChoice
    priority = models.SmallIntegerField(
        choices=PriorityChoice.choices,
        default=PriorityChoice.normal,
    )  # 优先级

    # -------------------- next --------------------

    crontabStr = models.CharField(max_length=20, null=False, blank=False)  # crontab 配置
    interval = models.PositiveIntegerField(null=True)  # 执行间隔

    currentTask = models.ForeignKey(
        to="TaskRec", null=True, on_delete=models.SET_NULL, related_name='currentScheme'
    )  # 当前任务
    nextTime = TimeStampField(null=False, default=0)  # 下个任务时间

    retainTimeLimit = models.PositiveIntegerField(null=False, default=86400 * 7)  # 任务保留时间

    def creatNextTask(self):
        if getCurrentTime() < self.nextTime:
            return False

        if self.crontabStr:
            pass
        else:
            if not self.interval:
                return False
            self.nextTime = self.nextTime + self.interval


class TaskPackage(models.Model):
    sn = models.BigAutoField(primary_key=True, )
    name = models.CharField(max_length=30, unique=True)  # 名称

    # -------------------- create --------------------
    createTime = TimeStampField(null=False)
    createUser = UserField(null=False)

    # -------------------- priority --------------------
    PriorityChoice = TaskPriorityChoice

    priority = models.SmallIntegerField(
        choices=PriorityChoice.choices,
        default=PriorityChoice.normal
    )  # 优先级

    # -------------------- type --------------------
    TypeChoice = TaskTypeChoice
    type = models.SmallIntegerField(
        choices=TypeChoice.choices,
        default=TypeChoice.normal,
    )  # 类型

    planTime = TimeStampField(null=False)  # 计划时间

    count = models.PositiveIntegerField(default=0, )  # 总数
    success = models.PositiveIntegerField(default=0, )  # 成功
    fail = models.PositiveIntegerField(default=0, )  # 失败
    running = models.PositiveIntegerField(default=0, )  # running

    finished = models.BooleanField(default=False, )


TaskRecQueueFields = (
    'taskSn', 'type', 'priority',
    'func', 'args', 'kwargs', 'combine', 'callback', 'timeout', 'priority',
)


class TaskRec(models.Model):
    taskSn = models.BigAutoField(primary_key=True)  # task sn

    # -------------------- create --------------------
    createTime = TimeStampField(null=False)
    createUser = UserField(null=False)

    # --------------------  --------------------
    name = models.CharField(max_length=50, null=True, )  # 作业名称
    group = models.CharField(max_length=50, null=True, )  # 分组
    package = models.ForeignKey(to=TaskPackage, null=True, on_delete=models.SET_NULL, related_name='taskRec')  # 作业包
    scheme = models.ForeignKey(to=TaskScheme, null=True, on_delete=models.SET_NULL, related_name='taskRec')  # 作业计划

    # -------------------- type --------------------
    TypeChoice = TaskTypeChoice
    type = models.SmallIntegerField(
        null=False,
        choices=TypeChoice.choices,
        default=TypeChoice.normal,
    )  # 类型

    # -------------------- priority --------------------
    PriorityChoice = TaskPriorityChoice

    priority = models.PositiveSmallIntegerField(
        null=False,
        choices=PriorityChoice.choices,
        default=PriorityChoice.normal,
    )  # 优先级，小值优先

    # -------------------- task config --------------------
    func = models.CharField(
        null=False, blank=False,
        max_length=50,
    )  # func location string
    args = models.TextField(null=True, blank=False, )  # args
    kwargs = models.TextField(null=True, blank=False, )  # kwargs
    combine = models.BigIntegerField(null=True)  # combine key

    result = models.TextField(null=True, blank=False, )  # return value
    callback = models.CharField(
        null=True, blank=False,
        max_length=50, default=None,
    )  # callback func location string

    # -------------------- status --------------------
    StatusChoice = TaskStatusChoice

    status = models.SmallIntegerField(null=False, choices=StatusChoice.choices, default=StatusChoice.normal, )  # 状态
    errorText = models.CharField(max_length=100, null=True, blank=False, default=None)  # 错误信息

    # -------------------- time stamp --------------------
    planTime = TimeStampField(null=True)  # 计划时间
    retryTime = TimeStampField(null=True)  # 重试时间

    startTime = TimeStampField(null=True)  # 开始时间
    overTime = TimeStampField(null=True)  # 超时时间
    endTime = TimeStampField(null=True)  # 结束时间

    timeout = models.SmallIntegerField(null=True)  # 运行时限
    delay = models.SmallIntegerField(default=10, null=False, )  # 延迟

    executorName = models.CharField(null=True, max_length=50, )  # process name
    retry = models.SmallIntegerField(default=0)  # 重试
    execute = models.SmallIntegerField(default=0)  # 执行次数

    pause = models.BooleanField(default=False)  # 暂停
    cancel = models.BooleanField(default=False)  # 取消

    @classmethod
    def initTaskRec(cls, taskSn: int):
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
    ):
        currentTime = getCurrentTime()

        queryLimit = 1000
        if isinstance(limit, int):
            queryLimit = limit

        queryConfig = [
            Q(
                status__gte=cls.StatusChoice.error, status__lt=cls.StatusChoice.success,
            ),
            ~(Q(status=cls.StatusChoice.error) & Q(retry__gt=currentTime)),
            Q(planTime__isnull=True) | Q(planTime__lte=currentTime),
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

        try:

            if taskQuery.count() < 1:
                return None
        except BaseException as err_:
            return err_

        return taskQuery

    def setStatus(self, status: int):
        self.status = status
        self.statusTime = getCurrentTime()
        self.save()

    def invalidConfig(self, errorText: str):
        if self.status == self.StatusChoice.running:
            return False
        self.errorText = errorText[:100]
        self.setStatus(self.StatusChoice.invalid_config)
        return True

    def setError(self, errorText: str, errorStatus=StatusChoice.error):
        if not self.status == self.StatusChoice.running:
            return False
        self.errorText = errorText[:100]
        if self.execute >= self.retry:
            self.setStatus(self.StatusChoice.fail)
            return
        self.retryTime = getCurrentTime() + self.delay
        self.setStatus(errorStatus)
        return True

    def setRunning(self, overTime: int, executorName: str = None, ):
        if self.status >= self.StatusChoice.success:
            return False
        self.execute += 1
        self.executorName = executorName
        self.startTime = getCurrentTime()
        self.overTime = getCurrentTime() + (self.timeout or TASK_TIMEOUT)
        self.setStatus(self.StatusChoice.running)
        return True

    def setSuccess(self, result: str = None):
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
