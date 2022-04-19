from time import time as getCurrentTime
from django.db import models
from django.db.models import Q, F


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
    retry = -50
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

    # currentTaskID = models.PositiveBigIntegerField(null=True)  # 当前任务 ID
    currentTask = models.ForeignKey(to='TaskRec', null=True, on_delete=models.SET_NULL, related_name='taskScheme')  #
    retainTime = models.PositiveIntegerField(null=False, default=86400 * 7)  # 任务保留时间


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

    schemeTime = TimeStampField(null=False)  # 计划时间

    count = models.IntegerField(default=0, )
    success = models.IntegerField(default=0, )
    fail = models.IntegerField(default=0, )
    remain = models.IntegerField(default=0, )
    finished = models.BooleanField(default=False, )


TaskConfigValue = (
    'taskSn', 'type', 'priority',
    'func', 'args', 'kwargs', 'combine', 'callback',
)


class TaskRec(models.Model):
    taskSn = models.BigAutoField(primary_key=True)  # task sn

    # -------------------- create --------------------
    createTime = TimeStampField(null=False)
    createUser = UserField(null=False)

    # --------------------  --------------------
    name = models.CharField(max_length=50, null=True, )  # 作业名称
    group = models.CharField(max_length=50, null=True, )  # 分组
    taskPackage = models.ForeignKey(to=TaskPackage, null=True, on_delete=models.SET_NULL)  # 作业包
    taskScheme = models.ForeignKey(to=TaskScheme, null=True, on_delete=models.SET_NULL, )  # 作业计划

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
        max_length=30,
    )  # func location string
    args = models.BinaryField()  # args
    kwargs = models.BinaryField()  # kwargs
    combine = models.BigIntegerField(null=True)  # combine key

    result = models.BinaryField(null=True, )  # return value
    callback = models.CharField(
        null=True, blank=False,
        max_length=50, default=None,
    )  # callback func location string

    # -------------------- status --------------------
    StatusChoice = TaskStatusChoice

    status = models.SmallIntegerField(null=False, choices=StatusChoice.choices, default=StatusChoice.normal, )  # 状态
    errorText = models.CharField(max_length=100, null=True, blank=False, default=None)  # 错误信息

    # -------------------- time stamp --------------------
    schemeTime = TimeStampField(null=False)  # 计划时间
    retryTime = TimeStampField(null=True)  # 重试时间

    startTime = TimeStampField(null=True)  # 开始时间
    endTime = TimeStampField(null=True)  # 结束时间

    timeLimit = models.SmallIntegerField(null=True)  # 运行时限
    delay = models.SmallIntegerField(default=10, null=False, )  # 间隔

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
            limit: int = 100,
            status: int = None, priority: int = None, taskType: int = None,
            **kwargs
    ):
        currentTime = getCurrentTime()

        queryConfig = [
            Q(
                status__gte=cls.StatusChoice.retry, status__lt=cls.StatusChoice.success,
            ),
            ~(Q(status=cls.StatusChoice.retry) & Q(retry__gt=currentTime)),
            Q(schemeTime__isnull=True) | Q(schemeTime__lte=currentTime),
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
        ).order_by('priority', 'startTime', 'createTime')[:limit]

        print(str(taskQuery.query))

        if taskQuery.count() < 1:
            return None

        return None

    def setStatus(self, status: int):
        self.status = status
        self.statusTime = getCurrentTime()
        self.save()

    def setRunning(self):
        self.execute += 1
        self.startTime = getCurrentTime()
        self.setStatus(self.StatusChoice.running)

    def setError(self, errorText: str):
        self.errorText = errorText[:100]
        if self.execute > self.retry:
            self.setStatus(self.StatusChoice.fail)
            return
        self.retryTime = getCurrentTime() + self.delay
        self.setStatus(self.StatusChoice.retry)

    def setSuccess(self, result: bytes = None):
        if isinstance(result, bytes):
            self.result = result
        self.endTime = getCurrentTime()
        self.setStatus(self.StatusChoice.success)

    def invalidConfig(self, errorText: str):
        self.errorText = errorText[:100]
        self.setStatus(self.StatusChoice.invalid_config)


TaskRec.objects.filter(
    retry__lte=getCurrentTime(),
)

# def running(self):
#     self.execute += 1
#     self.startTime = time.time()
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
