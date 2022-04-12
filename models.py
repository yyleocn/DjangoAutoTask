from django.db import models


class UserField(models.CharField):
    def __init__(self, *args, **kwargs):
        kwargs['max_length'] = 20
        kwargs['blank'] = False
        if kwargs.get('null'):
            kwargs['default'] = None
        super().__init__(*args, **kwargs)


class TimeStampField(models.BigIntegerField):
    pass


class TaskScheme(models.Model):
    schemeSn = models.AutoField(primary_key=True)  # scheme sn

    # -------------------- create --------------------
    createTime = TimeStampField(null=False)
    createUser = UserField(null=False)

    # -------------------- priority --------------------
    class PriorityChoices(models.IntegerChoices):
        max = 10
        scheme = 50
        normal = 100
        idle = 200
        pause = 1000

    priority = models.SmallIntegerField(choices=PriorityChoices.choices, default=PriorityChoices.normal)  # 优先级


class TaskPackage(models.Model):
    sn = models.BigAutoField(primary_key=True, )
    name = models.CharField(max_length=30, unique=True)  # 名称

    # -------------------- create --------------------
    createTime = TimeStampField(null=False)
    createUser = UserField(null=False)

    # -------------------- priority --------------------
    class PriorityChoices(models.IntegerChoices):
        max = 10
        scheme = 50
        normal = 100
        idle = 200
        pause = 1000

    priority = models.SmallIntegerField(choices=PriorityChoices.choices, default=PriorityChoices.normal)  # 优先级

    # -------------------- type --------------------
    class TypeChoices(models.IntegerChoices):
        normal = 0
        scheme = 1

    type = models.SmallIntegerField(default=0)  # 类型

    schemeRec = models.ForeignKey(to=TaskScheme, null=True, on_delete=models.SET_NULL)

    schemeTime = TimeStampField(null=False)  # 计划时间

    count = models.PositiveIntegerField(default=0, )
    success = models.PositiveIntegerField(default=0, )
    fail = models.PositiveIntegerField(default=0, )
    remain = models.PositiveIntegerField(default=0, )
    finished = models.BooleanField(default=False, )


class TaskRec(models.Model):
    taskSn = models.BigAutoField(primary_key=True)  # task sn

    # -------------------- create --------------------
    createTime = TimeStampField(null=False)
    createUser = UserField(null=False)

    # --------------------  --------------------
    name = models.CharField(max_length=50, null=True, )  # 作业名称
    group = models.CharField(max_length=50, null=True, )  # 分组
    taskPackage = models.ForeignKey(to=TaskPackage, null=True, on_delete=models.SET_NULL)  # 作业包

    # -------------------- type --------------------
    class TypeChoices(models.IntegerChoices):
        normal = 0
        scheme = 1

    type = models.PositiveSmallIntegerField(
        null=False,
        choices=TypeChoices.choices,
        default=TypeChoices.normal,
    )  # 类型

    # -------------------- priority --------------------
    class PriorityChoices(models.IntegerChoices):
        max = 10
        scheme = 50
        normal = 100
        idle = 200
        pause = 1000

    priority = models.SmallIntegerField(
        null=False,
        choices=PriorityChoices.choices,
        default=PriorityChoices.normal,
    )  # 优先级，小值优先

    # -------------------- task config --------------------
    func = models.CharField(
        null=False, blank=False,
        max_length=30,
    )  # func location string
    args = models.BinaryField()  # args
    kwargs = models.BinaryField()  # kwargs
    result = models.BinaryField(null=True, )  # return value
    callback = models.CharField(
        null=True, blank=False,
        max_length=30, default=None,
    )  # callback func location string

    # -------------------- status --------------------
    class StatusChoices(models.IntegerChoices):
        invalid_config = -999
        callback_error = -200
        fail = -100
        retry = -50
        normal = 0
        success = 100
        finish = 200

    status = models.SmallIntegerField(null=False, choices=StatusChoices.choices, default=StatusChoices.normal, )  # 状态
    errorText = models.CharField(max_length=100, null=True, blank=False, default=None)  # 错误信息

    # -------------------- time stamp --------------------
    schemeTime = TimeStampField(null=False)  # 计划时间

    startTime = TimeStampField(null=True)  # 开始时间
    executorName = models.CharField(null=True, max_length=50)  # process name

    endTime = TimeStampField(null=True)  # 结束时间

    timeLimit = models.PositiveSmallIntegerField(null=True)  # 运行时限
    delay = models.PositiveSmallIntegerField(default=10, null=False)  # 间隔

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
            type=taskType, lockTime=None,
            status__gt=TaskRec.StatusChoices.fail,
            priority__lt=TaskRec.PriorityChoices.pause,
        ).orderBy('-priority', 'startTime', 'createTime')[:limit]

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
    #
    # def taskFinish(self, result):
    #     self.result = agent.serialize(result)
    #     self.setStatus(RUN_SUCCESS)
    #
