from __future__ import annotations

import time

from croniter import croniter
from django.db import models
from django.db.models import QuerySet, Q

from . import Public


def taskTimeLimit() -> int:
    return Public.CONFIG.taskTimeLimit


def currentStamp() -> int:
    return int(time.time())


#     ######            #          ##        #
#     #     #           #           #
#     #     #  #     #  ######      #      ###      #####
#     ######   #     #  #     #     #        #     #
#     #        #     #  #     #     #        #     #
#     #        #    ##  #     #     #        #     #
#     #         #### #  ######     ###     #####    #####

class TaskFieldPublic(models.Model):
    # -------------------- requirement --------------------
    createTime = models.BigIntegerField(default=currentStamp)
    createUser = models.CharField(max_length=20, null=False, blank=False, )

    name = models.CharField(max_length=50, null=False, blank=False, )  # 作业名称
    # note = models.CharField(max_length=100, null=True, blank=False, )  # 备注
    tag = models.JSONField(max_length=50, null=True, )  # 标签

    planTime = models.BigIntegerField(default=0)  # 计划时间，默认为 0 表示立即执行

    timeLimit = models.SmallIntegerField(default=taskTimeLimit)  # 运行时限
    retryDelay = models.SmallIntegerField(default=30, )  # 重试延迟
    retryLimit = models.SmallIntegerField(default=0, )  # 重试次数限制

    # -------------------- type --------------------
    class TypeChoice(models.IntegerChoices):
        normal = 0
        scheme = 10

    type = models.SmallIntegerField(choices=TypeChoice.choices, default=TypeChoice.normal, )  # 类型

    # -------------------- priority --------------------

    class PriorityChoice(models.IntegerChoices):
        max = 10
        scheme = 50
        normal = 100
        idle = 200

    priority = models.SmallIntegerField(choices=PriorityChoice.choices, default=PriorityChoice.normal, )  # 优先级

    pause = models.BooleanField(default=False)  # 暂停
    cancel = models.BooleanField(default=False)  # 取消

    # -------------------- task config --------------------
    config = models.TextField(null=False, blank=False)  # 任务配置，包含 func / args / kwargs 三部分
    # func = models.CharField(null=False, blank=False, max_length=50, )  # func location string
    # args = models.TextField(null=True, blank=False, )  # args
    # kwargs = models.TextField(null=True, blank=False, )  # kwargs

    result = models.TextField(null=True, blank=False, )  # return value

    # trigger = models.CharField(null=True, blank=False, max_length=50, )  # success / fail 触发器，func location string

    combine = models.BigIntegerField(null=True)  # combine key

    class Meta:
        abstract = True


#      #######                    #        ######                     #
#         #                       #        #     #                    #
#         #      ######   #####   #   ##   #     #   ######   #####   #   ##    ######   ######   #####
#         #     #     #  #        #  #     ######   #     #  #        #  #     #     #  #     #  #     #
#         #     #     #   ####    ###      #        #     #  #        ###      #     #  #     #  #######
#         #     #    ##       #   #  #     #        #    ##  #        #  #     #    ##   ######  #
#         #      #### #  #####    #   ##   #         #### #   #####   #   ##    #### #        #   #####
#                                                                                        #####

class TaskPackage(TaskFieldPublic):
    taskPackageSn = models.BigAutoField(primary_key=True, )

    taskCount = models.PositiveIntegerField(default=0, )  # 总数
    successCount = models.PositiveIntegerField(default=0, )  # 成功
    failCount = models.PositiveIntegerField(default=0, )  # 失败
    runningCount = models.PositiveIntegerField(default=0, )  # running

    finished = models.BooleanField(default=False, )

    def refreshStatus(self):
        queryRes = TaskRec.objects.filter(taskPackage_id=self.taskPackageSn).values('state')

    func = None
    args = None
    kwargs = None
    combine = None
    result = None

    timeLimit = None
    retryDelay = None
    retryLimit = None


#     #######                    #         #####            #
#        #                       #        #     #           #
#        #      ######   #####   #   ##   #         #####   ######    #####   ### ##    #####
#        #     #     #  #        #  #      #####   #        #     #  #     #  #  #  #  #     #
#        #     #     #   ####    ###            #  #        #     #  #######  #  #  #  #######
#        #     #    ##       #   #  #     #     #  #        #     #  #        #  #  #  #
#        #      #### #  #####    #   ##    #####    #####   #     #   #####   #     #   #####

class TaskScheme(TaskFieldPublic):
    taskSchemeSn = models.AutoField(primary_key=True)  # scheme sn

    # -------------------- next --------------------
    state = models.SmallIntegerField(default=1)  # 计划状态
    cronStr = models.CharField(max_length=20, null=False, blank=False)  # crontab 配置
    interval = models.PositiveIntegerField(null=True)  # 执行间隔

    currentTask = models.ForeignKey(
        to="TaskRec", null=True, on_delete=models.SET_NULL, related_name='currentTaskScheme',
    )  # 当前任务

    retainTime = models.PositiveIntegerField(null=False, default=86400 * 7)  # 任务保留时间

    priority = None
    result = None

    message = models.CharField(max_length=30, default=None, blank=True)

    @classmethod
    def queryDueScheme(cls) -> QuerySet[TaskScheme]:
        return cls.objects.filter(planTime__lt=currentStamp(), cancel=False, pause=False, )

    @classmethod
    def processDueScheme(cls):
        dueSchemeArray = list(cls.queryDueScheme())
        for taskScheme in dueSchemeArray:
            taskScheme.nextTaskCreate()

    def nextTaskCreate(self):
        currentTime = currentStamp()
        if currentTime < self.planTime:
            return False

        if self.cronStr:
            planTime = self.planTime
            cron = croniter(self.cronStr, currentTime)
            self.planTime = cron.next()

        else:
            if not self.interval:
                return False
            self.planTime += self.interval

        # print(f'  TaskScheme {self.taskSchemeSn} create next task')

        nextTaskRec = self.createTaskRec(planTime=self.planTime)

        self.currentTask = nextTaskRec

        self.save()

    def createTaskRec(self, planTime: int | float) -> TaskRec:
        nextTask = TaskRec(
            createUser=self.createUser,

            type=TaskRec.TypeChoice.scheme,
            name=f'''{self.name}-{Public.timeStampToString(planTime, formatStr='%Y%m%d-%H%M%S')}''',
            tag=self.tag,

            taskSchemeSn=self.taskSchemeSn,

            planTime=self.planTime,
            priority=TaskRec.PriorityChoice.scheme,

            config=self.config,
            # func=self.func,
            # args=self.args,
            # kwargs=self.kwargs,

            timeLimit=self.timeLimit,
            delay=self.retryDelay,
            retry=self.retryLimit,
        )
        nextTask.save()
        return nextTask


#      #######                    #        ######
#         #                       #        #     #
#         #      ######   #####   #   ##   #     #   #####    #####
#         #     #     #  #        #  #     ######   #     #  #
#         #     #     #   ####    ###      #   #    #######  #
#         #     #    ##       #   #  #     #    #   #        #
#         #      #### #  #####    #   ##   #     #   #####    #####

class TaskRec(TaskFieldPublic):
    taskSn = models.BigAutoField(primary_key=True)  # task sn

    # --------------------  --------------------
    taskPackageSn = models.BigIntegerField(null=True)  # 任务包 sn
    taskSchemeSn = models.BigIntegerField(null=True)  # 计划 sn

    # taskPackage = models.ForeignKey(
    #     to='TaskPackage', related_name='taskRec',
    #     null=True, on_delete=models.SET_NULL,
    # )  # 任务包
    #
    # taskScheme = models.ForeignKey(
    #     to='TaskScheme', related_name='taskRec',
    #     null=True, on_delete=models.SET_NULL,
    # )  # 计划

    # -------------------- state --------------------
    class TaskStateChoice(models.IntegerChoices):
        fail = -100
        error = -10
        init = 0
        running = 10
        success = 100

    taskState = models.SmallIntegerField(
        null=False,
        choices=TaskStateChoice.choices,
        default=TaskStateChoice.init,
    )  # 任务状态

    previousTask = models.ForeignKey(
        to='self', null=True,
        related_name='followTask', on_delete=models.CASCADE,
    )

    class ErrorCodeChoice(models.IntegerChoices):
        crash = 100001
        timeout = 200001
        invalidConfig = 300001

    errorCode = models.SmallIntegerField(null=True, choices=ErrorCodeChoice.choices, )  # 错误代码
    errorMessage = models.CharField(max_length=20, null=True, blank=False, )  # 错误信息
    detail = models.TextField(null=True, blank=False, )  # 记录 error / cancel 的详细信息

    # -------------------- time stamp --------------------
    retryTime = models.BigIntegerField(default=0)  # 重试时间
    timeout = models.BigIntegerField(null=True)  # 超时时间

    startTime = models.BigIntegerField(null=True)  # 开始时间
    endTime = models.BigIntegerField(null=True)  # 结束时间

    executorName = models.CharField(null=True, max_length=30, )  # process name
    execute = models.SmallIntegerField(default=0)  # 执行次数

    @classmethod
    def getTaskRec(cls, taskSn: int) -> TaskRec | None:
        if not isinstance(taskSn, int):
            return None
        if not cls.objects.filter(taskSn=taskSn).exists():
            return None
        taskRec = cls.objects.get(taskSn=taskSn)
        # if not cls.TriggerStateChoice.fail < taskRec.state < cls.TriggerStateChoice.success:
        #     return None
        return taskRec

    @classmethod
    def getTaskQueue(
            cls, *_,
            size: int = None,
            state: int = None, priority: int = None, taskType: int = None,
            **kwargs
    ) -> QuerySet[TaskRec]:
        currentTime = currentStamp()

        querySize = Public.CONFIG.queueSize
        if isinstance(size, int):
            querySize = size

        qConfig = []

        if isinstance(taskType, int):
            qConfig.append(Q(type=taskType))

        if isinstance(priority, int):
            qConfig.append(Q(priority__lte=priority))

        if isinstance(state, int):
            qConfig.append(Q(state=state))

        taskQuery = cls.objects.filter(
            Q(prevTask__isnull=True) | Q(prevTask__taskState__gte=cls.TaskStateChoice.success),  # 没有前置任务或已完成
            state__gt=cls.TaskStateChoice.fail, state__lt=cls.TaskStateChoice.success,  # 状态介于 fail 和 success 之间
            planTime__lte=currentTime, retryTime__lte=currentTime,  # planTime & retryTime 小于当前时间
            pause=False, cancel=False,  # 没有 暂停/取消
            *qConfig,

        ).order_by('priority', 'startTime', 'createTime')[:querySize]

        return taskQuery

    @classmethod
    def overtimeTask(cls, ) -> QuerySet[TaskRec]:
        currentTime = currentStamp()
        queryConfig = [
            Q(state=cls.TaskStateChoice.running),
            Q(expireTime__lt=currentTime - 5),
        ]

        return cls.objects.filter(*queryConfig)

    def updateState(self, state: int):
        self.taskState = state
        self.taskStateTime = currentStamp()
        self.save()

    def setRunning(self, executorName: str) -> int:
        if self.previousTask is not None:
            if self.previousTask.taskState < self.TaskStateChoice.success:
                return False

        if self.taskState >= self.TaskStateChoice.success:
            return False

        self.execute += 1
        self.executorName = executorName[:30]

        self.startTime = currentStamp()
        self.timeout = self.startTime + self.timeLimit

        self.updateState(self.TaskStateChoice.running)

        return self.timeout

    def setError(self, errorCode: ErrorCodeChoice, message: str = None, detail: str = None) -> bool:
        if not self.taskState == self.TaskStateChoice.running:
            return False

        if isinstance(errorCode, self.ErrorCodeChoice):
            self.errorCode = errorCode

        if isinstance(message, str):
            self.errorMessage = message

        if isinstance(detail, str):
            self.detail = detail

        if errorCode == self.ErrorCodeChoice.invalidConfig:
            self.errorCode = self.ErrorCodeChoice.invalidConfig
            self.updateState(self.TaskStateChoice.fail)
            return True

        if self.execute >= self.retryLimit:
            self.updateState(self.TaskStateChoice.fail)
            return True

        self.retryTime = currentStamp() + self.retryDelay
        self.updateState(self.TaskStateChoice.error)
        return True

    def setSuccess(self, result: str = None) -> bool:
        if not self.taskState == self.TaskStateChoice.running:
            return False

        if result is not None:
            self.result = result

        self.endTime = currentStamp()
        self.updateState(self.TaskStateChoice.success)
        return True

    def remove(self):
        followTaskArr = tuple(self.followTask.all())
        if self.taskState < TaskRec.TaskStateChoice.success:  # 当前任务未完成，后续任务标记为取消
            for followTask in followTaskArr:
                followTask.previousTask = None
                followTask.cancel = True  # 后续任务标记为取消
                followTask.detail = f'Previous task {self.taskSn}-{self.name} removed.'  # 标记取消原因
                followTask.save()
        else:  # 当前任务已完成，后续任务清除关联
            for followTask in followTaskArr:
                followTask.previousTask = None
                followTask.save()

        self.delete()

        del self

        return followTaskArr


TaskRecQueueFields = (
    'taskSn', 'type', 'priority',
    'func', 'args', 'kwargs', 'combine', 'callback', 'timeLimit', 'priority',
)
