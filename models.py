from __future__ import annotations

import time

from croniter import croniter

from django.db import models
from django.db.models import QuerySet, Q

from django.db.models.signals import pre_delete
from . import Public


def getTaskTimeLimit() -> int:
    return Public.CONFIG.taskTimeLimit


def getNowTimeStamp() -> int:
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
    createTime = models.BigIntegerField(default=getNowTimeStamp)
    createUser = models.CharField(max_length=20, null=False, blank=False, )

    name = models.CharField(max_length=50, null=False, blank=False, )  # 作业名称
    tag = models.JSONField(max_length=50, null=True, )  # 标签

    planTime = models.BigIntegerField(default=0)  # 计划时间，默认为 0 表示立即执行

    executeTimeLimit = models.SmallIntegerField(default=getTaskTimeLimit)  # 运行时限
    retryDelay = models.SmallIntegerField(default=30, )  # 重试延迟
    retryLimit = models.SmallIntegerField(default=0, )  # 重试次数限制

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
    config = models.TextField(null=False, blank=False)  # TaskConfig 的 json 数据，包含 func / args / kwargs 三部分
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
        queryRes = TaskRec.objects.filter(taskPackage_id=self.taskPackageSn).values('taskState')

    func = None
    args = None
    kwargs = None
    combine = None
    result = None

    executeTimeLimit = None
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
    cronStr = models.CharField(max_length=20, null=False, blank=False)  # crontab 配置
    interval = models.PositiveIntegerField(default=86400)  # 执行间隔，默认一天

    currentTask = models.ForeignKey(
        to='TaskRec', null=True, on_delete=models.SET_NULL, related_name='currentTaskScheme',
    )  # 当前任务

    retainTime = models.PositiveIntegerField(null=False, default=86400 * 7)  # 任务保留时间

    priority = None
    result = None

    message = models.CharField(max_length=30, null=True, blank=True)

    @classmethod
    def queryDueScheme(cls) -> QuerySet[TaskScheme]:
        return cls.objects.filter(planTime__lt=getNowTimeStamp() + 30, cancel=False, pause=False, )

    @classmethod
    def processDueScheme(cls):
        dueSchemeArray = list(cls.queryDueScheme())
        for taskScheme in dueSchemeArray:
            taskScheme.schemeApply()

    def schemeApply(self):
        currentTime = getNowTimeStamp()
        if currentTime + 30 < self.planTime:
            return False

        if self.cronStr:
            cronTimer = croniter(self.cronStr, currentTime)
            nextPlanTime = cronTimer.next()
            if nextPlanTime <= self.planTime:
                return False
            self.planTime = cronTimer.next()
        else:
            if not self.interval:
                return False
            self.planTime += self.interval

        # print(f'  TaskScheme {self.taskSchemeSn} create next task')

        nextTaskRec = self.createTaskRec()

        self.currentTask = nextTaskRec

        self.save()

    def createTaskRec(self) -> TaskRec:
        nextTask = TaskRec(
            createUser=self.createUser,

            # type=TaskRec.TypeChoice.scheme,
            name=f'''{self.name}-{Public.timeStampToString(self.planTime, formatStr='%Y%m%d-%H%M%S')}''',
            tag=self.tag,

            taskSchemeSn=self.taskSchemeSn,

            planTime=self.planTime,
            priority=TaskRec.PriorityChoice.scheme,
            executeTimeLimit=self.executeTimeLimit,

            config=self.config,

            timeLimit=self.executeTimeLimit,
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
    class Meta:
        # indexes = (
        #     'taskSn',
        # )
        index_together = (
            'taskSn',
            'priority', 'type',
            'prevTask', 'taskState',
            'planTime', 'retryTime',
            'pause', 'cancel',
        )
        ordering = (
            'priority', 'taskSn',
        )

    taskSn = models.BigAutoField(primary_key=True)  # task sn

    # -------------------- package & scheme --------------------
    taskPackageSn = models.BigIntegerField(null=True)  # 任务包 sn
    taskSchemeSn = models.BigIntegerField(null=True)  # 计划 sn

    # -------------------- task state --------------------
    class TaskStateChoice(models.IntegerChoices):
        fail = -100
        crash = -10
        init = 0
        running = 10
        success = 100

    taskState = models.SmallIntegerField(
        null=False,
        choices=TaskStateChoice.choices,
        default=TaskStateChoice.init,
    )  # 任务状态
    taskStateTime = models.BigIntegerField(default=0)  # 状态标记时间

    previousTask = models.ForeignKey(  # 前置任务，可以实现任务链功能
        to='self', null=True,
        related_name='followTask', on_delete=models.PROTECT,
    )

    class ErrorCodeChoice(models.IntegerChoices):
        crash = 100001
        timeout = 200001
        invalidConfig = 300001

    result = models.TextField(null=True, blank=False, default=None, )  # return value
    detail = models.TextField(null=True, blank=False, default=None, )  # 记录 error / cancel 的详细信息
    execWarn = models.TextField(null=True, blank=False, default=None, )

    errorCode = models.SmallIntegerField(null=True, choices=ErrorCodeChoice.choices, )  # 错误代码
    errorMessage = models.CharField(max_length=20, null=True, blank=False, )  # 错误信息

    # -------------------- time stamp --------------------
    retryTime = models.BigIntegerField(default=0)  # 重试时间
    timeout = models.BigIntegerField(null=True)  # 超时时间

    startTime = models.BigIntegerField(null=True)  # 开始时间
    endTime = models.BigIntegerField(null=True)  # 结束时间

    workerName = models.CharField(null=True, max_length=30, )  # 名字
    execute = models.SmallIntegerField(default=0)  # 执行次数

    @classmethod
    def manageTaskRec(cls, taskSn: int) -> TaskRec | None:
        if not isinstance(taskSn, int):
            return None
        if not cls.objects.filter(taskSn=taskSn).exists():
            return None
        taskRec = cls.objects.get(taskSn=taskSn)

        if not cls.TaskStateChoice.fail < taskRec.taskState < cls.TaskStateChoice.success:
            return None

        return taskRec

    @classmethod
    def getTaskQueue(
            cls, *_,
            size: int = None,
            taskState: int = None, priority: int = None, taskType: int = None,
            **kwargs
    ) -> QuerySet[TaskRec]:
        currentTime = getNowTimeStamp()

        querySize = Public.CONFIG.queueSize
        if isinstance(size, int):
            querySize = size

        qConfig = []

        if isinstance(taskType, int):
            qConfig.append(Q(type=taskType))

        if isinstance(priority, int):
            qConfig.append(Q(priority__lte=priority))

        if isinstance(taskState, int):
            qConfig.append(Q(taskState=taskState))

        taskQuery = cls.objects.filter(
            Q(prevTask__isnull=True) | Q(prevTask__taskState__gte=cls.TaskStateChoice.success),  # 没有前置任务或已完成
            taskState__gt=cls.TaskStateChoice.fail, taskState__lt=cls.TaskStateChoice.success,  # 状态介于 fail 和 success 之间
            planTime__lte=currentTime, retryTime__lte=currentTime,  # planTime & retryTime 小于当前时间
            pause=False, cancel=False,  # 没有 暂停/取消
            *qConfig,

        ).order_by('priority', 'startTime', 'createTime')[:querySize]

        return taskQuery

    @classmethod
    def overtimeTask(cls, ) -> QuerySet[TaskRec]:
        currentTime = getNowTimeStamp()
        queryConfig = [
            Q(taskState=cls.TaskStateChoice.running),
            Q(expireTime__lt=currentTime - 5),
        ]

        return cls.objects.filter(*queryConfig)

    def updateState(self, taskState: int):
        self.taskState = taskState
        self.taskStateTime = getNowTimeStamp()
        self.save()

    def setRunning(self, workerName: str) -> int:
        if self.previousTask is not None:
            if self.previousTask.taskState < self.TaskStateChoice.success:
                return False

        if self.taskState >= self.TaskStateChoice.success:
            return False

        self.execute += 1
        self.workerName = workerName[:30]

        self.startTime = getNowTimeStamp()
        self.timeout = self.startTime + self.executeTimeLimit

        self.updateState(self.TaskStateChoice.running)

        return self.timeout

    def setError(
            self, errorCode: ErrorCodeChoice,
            message: str = None, detail: str = None, execWarn: str = None,
    ) -> bool:
        if not self.taskState == self.TaskStateChoice.running:
            return False

        if isinstance(errorCode, self.ErrorCodeChoice):
            self.errorCode = errorCode

        if isinstance(message, str):
            self.errorMessage = message

        if isinstance(detail, str):
            self.detail = detail

        if isinstance(execWarn, str):
            self.execWarn = execWarn

        if errorCode == self.ErrorCodeChoice.invalidConfig:
            self.errorCode = self.ErrorCodeChoice.invalidConfig
            self.updateState(self.TaskStateChoice.fail)
            return True

        if self.execute >= self.retryLimit:
            self.updateState(self.TaskStateChoice.fail)
            return True

        self.retryTime = getNowTimeStamp() + self.retryDelay
        self.updateState(self.TaskStateChoice.crash)
        return True

    def setSuccess(self, result: str = None, execWarn: str | None = None, ) -> bool:
        if not self.taskState == self.TaskStateChoice.running:
            return False

        if isinstance(result, str):
            self.result = result

        if isinstance(execWarn, str):
            self.execWarn = execWarn

        self.endTime = getNowTimeStamp()
        self.updateState(self.TaskStateChoice.success)
        return True

    # def remove(self):
    #     """
    #     对每个任务调用 remove 进行删除，不要使用 delete
    #     """
    #     followTaskArr = tuple(self.followTask.all())
    #     if self.taskState == TaskRec.TaskStateChoice.running:  # 运行中的任务无法删除
    #         self.cancel = True  # 标记为取消
    #         self.save()
    #         return False
    #
    #     if self.taskState < TaskRec.TaskStateChoice.success:  # 当前任务未完成，后续任务标记为取消
    #         for followTask in followTaskArr:
    #             followTask.previousTask = None
    #             followTask.cancel = True  # 后续任务标记为取消
    #             followTask.detail = f'Previous task {self.taskSn}-{self.name} removed.'  # 标记取消原因
    #             followTask.save()
    #
    #     else:  # 当前任务已完成，后续任务清除关联
    #         for followTask in followTaskArr:
    #             followTask.previousTask = None
    #             followTask.save()
    #
    #     self.delete()
    #
    #     del self
    #
    #     return followTaskArr


def taskRecPreDelete(sender, instance: TaskRec, using, origin, **kwargs):
    """
    TaskRec 删除前处理
    """

    if instance.taskState == TaskRec.TaskStateChoice.running:  # 运行中的任务不清除后置任务，无法删除
        instance.cancel = True  # 将任务标记为取消
        instance.save()
        return

    followTaskArr = tuple(instance.followTask.all())

    if instance.taskState < TaskRec.TaskStateChoice.success:
        # 当前任务未完成，后续任务标记为取消
        for followTask in followTaskArr:
            followTask.previousTask = None
            followTask.cancel = True  # 后续任务标记为取消
            followTask.detail = f'Previous task {instance.taskSn}-{instance.name} removed.'  # 标记取消原因
            followTask.save()
        return

    # 当前任务已完成，后续任务清除关联
    for followTask in followTaskArr:
        followTask.previousTask = None
        followTask.save()


pre_delete.connect(
    taskRecPreDelete,
    sender=TaskRec,
    dispatch_uid='TaskRecPreDelete',
)

TaskRecQueueFields = (
    'taskSn', 'type', 'priority',
    'func', 'args', 'kwargs', 'combine', 'callback', 'timeLimit', 'priority',
)
