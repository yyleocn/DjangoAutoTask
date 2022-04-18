import json
import zlib
from hashlib import md5

from pyDes import des, PAD_NORMAL, ECB

from django.core.exceptions import AppRegistryNotReady
from django.apps.registry import apps

try:
    apps.check_apps_ready()
except AppRegistryNotReady:
    import django

    django.setup()

from .Component import CONFIG

from .models import TaskRec


class AutoTaskHandler:
    _desKey = bytes.fromhex(md5((CONFIG.secretKey * 1024).encode('UTF-8')).hexdigest()[:16])

    desObj = des(_desKey, ECB, _desKey, padmode=PAD_NORMAL, pad=' ')

    @classmethod
    def serialize(cls, data: any) -> bytes:
        return cls.desObj.encrypt(
            zlib.compress(
                json.dumps(data).encode('UTF-8')
            )
        )

    @classmethod
    def deserialize(cls, rawHex: bytes) -> any:
        return json.loads(
            zlib.decompress(
                cls.desObj.decrypt(rawHex),
            ).decode('UTF-8'),
        )

    @classmethod
    def getTaskQueue(cls, *_, taskType, limit):
        TaskRec.getTaskQueue(taskType=taskType, limit=limit)

    @classmethod
    def setTaskStatus(cls, *_, taskSn, status: int, ):
        taskRec = TaskRec.initTaskRec(taskSn=taskSn)
        if taskRec is None:
            return False
        taskRec.setStatus(status=status)
        return True

    @classmethod
    def taskSuccess(cls, *_, taskSn: int, result: any):
        taskRec = TaskRec.initTaskRec(taskSn=taskSn)
        if taskRec is None:
            return False
        taskRec.setSuccess(result=cls.serialize(result))
        return True

    @classmethod
    def taskInvalidConfig(cls, *_, taskSn: int, errorText: str, ):
        taskRec = TaskRec.initTaskRec(taskSn=taskSn)
        if taskRec is None:
            return False
        taskRec.invalidConfig(errorText=errorText)
        return True

    @classmethod
    def taskError(cls, *_, taskSn: int, errorText: str, ):
        taskRec = TaskRec.initTaskRec(taskSn=taskSn)
        if taskRec is None:
            return False
        taskRec.setError(errorText=errorText)
        return True

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
