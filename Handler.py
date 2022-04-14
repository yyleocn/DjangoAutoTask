import json
import zlib

import cryptocode

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
    @staticmethod
    def serialize(data: any) -> str:
        return zlib.compress(
            json.dumps(data).encode('UTF-8')
        ).hex()

    @staticmethod
    def deserialize(rawStr: str) -> any:
        return json.loads(
            zlib.decompress(
                bytes.fromhex(rawStr),
            ),
        )

    # @staticmethod
    # def serialize(data: any) -> bytes:
    #     print(
    #         zlib.compress(
    #             json.dumps(data).encode('UTF-8')
    #         ).hex(),
    #     )
    #     return cryptocode.encrypt(
    #         zlib.compress(
    #             json.dumps(data).encode('UTF-8')
    #         ).hex(),
    #         CONFIG.secretKey,
    #     )
    #
    # @staticmethod
    # def deserialize(hexStr: bytes) -> any:
    #     return json.loads(
    #         zlib.decompress(
    #             bytes.fromhex(
    #                 cryptocode.decrypt(hexStr, CONFIG.secretKey, )
    #             ),
    #         ),
    #     )

    @staticmethod
    def getTaskQueue(*_, taskType, limit):
        TaskRec.getTaskQueue(taskType=taskType, limit=limit)

    @staticmethod
    def setTaskStatus(*_, taskSn, status, ):
        pass

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
