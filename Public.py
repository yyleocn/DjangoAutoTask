from __future__ import annotations

import json
import time

from pydoc import safeimport

from inspect import getmodule, isfunction

from typing import (TYPE_CHECKING, Callable, Iterator, TypeAlias, Iterable, TypedDict, )
from types import (FunctionType, )

from multiprocessing import Event
from multiprocessing.connection import Connection

import dataclasses
from dataclasses_json import dataclass_json

from django.conf import settings

from warnings import warn, catch_warnings

if TYPE_CHECKING:
    from .Dispatcher import DispatcherClient
    from .Handler import AutoTaskHandler


def getNowStamp() -> int:
    return int(time.time())


def timeStampToString(timeStamp: int | float, formatStr: str = '%Y-%m-%d#%H:%M:%S') -> str:
    return time.strftime(formatStr, time.localtime(timeStamp))


def currentTimeStr(formatStr: str = '%Y%m%d%H:%M:%S'):
    return f'''{time.strftime('%Y%m%d-%H:%M:%S', time.localtime())}'''


# -------------------- import component by string --------------------
importCache = {}


def importComponent(path: str, *_, forceLoad: bool = False, cache: dict | None = None) -> any:
    if cache is None:
        cache = importCache

    if path in cache and not forceLoad:
        return cache[path]

    # pathParts = [part for part in path.split('.') if part]
    pathParts = path.split('.')
    n = len(pathParts)
    importModule = None

    while n > 0:
        try:
            importModule = safeimport('.'.join(pathParts[:n]), forceLoad)
        except:
            importModule = None
        if importModule is not None:
            break
        n = n - 1

    if not hasattr(importModule, '.'.join(pathParts[n:])):
        raise Exception(f'{path} not exist')
    return getattr(importModule, '.'.join(pathParts[n:]))


def importFunction(path: str, *_, forceLoad: bool = False, cache: dict | None = None) -> FunctionType:
    func = importComponent(
        path=path,
        forceLoad=forceLoad,
        cache=cache,
    )

    if not isinstance(func, FunctionType):
        raise Exception(f'{path} is not a function')

    return func


# -------------------- config --------------------
autoTaskConfig = getattr(settings, 'AUTO_TASK', dict())


@dataclasses.dataclass(frozen=True)
class AutoTaskConfig:
    # dispatcher
    authKey: bytes
    handlerClass: str = None

    host: str = 'localhost'
    port: int = 8890
    queueSize: int = 500
    dispatcherTimeout: int = 120

    # cluster
    name: str = 'AutoTask'
    poolSize: int = 2
    workerLifetime: int = 600

    # task
    execTimeLimit: int = 20
    execLimit: int = 5
    retryDelay: int = 30

    # handler: AutoTaskHandler = None

    def __getattr__(self, key):
        if key == 'handler':
            from .Handler import AutoTaskHandler
            object.__setattr__(self, 'handler', AutoTaskHandler())
            return self.handler
        raise AttributeError(f''''{self.__class__.__name__}' object has no attribute '{key}' ''')

    def __post_init__(self):
        if self.handlerClass is not None:
            object.__setattr__(self, 'handler', importComponent(self.handlerClass)())


CONFIG = AutoTaskConfig(**autoTaskConfig)


# -------------------- read only dict --------------------

class ReadonlyDict(dict):
    """
    Readonly Dict, base on python dict type.
    Can't set data like dict[key]=value.
    Use dict.update() instead.
    """

    def __readonly__(self, ):
        raise RuntimeError('Cannot modify readonly dict')

    __setitem__ = __readonly__
    __delitem__ = __readonly__

    pop = __readonly__
    popitem = __readonly__
    clear = __readonly__
    setdefault = __readonly__
    # update = __readonly__

    del __readonly__


# -------------------- template --------------------
@dataclasses.dataclass(frozen=True)
class WorkerProcessConfig:
    localName: str
    sn: int
    dispatcherClient: DispatcherClient
    clusterOffline: Event
    pipe: Connection


class WorkerTaskData(TypedDict):
    taskSn: int
    name: str
    execTimeLimit: int

    funcPath: str
    args: list
    kwargs: dict


@dataclass_json
@dataclasses.dataclass(frozen=True)
class TaskData:
    name: str

    funcPath: str
    argsStr: str
    kwargsStr: str

    execTimeLimit: int

    taskSn: int = None
    priority: int = None
    blockKey: str | None = None

    note: str | None = None
    tag: str | None = None

    @classmethod
    def pack(
            cls, *_, name: str, func: Callable, args: tuple | list = None, kwargs: dict = None,
            blockKey: int = None, execTimeLimit: int = None, priority: int = None,
            note: str = None, tag: str = None,
    ) -> TaskData:
        assert isfunction(func), '无效的 func'
        funcPath = f'{getmodule(func).__name__}.{func.__name__}'

        argsStr = '[]'
        if args:
            argsStr = CONFIG.handler.serialize(args)

        kwargsStr = '{}'
        if kwargs:
            kwargsStr = CONFIG.handler.serialize(kwargs)

        if execTimeLimit is None:
            execTimeLimit = CONFIG.execTimeLimit

        return cls(
            name=name, funcPath=funcPath, argsStr=argsStr, kwargsStr=kwargsStr,
            blockKey=blockKey, execTimeLimit=execTimeLimit,
            note=note, tag=tag,
        )

    def exportToSaveModel(self) -> dict:
        dataDict = self.to_dict()
        emptyKey = []
        for key, value in list(dataDict.items()):
            if value is None:
                dataDict.pop(key)

        return dataDict

    def exportToWorker(self) -> str:
        dataDict: WorkerTaskData = dict(
            taskSn=self.taskSn,
            name=self.name,
            funcPath=self.funcPath,
            args=json.loads(self.argsStr),
            kwargs=json.loads(self.kwargsStr),
            execTimeLimit=self.execTimeLimit,
        )
        return CONFIG.handler.serialize(dataDict)


TaskDataArrayType: TypeAlias = list[TaskData, ...] | tuple[TaskData, ...]


@dataclasses.dataclass
class TaskState:
    taskData: TaskData

    endTime: int = None
    workerName: str = None
    done: bool = False

    def __post_init__(self):
        assert self.taskData.taskSn, '没有 taskSn 的任务无法加载'
        assert self.taskData.priority is not None, '没有 priority 的任务无法加载'

    def exportToWorker(self) -> str:
        return self.taskData.exportToWorker()

    @property
    def priority(self) -> int:
        return self.taskData.priority

    @property
    def taskSn(self) -> int:
        return self.taskData.taskSn


TaskStateArrayType: TypeAlias = list[TaskState, ...] | tuple[TaskState, ...]


class ProxyTimeout(Exception):
    pass


def remoteProxyCall(func: Callable, *args, retry=5, **kwargs):
    retryCounter = 0
    while retryCounter < retry + 1:
        try:
            return func(*args, **kwargs)._getvalue()
        except Exception as err_:
            print(f'  Proxy function {func.__name__} call error: {err_}')
            retryCounter = retryCounter + 1
            time.sleep(1)
    raise ProxyTimeout(f'TaskDispatcher call {func.__name__} fail')


_ = (
    Iterator, Iterable, warn, catch_warnings,
)
