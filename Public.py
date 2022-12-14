from __future__ import annotations

import time

from pydoc import safeimport
from dataclasses import dataclass
from typing import (TYPE_CHECKING, Callable, Iterator, TypeAlias, Iterable, )

from multiprocessing import Event
from multiprocessing.connection import Connection

from django.conf import settings
from dataclasses_json import dataclass_json

if TYPE_CHECKING:
    from .Dispatcher import DispatcherClient


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


def importFunction(path: str, *_, forceLoad: bool = False, cache: dict | None = None) -> Callable:
    func = importComponent(
        path=path,
        forceLoad=forceLoad,
        cache=cache,
    )

    if not isinstance(func, Callable):
        raise Exception(f'{path} is not a function')

    return func


# -------------------- config --------------------
autoTaskConfig = getattr(settings, 'AUTO_TASK', dict())


@dataclass(frozen=True)
class AutoTaskConfig:
    # dispatcher
    authKey: bytes
    handlerClass: str = None

    host: str = 'localhost'
    port: int = 8890
    queueSize: int = 500
    dispatcherTimeout: int = 300

    # cluster
    name: str = 'AutoTask'
    poolSize: int = 2
    workerLifetime: int = 600

    # task
    taskTimeLimit: int = 20
    taskRetryDelay: int = 30
    taskRetryCount: int = 5

    __handler = None

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
@dataclass(frozen=True)
class WorkerProcessConfig:
    sn: int
    dispatcherClient: DispatcherClient
    shutdownEvent: Event
    pipe: Connection
    localName: str


@dataclass_json
@dataclass(frozen=True)
class TaskConfig:
    func: str
    args: str | None = None
    kwargs: str | None = None


TaskConfigArrayType: TypeAlias = list[TaskConfig, ...] | tuple[TaskConfig, ...]


@dataclass(frozen=True)
class TaskData:
    name: str

    taskConfig: TaskConfig

    combine: int | None = None
    timeLimit: int | None = None

    note: str | None = None
    tags: tuple[str, ...] = ()


TaskDataArrayType: TypeAlias = list[TaskData, ...] | tuple[TaskData, ...]


@dataclass(frozen=True)
class TaskInfo:
    sn: int
    taskConfig: TaskConfig

    combine: int | None = None
    timeLimit: int | None = None

    # -------------------- unpack --------------------
    def unpack(self) -> tuple[Callable, list, dict]:
        try:
            func: Callable = importFunction(self.taskConfig.func)
        except:
            raise Exception('Invalid task function')
        if not callable(func):
            raise Exception('Invalid task function')

        args: list
        try:
            if self.taskConfig.args:
                args = CONFIG.handler.deserialize(self.taskConfig.args)
            else:
                args = []
        except:
            raise Exception('Invalid task args')
        if not isinstance(args, list):
            raise Exception('Invalid task args')

        kwargs: dict
        try:
            if self.taskConfig.kwargs:
                kwargs = CONFIG.handler.deserialize(self.taskConfig.kwargs)
            else:
                kwargs = {}
        except:
            raise Exception('Invalid task kwargs')
        if not isinstance(kwargs, dict):
            raise Exception('Invalid task kwargs')

        return func, args, kwargs


TaskInfoArrayType: TypeAlias = list[TaskInfo, ...] | tuple[TaskInfo, ...]


@dataclass
class TaskState:
    taskSn: int
    priority: int
    config: TaskInfo

    combine: int = None
    timeout: int = None
    workerName: str = None
    done: bool = False


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


__all__ = (
    'currentTimeStr', 'getNowStamp', 'timeStampToString',
    'CONFIG', 'Iterator',
    'TaskInfo', 'ReadonlyDict',
    'importComponent', 'importFunction',
    'WorkerProcessConfig', 'TaskInfo', 'TaskState',
    'remoteProxyCall', 'ProxyTimeout',
)
