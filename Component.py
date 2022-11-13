from __future__ import annotations

import time

from pydoc import safeimport
from dataclasses import dataclass
from typing import Callable

from multiprocessing import Event
from multiprocessing.connection import Connection
from multiprocessing.managers import BaseManager

from django.conf import settings


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
    # manager
    authKey: bytes
    handlerClass: str = None

    host: str = 'localhost'
    port: int = 8890
    queueSize: int = 500
    managerTimeLimit: int = 300

    # cluster
    name: str = 'AutoTask'
    poolSize: int = 2
    workerLifeTime: int = 600

    # task
    taskTimeLimit: int = 20

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
    taskManager: BaseManager
    shutdownEvent: Event
    pipe: Connection
    localName: str


@dataclass(frozen=True)
class TaskConfig:
    sn: int
    func: str
    timeLimit: int

    args: str | None = None
    kwargs: str | None = None
    combine: int | None = None

    callback: str | None = None

    # -------------------- TaskConfig --------------------
    def unpack(self) -> tuple[Callable, list, dict]:
        try:
            func: Callable = importFunction(self.func)
        except:
            raise Exception('Invalid task function')
        if not callable(func):
            raise Exception('Invalid task function')

        args: list
        try:
            if self.args:
                args = CONFIG.handler.deserialize(self.args)
            else:
                args: list = list()
        except:
            raise Exception('Invalid task args')
        if not isinstance(args, list):
            raise Exception('Invalid task args')

        kwargs: dict
        try:
            if self.kwargs:
                kwargs = CONFIG.handler.deserialize(self.kwargs)
            else:
                kwargs = dict()
        except:
            raise Exception('Invalid task kwargs')
        if not isinstance(kwargs, dict):
            raise Exception('Invalid task kwargs')

        return func, args, kwargs


@dataclass
class TaskInfo:
    taskSn: int
    priority: int
    config: TaskConfig

    combine: int = None
    timeout: int = None
    executor: str = None
    done: bool = False


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
    raise ProxyTimeout(f'TaskManager call {func.__name__} fail')


__all__ = (
    'currentTimeStr', 'getNowStamp', 'timeStampToString',
    'CONFIG',
    'TaskConfig', 'ReadonlyDict',
    'importComponent', 'importFunction',
    'WorkerProcessConfig', 'TaskConfig', 'TaskInfo',
    'remoteProxyCall', 'ProxyTimeout',
)
