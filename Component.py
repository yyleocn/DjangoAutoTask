import time

from pydoc import safeimport
from dataclasses import dataclass
from typing import Callable

from multiprocessing import Event
from multiprocessing.connection import Connection
from multiprocessing.managers import BaseManager

from django.conf import settings


# -------------------- import component by string --------------------
class ErrorDuringImport(Exception):
    """Errors that occurred while trying to import something to document it."""

    def __init__(self, filename, exc_info):
        self.filename = filename
        self.exc, self.value, self.tb = exc_info

    def __str__(self):
        exc = self.exc.__name__
        return 'problem in %s - %s: %s' % (self.filename, exc, self.value)


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
        raise Exception(f'{path} not exist.')
    return getattr(importModule, '.'.join(pathParts[n:]))


def importFunction(path: str, *_, forceLoad: bool = False, cache: dict | None = None) -> Callable:
    func = importComponent(
        path=path,
        forceLoad=forceLoad,
        cache=cache,
    )

    if not isinstance(func, Callable):
        raise Exception(f'{path} is not a function.')

    return func


# -------------------- config --------------------
autoTaskConfig = {
}

if hasattr(settings, 'AUTO_TASK'):
    autoTaskConfig.update(
        settings.AUTO_TASK
    )


@dataclass(frozen=True)
class AutoTaskConfig:
    # manager
    authKey: bytes
    handler_class: str = None

    host: str = 'localhost'
    port: int = 8800
    queueSize: int = 100
    managerTimeLimit: int = 300
    dbSecretKey: str = 'SecretKey'

    # cluster
    name: str = 'AutoTask'
    poolSize: int = 2
    processLifeTime: int = 600
    taskTimeout: int = 30


CONFIG = AutoTaskConfig(**autoTaskConfig)


# -------------------- read only dict --------------------
class ReadonlyDict(dict):
    """
    Readonly Dict, base on python dict type.
    Can't set data like dict[key]=value.
    Use dict.update() instead.
    """

    def __readonly__(self, ):
        raise RuntimeError("Cannot modify readonly dict.")

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
class SubProcessConfig:
    sn: int
    taskManager: BaseManager
    stopEvent: Event
    pipe: Connection
    localName: str


@dataclass(frozen=True)
class TaskConfig:
    sn: int
    func: str
    timeout: int

    args: str | None = None
    kwargs: str | None = None
    combine: int | None = None

    callback: str | None = None


@dataclass
class TaskState:
    taskSn: int
    priority: int
    config: TaskConfig

    combine: int = None
    overTime: int = None
    executor: str = None
    done: bool = False


def currentTimeStr():
    return f'''{time.strftime('%Y-%m-%d_%H:%M:%S', time.localtime())}'''


# class TaskException(BaseException):
#     def __init__(self, *_, code: int, reason: str, ):
#         pass

class ProxyTimeoutException(Exception):
    pass


def proxyFunctionCall(func: Callable, *args, retry=5, **kwargs):
    retryCounter = 0
    while retryCounter < retry + 1:
        try:
            return func(*args, **kwargs)._getvalue()
        except Exception as err_:
            print(f'  Proxy function {func.__name__} call error: {err_}')
            retryCounter = retryCounter + 1
            time.sleep(1)
    raise ProxyTimeoutException(f'TaskManager call {func.__name__} fail.')


__all__ = (
    'CONFIG',
    'TaskConfig', 'ReadonlyDict',
    'importComponent', 'importFunction',
    'SubProcessConfig', 'TaskConfig', 'TaskState',
    'currentTimeStr', 'proxyFunctionCall', 'ProxyTimeoutException',
)
