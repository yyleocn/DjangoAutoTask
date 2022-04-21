import time

from pydoc import safeimport
from dataclasses import dataclass
from typing import Callable

from multiprocessing import Event
from multiprocessing.connection import Connection
from multiprocessing.managers import SyncManager

from django.conf import settings

config = {
}

if hasattr(settings, 'AUTO_TASK'):
    config.update(
        settings.AUTO_TASK
    )


@dataclass(frozen=True)
class AutoTaskConfig:
    # manager
    authKey: bytes
    host: str = 'localhost'
    port: int = 8800
    queueSize: int = 100
    managerTimeout: int = 60
    dbSecretKey: str = 'SecretKey'

    # group
    name: str = 'AutoTask'
    poolSize: int = 2
    processLifeTime: int = 600
    processTimeout: int = 30


CONFIG = AutoTaskConfig(**config)


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

    pathParts = [part for part in path.split('.') if part]
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

    print(importModule)
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


@dataclass(frozen=True)
class SubProcessConfig:
    sn: int
    taskManager: SyncManager
    stopEvent: Event
    pipe: Connection
    localName: str


def currentTimeStr():
    return f'''{time.strftime('%Y-%m-%d_%H:%M:%S', time.localtime())}'''


@dataclass(frozen=True)
class TaskConfig:
    sn: int
    func: str
    args: str | None = None
    kwargs: str | None = None

    timeout: int = CONFIG.processTimeout
    callback: str | None = None


class TaskException(BaseException):
    def __init__(self, *_, code: int, reason: str, ):
        pass


__all__ = (
    'CONFIG',
    'TaskConfig', 'ReadonlyDict',
    'importFunction',
    'SubProcessConfig', 'currentTimeStr',
)
