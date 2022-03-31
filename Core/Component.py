import builtins
import sys
import time

from multiprocessing import Event
from multiprocessing.connection import Connection
from multiprocessing.managers import SyncManager

from dataclasses import dataclass, field
from inspect import isfunction
from typing import Callable


class ErrorDuringImport(Exception):
    """Errors that occurred while trying to import something to document it."""

    def __init__(self, filename, exc_info):
        self.filename = filename
        self.exc, self.value, self.tb = exc_info

    def __str__(self):
        exc = self.exc.__name__
        return 'problem in %s - %s: %s' % (self.filename, exc, self.value)


defaultCache = {}


def safeImport(path, cache_=None, forceLoad=0, ):
    if cache_ is None:
        cache = defaultCache
    else:
        cache = cache_
    """Import a module; handle errors; return None if the module isn't found.

    If the module *is* found but an exception occurs, it's wrapped in an
    ErrorDuringImport exception and reraised.  Unlike __import__, if a
    package path is specified, the module at the end of the path is returned,
    not the package at the beginning.  If the optional 'forceLoad' argument
    is 1, we reload the module from disk (unless it's a dynamic extension)."""
    try:
        # If forceLoad is 1 and the module has been previously loaded from
        # disk, we always have to reload the module.  Checking the file's
        # mtime isn't good enough (e.g. the module could contain a class
        # that inherits from another module that has changed).
        if forceLoad and path in sys.modules:
            if path not in sys.builtin_module_names:
                # Remove the module from sys.modules and re-import to try
                # and avoid problems with partially loaded modules.
                # Also remove any submodules because they won't appear
                # in the newly loaded module's namespace if they're already
                # in sys.modules.
                subs = [m for m in sys.modules if m.startswith(path + '.')]
                for key in [path] + subs:
                    # Prevent garbage collection.
                    cache[key] = sys.modules[key]
                    del sys.modules[key]
        module = __import__(path)
    except:
        # Did the error occur before or after the module was found?
        (exc, value, tb) = info = sys.exc_info()
        if path in sys.modules:
            # An error occurred while executing the imported module.
            raise ErrorDuringImport(sys.modules[path].__file__, info)
        elif exc is SyntaxError:
            # A SyntaxError occurred before we could execute the module.
            raise ErrorDuringImport(value.filename, info)
        elif issubclass(exc, ImportError) and value.name == path:
            # No such module in the path.
            return None
        else:
            # Some other error occurred during the importing process.
            raise ErrorDuringImport(path, sys.exc_info())
    for part in path.split('.')[1:]:
        try:
            module = getattr(module, part)
        except AttributeError:
            return None
    return module


def importFunction(path, forceLoad=0) -> Callable:
    """Locate an object by name or dotted path, importing as necessary."""
    parts = [part for part in path.split('.') if part]
    currentModule, n = None, 0
    while n < len(parts):
        nextModule = safeImport('.'.join(parts[:n + 1]), forceLoad)
        if nextModule:
            currentModule, n = nextModule, n + 1
        else:
            break
    if currentModule:
        target = currentModule
    else:
        target = builtins
    for part in parts[n:]:
        try:
            target = getattr(target, part)
        except AttributeError:
            raise Exception(f'{path} not exist.')
    if not isfunction(target):
        raise Exception(f'{path} is not a function.')
    return target


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
class TaskConfig:
    func: Callable = None
    args: tuple = tuple()
    kwargs: ReadonlyDict = field(default_factory=ReadonlyDict)

    def __init__(self, ):
        if not isinstance(self.func, Callable):
            raise Exception('Invalid function.')

        if not isinstance(self.args, tuple):
            raise Exception('Invalid args.')

        if not isinstance(self.kwargs, ReadonlyDict):
            raise Exception('Invalid kwargs.')


@dataclass(frozen=True)
class SubProcessConfig:
    taskManager: SyncManager
    stopEvent: Event
    pipe: Connection


def currentTimeStr():
    return f'{time.time():.3f}'


__all__ = (
    'TaskConfig', 'ReadonlyDict',
    'importFunction',
    'SubProcessConfig', 'currentTimeStr',
)
