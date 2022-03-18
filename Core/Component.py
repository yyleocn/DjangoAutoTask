from typing import Callable
from dataclasses import dataclass, field


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
            raise BaseException('Invalid function.')

        if not isinstance(self.args, tuple):
            raise BaseException('Invalid args.')

        if not isinstance(self.kwargs, ReadonlyDict):
            raise BaseException('Invalid kwargs.')
