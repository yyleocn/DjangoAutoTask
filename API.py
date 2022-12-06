from . import Public, models, Handler

if Public.TYPE_CHECKING:
    from .Public import Callable


def createTask(
        name: str,
        func: str,
        args: tuple,
        kwargs: dict,
):
    x = models.TaskRec(
        name=name,
        func=func,
        args=Handler.AutoTaskHandler.serialize(args),
    )


def createTaskPack():
    pass
