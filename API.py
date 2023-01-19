from __future__ import annotations
from . import Public

from .models import TaskRec, TaskScheme, TaskPackage

if Public.TYPE_CHECKING:
    from .Public import (TaskData, Iterable, )


#       #######                    #
#          #                       #
#          #      ######   #####   #   ##
#          #     #     #  #        #  #
#          #     #     #   ####    ###
#          #     #    ##       #   #  #
#          #      #### #  #####    #   ##

def createTask(taskData: TaskData):
    taskRec = TaskRec(
        **taskData.exportToSaveModel()
    )
    taskRec.save()


#       #######                    #               ####   #                    #
#          #                       #              #    #  #
#          #      ######   #####   #   ##        #        ######    ######   ###     # ####
#          #     #     #  #        #  #          #        #     #  #     #     #     ##    #
#          #     #     #   ####    ###           #        #     #  #     #     #     #     #
#          #     #    ##       #   #  #           #    #  #     #  #    ##     #     #     #
#          #      #### #  #####    #   ##          ####   #     #   #### #   #####   #     #

def createTaskChain(*taskDataArr: TaskData):
    previousTask = None
    for taskData in taskDataArr:
        taskRec = TaskRec(
            **taskData.exportToSaveModel(),
            previousTask=previousTask,
        )
        taskRec.save()
        previousTask = taskRec


#       #######                    #             ######                     #
#          #                       #             #     #                    #
#          #      ######   #####   #   ##        #     #   ######   #####   #   ##
#          #     #     #  #        #  #          ######   #     #  #        #  #
#          #     #     #   ####    ###           #        #     #  #        ###
#          #     #    ##       #   #  #          #        #    ##  #        #  #
#          #      #### #  #####    #   ##        #         #### #   #####   #   ##

def createTaskPack(packageName: str, taskDataArr: Iterable[TaskData, ...]):
    taskPackageRec = TaskPackage(
        name=packageName,
    )
    taskPackageRec.save()


def createTaskScheme(taskData: TaskData, cronStr: str = None, interval: int = None, retainTime: int = None):
    assert isinstance(cronStr, str) or isinstance(interval, int), 'cronStr 和 interval 必须有一个'

    taskDataDict = taskData.exportToSaveModel()

    if isinstance(cronStr, str):
        taskDataDict.update(cronStr=cronStr)
    if isinstance(interval, int):
        taskDataDict.update(interval=interval)

    if isinstance(retainTime, int):
        taskDataDict.update(retainTime=retainTime)

    taskScheme = TaskScheme(
        **taskDataDict,
    )
    taskScheme.save()
