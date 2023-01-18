from __future__ import annotations
from . import Public

from .models import TaskRec, TaskScheme, TaskPackage

if Public.TYPE_CHECKING:
    from .Public import (TaskData, TaskConfig, Iterable, )


#       #######                    #
#          #                       #
#          #      ######   #####   #   ##
#          #     #     #  #        #  #
#          #     #     #   ####    ###
#          #     #    ##       #   #  #
#          #      #### #  #####    #   ##

def createTask(taskData: TaskData):
    taskRec = TaskRec(
        name=taskData.name,
        configJson=taskData.configJson,
    )
    taskRec.save()


#       #######                    #               ####   #                    #
#          #                       #              #    #  #
#          #      ######   #####   #   ##        #        ######    ######   ###     # ####
#          #     #     #  #        #  #          #        #     #  #     #     #     ##    #
#          #     #     #   ####    ###           #        #     #  #     #     #     #     #
#          #     #    ##       #   #  #           #    #  #     #  #    ##     #     #     #
#          #      #### #  #####    #   ##          ####   #     #   #### #   #####   #     #

def createTaskChain(taskDataArr: Iterable[TaskData, ...]):
    previousTask = None
    for taskData in taskDataArr:
        taskRec = TaskRec(
            name=taskData.name,
            configJson=taskData.configJson,
            previousTask=previousTask,
            note=taskData.note,
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
