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

def createTaskChain(taskDataArr: Iterable[TaskData, ...]):
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
