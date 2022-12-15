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
        config=taskData.taskConfig.to_json(),
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
    prevTask = None
    taskData: TaskData
    for index, taskData in enumerate(taskDataArr):
        taskRec = TaskRec(
            name=taskData.name,
            config=taskData.taskConfig.to_json(),
            prevTask=prevTask,
            note=taskData.note,
        )
        taskRec.save()
        prevTask = taskRec


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
