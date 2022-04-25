# DjangoAutoTask

#### 基于django和Python.multiprocess的分布式自动做作业系统，使用多进程/多服务器并行处理大量作业

### 功能列表

- 功能清单
    - Manager 管理器
        - TaskManager
            - ☑ 拉取任务
            - ☑ 刷新队列
            - ☑ 任务状态：完成/失败/超时/无效
            - ping by Cluster
    - Cluster 作业器集群
        - WorkerCluster 作业器集群
            - ☑ WorkerProcess创建、检查 
            - ping TaskManager
        - WorkerProcess 作业器进程
            - ☑ 创建子进程
            - ☑ 超时检查
            - ☑ 子进程终止
    - Worker 子进程
      - ☑ 子进程函数
    - Schema 作业计划
      - 计划管理
        - 计划状态检查
        - 生成TaskRec
      - 作业检查
    - models 存储模型
        - TaskRec 任务记录
          - ☑ 任务状态：完成/失败/超时/无效/进行中
          - ☑ 获取队列
          - 超时检查
        - TaskPackage 任务包
          - 结果统计
        - TaskSchema 任务计划
          - 生成TaskRec

