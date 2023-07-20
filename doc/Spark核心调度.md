### DAG
1. DAG的概念
- 有向无环图(Directed Acyclic Graph)
- 表示作业的执行逻辑和依赖关系 

2. DAG的生成
- 根据RDD之间的依赖关系生成
- 每个RDD为一个顶点(Vertex)
- 依赖关系构成有向边(Edge)

3. DAG的执行
- 被划分为不同的Stage
- Stage内Task并行执行
- 遵循先后依赖顺序

4. DAG的容错机制  
- 仅需重算失败的Task
- 不需要重跑整个DAG
- 通过血缘关系重建丢失的分区

5. DAG的持久化
- 周期性持久化DAG到磁盘
- 实现作业的检查点(Checkpoint)
- 失败后可以从Checkpoint恢复

6. DAG的优化
- 动态优化运算顺序
- 切换 JOIN 算法
- 避免重复计算

7. DAG的监控
- Spark UI 展示 DAG
- 直观的了解作业流程
- 标识作业的瓶颈

总结起来,DAG是Spark作业调度和执行的关键抽象模型,理解DAG的工作原理对于Spark性能调优非常重要。

### Spark 中的 Job 和 Action 

1. 区别:

- Job 是 Spark 作业的调度单元,表示一个 Spark action 及其所有依赖的执行路径。一个应用可以包含多个 Job。

- Action 是 Spark 最终执行的操作符,会触发实际的计算,常见的如 collect()、count() 等。一个 Job 会包含一个或多个 Action。

2. 联系:

- Action 触发 Job 的执行。只有遇到 Action,一个 Job 包含的所有 RDD 计算才会被触发执行。

- 每个 Job 依赖于之前的 Job 输出。后续 Job 的输入包含了前面 Job 的输出。

- 每个 Job 包含一个或多个 Action 运算（一个action算子对应一个DAG）,Action 之间存在依赖关系。

- Job 在内部由多个 Stage 组成,Stage 由多 Task 组成。Action 触发 Task 的执行。

Action 触发了整个作业执行,而 Job 表示了全局的执行计划,二者互为依赖与关联。

### DAG和分区在Spark中的关系

1. 分区的并行度决定了DAG中同一个Stage中的Task个数。每个Task对应一个分区的数据处理。

2. DAG生成时,同一个RDD的不同分区会对应不同的Task。提高分区数可以增加Task并行度。

3. 但是分区数过多会导致Task个数增多,引入调度和内存压力。

4. 一个Stage结束后,下一Stage的Task数取决于前Stage输出的分区数。

5. 存在宽依赖的RDD会引入Shuffle,数据重新分区和调度,形成新的Stage。

6. 优化分区可以减少Shuffle,调整DAG结构,提高并行效率。

7. 划分合理的分区尤其对高密集内存操作的性能至关重要。

Spark中的DAG结构和执行过程都与RDD的分区密切相关。合理设置分区数可以优化DAG形状,提高作业效率。

### DAG的宽窄依赖
1. 窄依赖:每个分区最多被一个分区依赖,形成 pipelines,无需 shuffle。

2. 宽依赖:多个分区依赖同一分区,需要 shuffle 重分区。

3. 阶段划分:无shuffle为一个Stage,有shuffle划分为不同Stage。

4. Stage间存在宽依赖,Stage内为窄依赖。

5. 提高Stage内pipeline并行效率,减少shuffle。

6. 重分区会产生新的Stage, Task调度和数据传输。

7. 查看DAG可识别shuffle位置,针对重分区进行优化。 

8. 避免无意义的shuffle,减少Stage数,提高并行度。

9. 划分合理分区数,既兼顾并行也减少开销。

10. DAG的形状直接影响执行性能,是调优的重点。

Spark DAG的宽窄依赖和Stage划分逻辑是理解和调优Spark程序的关键所在。

### 内存迭代计算

1. 数据缓存到内存
- 使用persist()或cache()方法
- 选择不同的存储级别,如MEMORY_ONLY

2. 减少磁盘IO
- 迭代数据在内存中处理,跳过不必要的磁盘读写

3. 避免重复计算
- 对重复使用的RDD,Spark会自动持久化到内存

4. 数据本地化
- 任务调度到计算数据本地的Executor 

5. 并行计算
- 内存数据使用并行Task高效处理

6. 快速交互 
- 内存中数据支持低延迟的迭代计算

7. 减少GC次数
- 相对磁盘,内存计算GC次数更少

8. 数据压缩
- 对内存数据进行压缩以减少空间

9. 向量化计算
- 使用BLAS等库进行向量化计算

10. 容错机制
- 合理设置检查点Checkpoint

### Spark 并行度
1. 并行度的重要性
- Spark通过并行加速分布式计算
- 并行度直接影响作业执行效率

2. 提高并行度的方法
- 增加分区数,加大Task数量
- 优化数据分布,避免数据倾斜
- 开启Speculative Execution
- 增加Executor数量和内存

3. 实现任务并行  
- 一个Stage内多Task并行
- 同时运行不依赖的Stage
- 充分利用集群资源

4. 并行度与资源关系
- 受执行核心数和内存限制
- 并行开销也会占用资源
- 需要权衡资源利用和开销

5. 识别并行瓶颈 
- Spark UI查看任务运行情况
- 执行时间过长表示并行不足

6. 常见的并行优化手段
- 重分区增加Partition数
- 广播变量分发资源 
- 序列化优化,提高单任务效率

7. 调整并行配置
- repartition和coalesce重新分区
- 调整Executor资源规模
- 控制Speculative Execution

8. 持续监控和优化
- 根据Metrics分析并行度
- 持续优化资源利用率

### Spark 任务调度
1. Spark调度器
- Standalone Cluster Manager:Spark自带的简易调度器
- YARN: Hadoop的资源调度平台,Spark可以在YARN上运行
- Mesos: 通用的分布式资源管理器,也支持Spark调度

2. DAGScheduler
- 根据RDD的血缘关系生成有向无环图(DAG)  
- DAG划分成不同的Stage,避免出现循环依赖
- 将Stage根据宽依赖关系分割,形成有序的Stage DAG
- 将最后一个Stage根据RDD Partition划分成Task
- 向下传递Task给TaskScheduler执行

3. TaskScheduler
Spark中的TaskScheduler主要负责将任务(Task)分配给Executor(worker)运行。其主要功能包括:

- 根据应用程序的资源需求,向资源管理器(Standby、YARN等)请求Executor进程。

- 将DAGScheduler生成的Task按照一定策略分配给各个Executor运行。

- 主要的任务分配策略有数据本地性优先和资源利用均衡。

- 在调度池(Pool)的限制下,对不同用户提交的Task进行资源隔离和管理。

- 处理Task的失败重试以及Speculative Execution。

- 根据Executor的状态收集统计和性能指标。

- 与DAGScheduler协同工作,在资源利用和任务调度之间达到平衡。

- 利用BlockManager分发Task所需的块数据。

- 运行在Driver进程中,负责整个Application的任务调度。

- 新版的Spark增加了基于插槽(Slot)的资源隔离和请求模型。

TaskScheduler主要负责将DAGScheduler生成的任务Graph转换为一个运行时的Task调度计划,然后提交给Executor执行的关键组件。


4. 调度模式
- FIFO:先进先出顺序执行Job,易产生饥饿
- FAIR:所有Job公平平等地享有资源,防止饥饿

5. Task失败重试机制
- 固定次数重试失败的Task
- Speculative Execution重复运行相同Task
- 根据Lineage关系重建丢失分区

 ### Spark为什么比MapReduce快？

1. 内存计算

Spark将数据缓存在内存中,进行多次迭代计算。而MapReduce需要读写磁盘,较慢。

2. 避免磁盘IO

Spark基于内存的流水线操作,可以跳过不必要的磁盘IO。MapReduce每个阶段都需要写磁盘。

3. 数据复用

Spark会自动持久化正在使用的RDD,避免重复计算。MapReduce则需要重新读取和计算。

4. 延迟计算

Spark会延迟执行计算直到遇到Action。而MapReduce会即时进行转化操作。

5. 流水线操作

Spark采用流水线方式执行RDD转换操作。MapReduce需要写磁盘再读取,不像流水线。

6. 在内存中维护状态

Spark可以在内存中维护中间状态。而MapReduce作业启动需要重新开始。 

7. 运行环境优化

Spark设计了高效的执行引擎,包括DAG调度、内存管理等。

通过内存计算、流水线执行、避免不必要IO等优化,Spark实现了比MapReduce更高的计算速度。

### 层级关系梳理
- 一个Spark环境可以运行多个Application
- 一段代码运行起来，会成为一个Application
- 一个Application 内部可以有多个Job
- 每个Job由一个action算子产生，并且每一个Job都有自己的DAG执行图
- 一个Job的DAG执行图会基于宽窄依赖划分成不同的阶段
- 不同阶段内基于分区数量，形成多个并行的内存迭代管道
- 每一个内存迭代管道形成一个Task（DAG调度器划分将Job内划分出具体的task任务，一个Job被划分出来的Task在逻辑上称之为这个Job的TaskSet）