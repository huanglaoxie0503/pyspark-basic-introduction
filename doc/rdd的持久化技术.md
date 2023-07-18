# PySpark RDD 持久化技术

### 缓存

#### 1. 持久化的基本原理

- RDD持久化是将运行过程中的RDD数据保存下来,避免再次计算
- 使用persist()或cache()方法将RDD标记为持久化
- 底层是Spark的BlockManager实现存储管理
- 实际持久化发生在首次计算时,之后从持久化存储获取

#### 2. 持久化级别(StorageLevel)

- NONE - 不持久化,RDD每次操作都会重新计算
- DISK_ONLY - 仅将RDD数据持久化到磁盘
- DISK_ONLY_2 - 将RDD数据复制到2个节点的磁盘上
- DISK_ONLY_3 - 将RDD数据复制到3个节点的磁盘上
- MEMORY_ONLY - 仅将RDD数据持久化到内存中
- MEMORY_ONLY_2 - 将RDD数据复制到2个节点的内存中
- MEMORY_AND_DISK - 将RDD数据持久化到内存和磁盘上
- MEMORY_AND_DISK_2 - 将RDD数据复制到2个节点的内存和磁盘上
- OFF_HEAP - 在off-heap上分配内存来持久化RDD数据
- MEMORY_AND_DISK_DESER - 将反序列化的RDD数据持久化到内存和磁盘上

#### 3. 持久化技术细节

- 内存存储:直接或序列化后存储在内存
- 磁盘存储:序列化数据写入磁盘文件
- 数据复制:提高容错性
- 数据序列化:减少内存开销
- 数据分区:优化存储分布

#### 4. 持久化管理

- BlockManager负责存储空间管理
- 采用LRU驱逐算法回收空间
- 通过unpersist()移除持久化

#### 5. 使用建议 

- 根据数据大小/重用次数选取级别
- 监控内存使用和缓存命中率 
- 测试不同级别的性能
- 调优序列化参数
- 分区数不要过大或过小

### CheckPoint 
 RDD的checkpoint操作可以将RDD的数据保存到持久化存储中,用于容错恢复，设计上认为是安全的，仅支持硬盘存储。

1. 工作原理

- checkpoint会在RDD的血统(lineage)上切断依赖,并创建一个新的RDD
- 新的RDD的每个分区会保存到可靠的分布式存储系统中,如HDFS
- 如果之前的RDD分区丢失,可以从checkpoint中恢复

2. 使用方法

- 通过RDD的checkpoint()方法设置检查点 
- 需要指定一个可靠的存储系统,如SparkContext.setCheckpointDir()

3. 注意事项

- checkpoint会切断血统,可能导致重复数据,需注意避免
- 保存 checkpoint 的存储系统必须可靠
- checkpoint后会增加恢复时间开销
- 不要对存在血统的RDD做checkpoint

4. 应用场景

- 需要容错保证的关键数据
- 长时间迭代型应用中的中间数据

checkpoint是一个重要的容错机制,通过牺牲计算效率获得故障恢复能力。需要针对场景采取适当的checkpoint策略。

### RDD的缓存技术和checkpoint的主要区别和对比如下:

1. 目的不同

- 缓存是为了避免重复计算,加速运算，轻量级保存RDD，分散存储。
- checkpoint是为了提供容错能力,保证数据不丢失，重量级保存RDD，集中存储。

2. 使用方法

- 缓存通过persist()和cache()完成
- checkpoint通过RDD的checkpoint()方法

3. 工作机制

- 缓存会在血统中保持RDD间依赖关系 
- checkpoint会切断血统,创建新的RDD

4. 存储位置

- 缓存通常在内存和本地磁盘
- checkpoint在可靠的分布式存储，如HDFS

5. 性能影响

- 缓存可以提高计算性能，效率高。
- checkpoint会降低计算性能，效率会慢一些。

6. 内存占用

- 缓存会长期占用内存
- checkpoint的数据通常不加载到内存

7. 数据一致性

- 缓存不保证数据一致性
- checkpoint的数据可用于恢复丢失分区

缓存用于加速运算,checkpoint用于容错。两者在目的、使用场景上有明确区别。需要根据具体需求选择使用。