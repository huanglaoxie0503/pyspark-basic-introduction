### 一、在 PySpark 中,RDD 的创建主要有以下几种方式:

1. 从集合创建:通过 SparkContext 的 parallelize() 方法从本地集合创建RDD。例如:

```python
data = [1, 2, 3, 4] 
rdd = sc.parallelize(data)
```

2. 从外部存储创建:例如通过 textFile() 读取文本文件,sequenceFile() 读取 SequenceFile,或者从 Hadoop DataSource 读取如 Parquet 等。

3. 从其他 RDD 转换创建:通过对现有 RDD 执行操作后创建新的 RDD,比如 map(), filter(), sample() 等。

4. 从 RDD 分区创建:例如调用 RDD 的 partitionBy() 重新分区后创建 RDD。

5. 从 Spark 共享变量创建:Spark 有 broadcast variable 和 accumulator 两种共享变量,也可以用来创建 RDD。

6. 空RDD的创建:SparkContext 中的 emptyRDD() 方法可以创建一个空的 RDD。

Spark RDD 的创建可以来自不同的数据源,或者对现有 RDD 的继续处理转换。这使得 RDD 可以方便地承载不同场景下的数据和处理流程。


### 二、在PySpark中,可以通过以下几种方式查看RDD的分区信息:

1. 查看RDD的分区数:

```python
rdd.getNumPartitions()
```

这个可以快速得到RDD的分区总数。

2. 查看RDD每个分区的数据:

```python
rdd.glom().collect() 
```

glom()操作将每个分区的数据合并成数组,然后collect()收集所有分区数据。

3. 查看RDD的分区器(Partitioner):

```python
rdd.partitioner 
```

如果打印出来的是None,表示RDD使用的是HashPartitioner。

4. 查看RDD的分区数据大小:

```python
rdd.mapPartitionsWithIndex(lambda idx, it: len(list(it))).collect()
```

遍历每个分区并返回数据大小。

5. 查看RDD缓存存储信息:

```python
rdd.getStorageLevel()
```

可以看RDD缓存的方式(内存、磁盘等)。

所以Spark提供了多种方式去查看RDD的分区信息,可以用来调试和优化程序。

### 三、在 RDD 中,transformation 和 action 的主要区别有:

1. 计算时机不同:

- Transformation 是惰性计算,只会记住计算逻辑,不会立即执行。

- Action 会触发实际计算,强制执行包含的 transformation 操作。

2. 返回类型不同:

- Transformation 返回一个新的 RDD。例如 map() 返回一个映射后的新 RDD。

- Action 返回一个具体的计算结果。例如 reduce() 返回聚合的值。

3. 函数类型不同: 

- Transformation 通过传递函数来操作 RDD 中的每个元素。

- Action 通过执行计算来返回结果。

4. 调用场景不同:

- Transformation 主要用于构建 RDD 之间的依赖关系和运算流程。

- Action 触发整个流程的实际执行,并将结果返回给用户。

总结为:

- Transformation 构建计算流程,Action 触发计算。

- Transformation 返回 RDD,Action 返回值。

它们共同构成了 RDD 的惰性计算模型。

### 四、在 RDD 的 action 操作中,有几个常见的操作可以直接输出结果,不需要传回 driver 程序:

1. foreach(): 对 RDD 每个元素应用函数,常用于输出。计算在 executor 端进行。

2. foreachPartition():对 RDD 每个分区应用函数,常用于输出。计算在 executor 端进行。

3. saveAsTextFile():将 RDD 保存为文本文件到分布式存储系统,不会返回结果。

4. saveAsSequenceFile():将 RDD 保存为 SequenceFile 格式,不会返回结果。

5. saveAsObjectFile(): 将 RDD 元素序列化后保存为 SequenceFile,不会返回结果。

这些 action 操作可以避免将结果传送回 driver 程序,减少开销,适合输出、保存等不需要返回结果的场景。

另外一些 action 如 reduce(), collect() 等需要将结果聚合后返回给 driver 程序。

所以按结果返回 Location,可以将 action 分为两类,选择需要的 action 可以提高效率。

### 五、groupByKey和reduceByKey主要的区别有:

1. 聚合计算方面:

- groupByKey将数据集按Key进行分组,然后对每组中的Value集合进行聚合,返回一个(Key, Iterable)的RDD。

- reduceByKey在每个分区内对相同Key的数据进行预聚合,然后跨分区进行Value的合并,返回一个(Key, Value)的RDD。

2. 性能方面:

- groupByKey因为在驱动程序中收集Value集合,可能会造成OOM或GC overhead。reduceByKey是在mapper端进行预聚合,性能更好。

- groupByKey的输出是Iterable,如果需要进行进一步计算会重复遍历元素。reduceByKey的输出是单个值。

- reduceByKey可以在shuffle前结合combine提前聚合,优化性能。groupByKey没有combine操作。

3. 适用场景:

- groupByKey适用于按key分组后对values进行多种聚合的场景。reduceByKey更适合按key进行sum、min、max等聚合计算。

- groupByKey输出是列表,易读性更好,方便调试。reduceByKey可自定义输出类型。

总的概括来说：
- groupByKey是按key分组,然后对value集合进行聚合,在driver端集中处理,容易产生数据倾斜。
- reduceByKey是在mapper端进行combiner预聚合,然后聚合器合并value,更加分布式。

所以,reduceByKey性能更优但使用场景有限。groupByKey功能更丰富但需要注意OOM问题。需要根据实际情况选择。

### 六、在RDD中,mapPartitions()和foreachPartition()主要的区别有:

1. 计算功能上:

- mapPartitions()会对每个分区的元素应用一个函数,并返回一个新的RDD。类似map(),但应用于每个分区。

- foreachPartition()对每个分区应用一个函数,但不会返回新RDD,仅用于副作用操作。类似foreach()。

2. 返回值上:

- mapPartitions()返回一个新的RDD,包含了每分区映射的结果。

- foreachPartition()不返回任何值,只产生操作的副作用。

3. 是否lazy:

- mapPartitions()是lazy操作,必须调用action触发。

- foreachPartition()会直接执行函数操作,不lazy。

4. 使用场景上:

- mapPartitions()多用于RDD中需要分区级转换的场景。

- foreachPartition()多用于输出、保存等需要副作用的场景。

综上,mapPartitions()更侧重计算新值,foreachPartition()更侧重执行副作用操作。需要返回RDD时用前者,仅副作用用后者。

### 七、RDD 的分区(partition)相关操作,主要需要注意以下几个方面:

1. 分区数设置

- 合理设置分区数,一般与集群核数相 当或略多,避免过多分区。

- transformations会改变分区数,需要注意。

2. 分区函数

- 自定义分区时要注意产生数据倾斜的问题。

- 考虑 hash、range 等分区方式的特点。

3. Shuffle 操作

- repartition、join 等会 shuffle 数据,要注意效率。

- shuffle 前先执行 coalesce 或 reduceByKey 提高效率。 

4. 边界处理

- 少量极大、极小值要注意处理,不要进入同一个分区。

- 每分区计算时注意 empty partitions 的问题。

5. 并行度

- 合理设置并行任务数,避免资源浪费或竞争。

所以分区直接影响执行性能和计算正确性,需要根据数据和计算模式优化，尽量不要增加分区。测试和调优也很重要。

### 八、RDD 中,部分Transformation 操作会导致分区数的改变,主要包括:

1. coalesce - 缩减分区数

coalesce可以将RDD的分区数减少到指定数量,可以优化小数据集的处理。

```python
rdd = rdd.coalesce(3)
```

2. repartition - 增加分区数

repartition会根据参数创建额外的分区,可以优化数据倾斜。

```python
rdd = rdd.repartition(10)
```

3. reduceByKey - 减少分区 

reduceByKey会将相同key的数据合并,因此会减少分区数。

```python 
rdd = rdd.reduceByKey(func)
```

4. groupByKey - 增加分区

groupByKey会根据key分组,可能会产生更多分区。

```python
rdd = rdd.groupByKey()  
```

5. join - 根据join类型改变分区数

join会根据join类型(shuffle join)重新分区。

```python
rdd = rdd1.join(rdd2)
```

所以在使用这些操作时需要注意分区数的变化,并根据业务需求进行优化。