#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyspark import SparkConf, SparkContext


def rdd_countByKey(spark):
    """
    countByKey() 方法将返回一个字典,键是 RDD 中的键,值是每个键出现的次数。

    countByKey() 是一个action算子,会触发 RDD 的实际计算。它对大数据集很有用,可以高效地统计每个键的出现次数。

    需要注意的是,countByKey() 要求 RDD 中的键是可哈希的,需要实现 hash() 方法。如果键不是可哈希的,可以使用 map() 先转换为可哈希键,再使用 countByKey()。
    :param spark:
    :return:
    """
    file_name = '/Users/oscar/projects/big_data/pyspark-basic-introduction/data/words.txt'
    rdd = spark.textFile(file_name)

    rdd2 = rdd.flatMap(lambda x: x.split(' ')).map(lambda x: (x, 1))
    # 通过 countByKey() 对key进行奇数
    # 返回值类型：<class 'collections.defaultdict'>
    result = rdd2.countByKey()
    print(result)
    print(type(result))


def rdd_collect(spark):
    """
    collect() 是一个行动操作(action),它是将 RDD 中所有分区的数据按顺序收集到驱动程序中,形成一个 Python list

    collect() 的具体工作流程是:
        1. 触发 RDD 的计算。
        2. 从 RDD 的各个分区中读取数据,发送到驱动程序。
        3. 在驱动程序中,将分区数据合并成一个 Python list。
        4. 返回这个 Python list。

    collect() 的注意事项:
        1. collect() 将整个 RDD的数据收集到驱动程序内存中,如果数据量很大,可能会造成内存溢出。
        2. collect() 的结果不会更新,是 RDD 执行时的静态快照。
        3. 生产环境中很少使用 collect(),更多的是保存 RDD 内容到外部存储系统。
        4. 在另一个行动操作比如 count() 之后,不可以再次使用 collect(),否则会抛出错误。

    所以 collect() 适用于对小规模 RDD 进行调试,不适用于大数据量的 RDD。需要收集大规模 RDD 数据时,可以采用其他方式保存。

    对于需要收集大规模 RDD 数据的情况,可以考虑使用以下几种方式保存,而不是直接使用 collect():
        1. 保存为文本文件：可以使用 saveAsTextFile() 方法将 RDD 保存为文本文件到分布式存储系统如 HDFS 中: rdd.saveAsTextFile("hdfs://...")
        2. 保存为 SequenceFile：SequenceFile 是Hadoop用来存储二进制形式的key-value对的数据格式。可以使用 sparkContext.sequenceFile() 进行保存。
        3. 保存为 Parquet 文件：Parquet是列式存储格式,支持很好的压缩率和查询性能，可以用 saveAsParquetFile() 方法保存。
        4. 保存到 Hive 表中：可以通过创建 Hive 外部表,将 RDD 数据加载到 Hive 中。
        5. 保存到数据库：可以通过 JDBC 将 RDD 中的数据保存到关系型数据库。
        6. 输出到 Kafka：可以通过 foreach + Kafka producer 将数据输出到 Kafka 中。
    :param spark:
    :return:
    """
    rdd = spark.parallelize([1, 2, 3, 4])

    result = rdd.collect()

    print(result)

    # [1, 2, 3, 4]


def rdd_reduce(spark):
    """
    reduce 是一个行动操作(action),它会对 RDD 元素进行聚合,返回一个聚合结果。
    reduce() 的使用方法为：rdd.reduce(func)

    reduce() 的注意事项:
       1. 初始值:reduce() 中并没有初始值的概念。第一次调用时,x 和 y 会是分区内的前两个元素。
       2. 分区顺序:reduce() 中分区之间的计算顺序是不确定的。
       3. 空分区:如果RDD某个分区数据为空,reduce会抛出异常。
       4. 并行度:reduce的并行度由分区数决定。

    所以 reduce() 适用于分区内及分区间可以交换的聚合计算,且元素类型固定。如果要指定初始值或者控制聚合顺序,可以考虑 aggregate() 操作。
    :param spark:
    :return:
    """
    rdd = spark.parallelize([1, 2, 3, 4, 5])

    result = rdd.reduce(lambda x, y: x + y)

    print(result)  # 输出:10


def rdd_fold(spark):
    """
    fold() 是与 reduce() 类似的聚合操作,主要区别在于 fold() 可以指定初始值。

    fold() 的语法是: rdd.fold(zeroValue, func) 这里 zeroValue 是指定的初始值,func 是聚合函数

    fold() 和 reduce() 主要的区别:
        1. fold() 可以指定初始值,而 reduce() 不能指定,它依赖第一对元素作为起始值。
        2. 当RDD某个分区数据为空时,reduce会失败,而fold可以正常执行。
        3. fold() 的最终结果不受分区顺序影响,reduce() 的顺序有影响。
        4. fold() 的类型可以与初始值类型不同,reduce() 的类型必须与元素类型相同。

    当需要指定初始值时,fold() 功能更强大,也更安全。但 reduce() 在不需要初始值时,用法更简洁。
    :param spark:
    :return:
    """
    rdd = spark.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9], 3)

    result = rdd.fold(10, lambda x, y: x + y)

    print(result)  # 输出:10


def rdd_first(spark):
    """
    first() 是一个行动操作(action),它返回 RDD 中的第一个元素

    first() 的一些重要注意事项:
        1. RDD 如果为空,则会抛出异常。可以结合 rdd.isEmpty() 先判断是否为空。
        2. 只会返回第一个元素,不会触发全量计算。常用于测试 RDD 是否正确生成。
        3. 元素顺序取决于 RDD 的分区规则。不能依赖 first() 获取“最小”或“最大”元素。
        4. 对于每个 RDD,first() 操作只会执行一次,后续再调用会直接返回第一次的结果,而不会重新计算。
        5. 如果 RDD 排序后再取 first(),可以获取排序后的第一个元素。

    所以 first() 适用于测试 RDD 功能的小规模数据集,不适用于生产环境下的大数据量 RDD。在生产环境下,better first() 要结合其他操作符使用,比如取样本数据。
    :param spark:
    :return:
    """
    rdd = spark.parallelize([4, 2, 3, 1, 3, 5, 6, 7, 8, 9])

    print(rdd.first())  # 输出 4


def rdd_take(spark):
    """
    take()是一个行动操作(action),它从RDD中按顺序返回n个元素组成的列表。

    take()的基本语法: rdd.take(n) 这里n表示要返回的元素数量。
    take()会触发RDD的计算,从RDD第一个分区按顺序取出n个元素,如果不足n个则取完为止。

    take()的一些重要注意事项:
        1. n的值通常不要设太大,否则会导致驱动程序内存溢出。
        2. 如果RDD元素数量少于n,则只会返回所有元素。
        3. 每个RDD只会取一次,重复调用不会重新计算。
        4. 返回结果顺序依赖RDD的分区。
        5. 如果要实现采样,应该使用takeSample(),而不是take()。
    所以take()适用于测试小规模RDD,或者取RDD前几个元素。不适合生产环境下的大规模采样操作。在大数据场景下,建议使用takeSample()进行随机或分层抽样。
    :param spark:
    :return:
    """
    rdd = spark.parallelize([3, 1, 4, 2])

    print(rdd.take(2))  # [3, 1]


def rdd_takeSample(spark):
    """
    takeSample() 是实现从 RDD 抽样的常用action算子。它可以随机抽样或按特定比例进行分层抽样。

    takeSample() 的基本语法: rdd.takeSample(withReplacement, num, [seed])
        1. withReplacement:是否是放回抽样,True为放回,False为不放回
        2. num:抽样大小
        3. seed:随机种子(可选)

    takeSample() 的优点是:
        1. 可以指定任意抽样比例,不会OOM。
        2. 支持随机和分层抽样。
        3. 抽样结果可重复。
    所以 takeSample() 非常适合生产环境下在大数据量 RDD 进行抽样分析的场景。
    需要注意的参数调优,比如当样本量过小时可能会出现误差。可以多次抽样取平均值来提高准确度。
    :return:
    """
    rdd = spark.parallelize([4, 2, 3, 1, 3, 5, 6, 7, 8, 9])
    print(rdd.takeSample(True, 22))  # [6, 7, 3, 7, 2, 3, 8, 1, 4, 3, 3, 3, 5, 3, 6, 9, 9, 2, 6, 1, 3, 2]
    print(rdd.takeSample(False, 22))  # [3, 4, 6, 2, 3, 1, 5, 8, 9, 7]
    print(rdd.takeSample(False, 5))  # [8, 2, 4, 7, 6]
    # rdd.takeSample(False, 10)
    # rdd.takeSample(False, 0.1, 888)


def rdd_top(spark):
    """
    top()是一个action算子,它可以用来获取RDD中按指定顺序排序后的前N个元素。

    top() 的基本语法:rdd.top(num, key=None) 其中num表示获取前多少个元素,key是一个用于排序的lambda函数。

    top()的一些重要注意事项:
        1. 默认按元素自然顺序排序(降序),也可以传入key函数进行自定义排序。
        2. 如果RDD元素少于num,则只会返回所有元素。
        3. 返回列表顺序依赖于RDD的分区。
        4. 重复调用top()不会重新计算,结果固定。

    所以top()适用于获取RDD的top N元素。但前提需要提前进行全局排序,否则获取的可能不是真正意义上的top N元素。
    如果只是需要采样或抽样,不要使用top(),而要使用take()或takeSample()。
    :param spark:
    :return:
    """
    rdd = spark.parallelize([4, 2, 3, 1])

    # 默认递增排序
    print(rdd.top(2))  # [1, 2]

    # 递减排序
    print(rdd.top(2, key=lambda x: -x))  # [4, 3]


def rdd_count(spark):
    """
    count()是一个行动操作,用于返回RDD中的元素个数。
    count()的基本语法如下: rdd.count()
    count()会遍历RDD中的每一个分区,将分区内的元素个数加在一起,从而得到RDD的总元素个数。

    count()的一些重要注意事项:
        1. count()会触发RDD的运算并返回结果,不会像取前几个元素那样延迟计算。
        2. count()在重复调用时只会计算一次,不会重新计算。
        3. count()会将RDD的每个分区元素个数加起来,而不会将数据拉取到驱动程序中。
        4. 如果想返回唯一元素的个数,可以在count()前调用distinct()进行去重。

    所以count()适用于快速获取RDD总元素个数的场景。但注意不要对过于大的数据集直接使用count(),因为计算所有的元素会消耗大量资源。
    :param spark:
    :return:
    """
    rdd = spark.parallelize([1, 2, 3, 4, 5])
    print(rdd.count())  # 4


def rdd_takeOrdered(spark):
    """
    takeOrdered() 是获取RDD按指定顺序排序后的前N个元素的行动操作。
    takeOrdered() 的基本语法: rdd.takeOrdered(n, key) 其中n表示取前n个元素,key表示按什么规则排序.

    takeOrdered() 的一些注意事项:
        1. 要求整个RDD先进行全局排序,这样才能正确获取到前N个元素。
        2. 如果RDD元素数量少于n,则只返回所有元素。
        3. 返回的顺序依赖RDD的分区。
        4. 仅计算一次,不会重新触发计算。
        5. 如果只要采样元素,建议使用take()或takeSample()。
    所以takeOrdered()最适合在需要获得排序后的数据时使用,但需要注意提前进行全局排序。如果只是要采样,不要使用它。
    :param spark:
    :return:
    """
    rdd = spark.parallelize([2, 3, 1, 4, 5, 7, 6, 9, 8])
    print(rdd.takeOrdered(3))
    # 根据lambda表达式中的排序规则取前2个元素。
    result = rdd.takeOrdered(2, lambda x: -x)  # [4, 3]
    print(result)


def rdd_foreach(spark):
    """
    foreach() 是最常用的输出操作之一。它会将函数应用到 RDD 的每一个元素,常用于对 RDD 进行遍历或输出
    foreach() 的基本语法: rdd.foreach(func) 这里 func 是一个处理函数,会应用到 RDD 每个元素

    foreach() 的一些重要特点:
        1. 用于对 RDD 所有元素进行遍历,通常用于输出。
        2. 函数内部不应该改变外部变量,foreach 是为了副作用而运行。
        3. 不会对元素进行归约,只进行遍历。
        4. foreach 的结果不会返回,也不能改变 RDD 内容。
        5. 可以通过分区foreach控制并行度。
    所以 foreach 主要用于 RDD 输出,或者副作用操作,不适合用于聚合计算等改变 RDD 内容的操作。
    :param spark:
    :return:
    """
    rdd = spark.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9])

    def f(x):
        print(x * 20)

    # 写法一
    rdd.foreach(f)
    # 写法二
    rdd.foreach(lambda x: print(x * 10))


def rdd_saveAsTextFile(spark):
    """
    saveAsTextFile() 用来将 RDD 中的数据以文本文件的形式保存到分布式存储系统中,比如 HDFS、S3 等。
    saveAsTextFile() 的基本语法: rdd.saveAsTextFile(path) 这里 path 为要保存的路径,需要指定为分布式存储系统中的路径。

    saveAsTextFile() 的一些注意事项:
        1. 默认按照分区保存为多个文件,可以通过 coalesce() 或 repartition() 控制文件数。
        2. 也可以指定格式为 SequenceFile 等其他格式。
        3. 保存操作是 lazy 的,只有在action时才会真正保存。
        4. saveAsTextFile() 不能保存空 RDD,需要先过滤掉空。
    所以,saveAsTextFile() 提供了一个将 RDD 数据持久化保存的简单方案,适合用于将处理后的数据保存到 HDFS、S3 等外部系统中。
    :param spark:
    :return:
    """
    rdd = spark.parallelize(["Spark", "PySpark", "Big Data"]).coalesce(4)
    # 定义分区
    # rdd = spark.parallelize(["Spark", "PySpark", "Big Data"], 3)
    # 将 RDD 的每个元素以字符串形式保存为一个文件,写入到 HDFS 的 /user/hdfs/rdd 路径下。
    rdd.saveAsTextFile("/Users/oscar/projects/big_data/pyspark-basic-introduction/data/rdd")


def rdd_mapPartitions(spark):
    """
    mapPartitions() 是对每个分区进行数据转换的操作。它为每个分区单独运行 map 函数,类似于 map(),但更侧重于分区级的操作。
    mapPartitions() 的基本语法: rdd.mapPartitions(f) 这里 f 是一个函数,它接受一个分区数据的迭代器,并返回转换后的迭代器

    mapPartitions() 的特点:
        1. 对每个分区单独运行 map 函数,提高了并行度。
        2. 函数处理的是分区内全部数据,可以优化内存使用。
        3. 如果函数中需要创建额外连接或资源,也只需要创建一次。
        4. 但是分区无法相互通信。

    所以 mapPartitions() 在需要分区级的操作时很有用,但不适合分区之间需要协同的操作。
    :param spark:
    :return:
    """
    rdd = spark.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9], 3)

    def f(iters):
        result = []
        for x in iters:
            result.append(x ** 2)
        return result

    print(rdd.mapPartitions(f).collect())
    # [1, 4, 9, 16, 25, 36, 49, 64, 81]


def rdd_foreachPartition(spark):
    """
    foreachPartition() 是对每个分区进行操作的方法。它类似于 foreach(),但函数是针对每个分区而不是每个元素。
    foreachPartition() 的基本语法: rdd.foreachPartition(f)
    :param spark:
    :return:
    """
    rdd = spark.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9], 3)

    def f(iters):
        print("Partition:")
        for x in iters:
            print(x)

    rdd.foreachPartition(f)
    """
    Partition:
    4
    5
    6
    Partition:
    1
    2
    3
    Partition:
    7
    8
    9
    """


def rdd_partitionBy(spark):
    """
    partitionBy() 用于根据指定的分区规则对 RDD 进行重新分区。
    partitionBy() 的基本语法是: rdd.partitionBy(numPartitions, partitionFunc) 这里 numPartitions 指定新的分区数,partitionFunc 是分区函数,输入元素并返回其分区ID。

    partitionBy() 的常见分区策略:
        1. HashPartitioner:基于 hash 实现分区
        2. RangePartitioner:基于范围实现分区
        3. CustomPartitioner:自定义分区函数
    partitionBy() 允许我们根据业务需求进行自定义分区,实现更优的工作负载分布和执行性能。
    但注意重新分区是一个 shuffle 操作,有一定 overhead,不要过度使用。
    :param spark:
    :return:
    """
    rdd = spark.parallelize(
        [
            ('hadoop', 1),
            ('spark', 1),
            ('flink', 1),
            ('hadoop', 1),
            ('spark', 1)
        ]
    )

    def process(k):
        if 'hadoop' == k:
            return 0
        if 'spark' == k:
            return 1
        else:
            return 2

    print(rdd.partitionBy(3, process).glom().collect())


def rdd_repartition(spark):
    """
    repartition() 用于对 RDD 进行重新分区。它可以扩大或缩小 RDD 的分区数。
    repartition() 的基本语法: rdd.repartition(numPartitions) 这里 numPartitions 指定新的分区数

    repartition() 的一些注意事项:
        1. repartition 是一个 wide 操作,将通过 shuffle 进行大量数据交换。
        2. 如果新分区数与原数相同,则不会进行实际重新分区。
        3. 一般在进行 join 等 shuffle 操作前预先重新分区,可以减少 shuffle 数据。
        4. 可以通过 coalesce() 缩减分区数,避免 shuffle。
        5. 重新分区是一个耗资源的操作,需要根据业务需求进行优化。
        6. 不要以为分区数越多越好,要基于数据大小、执行器核数等因素进行确定。
    所以 repartition 需要根据实际情况进行分区数优化,以平衡任务执行性能。过度使用可能带来额外开销。
    :return:
    """
    rdd = spark.parallelize([1, 2, 3, 4, 5], 3)
    print(rdd.getNumPartitions())
    print(rdd.repartition(1).getNumPartitions())
    print(rdd.repartition(5).getNumPartitions())
    # coalesce 修改分区
    print(rdd.coalesce(1).getNumPartitions())
    print(rdd.coalesce(5, shuffle=True).getNumPartitions())
    # 将原有的 2 个分区重新分为 4 个分区
    # print(rdd.repartition(4).glom().collect())
    # [[1], [2], [3], [4]]


if __name__ == '__main__':
    # 初始化执行环境，构建 SparkContext 对象
    conf = SparkConf()
    conf.setAppName("RddTransformation")
    conf.setMaster("local[*]")

    sc = SparkContext(conf=conf)

    # rdd_countByKey(spark=sc)
    # rdd_collect(spark=sc)
    # rdd_reduce(spark=sc)
    # rdd_fold(spark=sc)
    # rdd_first(spark=sc)
    # rdd_take(spark=sc)
    # rdd_top(spark=sc)
    # rdd_count(spark=sc)
    # rdd_takeSample(spark=sc)
    # rdd_takeOrdered(spark=sc)
    # rdd_foreach(spark=sc)
    # rdd_saveAsTextFile(spark=sc)
    # rdd_mapPartitions(spark=sc)
    # rdd_foreachPartition(spark=sc)
    # rdd_partitionBy(spark=sc)
    rdd_repartition(spark=sc)

    """
    在 action 算子中：foreach、saveAsTextFile 是分区(Executor) 直接执行的，跳过Driver,其余算子都会将结果发送到Driver
    """

    """
    groupByKey是按key分组,然后对value集合进行聚合,在driver端集中处理,容易产生数据倾斜。
    reduceByKey是在mapper端进行combiner预聚合,然后聚合器合并value,更加分布式。
   
    groupByKey和reduceByKey的区别:
    groupByKey
        从上往下有两个阶段:Map和Reduce
        Map阶段中有一个数据集,经过map后按Key分成了两个组
        Reduce阶段中对两个组的数据Value集合进行聚合操作
        最后输出了两个(Key, Value集合)
    reduceByKey
        也有Map和Reduce两个阶段
        Map阶段中有一个组合器(combiner)对每个Key的Value进行预聚合
        Reduce阶段中对Value继续进行聚合,并输出(Key, Value)
    """
