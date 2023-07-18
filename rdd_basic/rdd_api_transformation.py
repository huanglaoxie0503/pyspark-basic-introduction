#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyspark import SparkConf, SparkContext


def rdd_map(spark):
    """
    map(func):返回一个新的RDD,该RDD由每一个输入元素经过func函数转换后组成
    :return:
    """
    rdd = spark.parallelize([1, 2, 3, 4, 5, 6], 3)

    def add(data):
        return data * 10

    # 写法一、自定义犯法
    print(rdd.map(add).collect())

    # 写法二、lambda 表达式来写匿名函数
    print(rdd.map(lambda x: x * 10).collect())


def rdd_flatMap(spark):
    """
    flatMap(func):类似map,但每个输入元素可以映射为0或多个输出元素
    :param spark:
    :return:
    """
    rdd = spark.parallelize(
        [
            'hadoop spark hadoop',
            'spark hadoop hadoop',
            'hadoop flink spark'
        ]
    )
    # 获取所有的单词，组成新的RDD，flatMap 的传入参数和 map 一致，解除嵌套无需逻辑
    rdd2 = rdd.flatMap(lambda line: line.split(' '))
    print(rdd2.collect())
    # ['hadoop', 'spark', 'hadoop', 'spark', 'hadoop', 'hadoop', 'hadoop', 'flink', 'spark']


def rdd_mapValues(spark):
    """
    mapValues()的主要作用是将RDD中的value进行转换,key保持不变
    mapValues()与map()的不同之处在于,map()是作用于RDD中的每一个元素,即同时可以转换key和value;而mapValues()只是作用于value,保持key不变。
    适合使用mapValues()的场景例如:
        a、需要更新RDD中value的值而保持key不变
        b、需要将value转换为另一种类型而不影响key
        c、需要对value进行过滤或去重而保持key的对应关系
        d、需要保持RDD中数据的分区,而仅转换value
    因为mapValues()不会影响key,所以不会引起数据重新分区,避免了额外的shuffle开销。
    mapValues()是一个非常有用的RDD转换函数,可以避免不必要的shuffle并保持数据分区,适合在不需要改变key的情况下仅转换value的场景。和map()相比,mapValues()可以带来更好的性能和效率。
    :param spark:
    :return:
    """
    rdd = spark.parallelize(
        [
            ('a', 1),
            ('a', 11),
            ('a', 6),
            ('b', 3),
            ('b', 9)
        ]
    )
    # 将二元组的所有value都乘以10进行处理
    print(rdd.mapValues(lambda x: x * 10).collect())


def rdd_reduceByKey(spark):
    """
    reduceByKey的主要作用是将RDD中相同key的值聚合起来,其输入和输出都是一个key-value对的RDD。
    reduceByKey的工作原理是:
    1、 在shuffle阶段,将相同key的数据分到同一个partition中
    2、在每个partition内部,将相同key的值按照传入的func函数进行聚合
    3、将不同partition的数据聚合的结果返回
    :return:
    """
    rdd = spark.parallelize(
        [
            ('a', 1),
            ('a', 1),
            ('b', 1),
            ('b', 1),
            ('b', 1),
        ]
    )
    result = rdd.reduceByKey(lambda x, y: x + y)
    print(result.collect())  # [('a', 2), ('b', 3)]


def rdd_groupBy(spark):
    """
    将RDD里的每一个元素根据key进行分组,key相同的元素放到一个迭代器中。

    groupBy会将传入的函数应用到RDD的每一个元素上,根据返回的key进行分组。返回一个以key为组的RDD,其中每个组包含对应该key的所有元素。

    groupBy与groupByKey的区别在于:
        1. groupByKey是按照元素本身的key进行分组,而groupBy可以通过函数产生key进行分组
        2. groupByKey的输入是KV类型的RDD,输出是KV类型;groupBy的输入输出都是非KV类型
        3. groupByKey维护每个key对应的value集合,groupBy仅保留相同key的元素
    相比groupByKey,groupBy可以更加灵活地指定分组规则,不需要输入是KV类型。

    groupBy的注意事项:
        1. 会引起shuffle操作,对数据分区进行重组
        2. 对结果RDD进行操作需要注意分组键的作用范围
        3. 可以结合map、filter等进行后续操作优化流程

    所以groupBy可以实现更灵活按需的分组操作,但需要注意repartition造成的性能影响。
    :param spark:
    :return:
    """
    rdd = spark.parallelize(
        [
            ('a', 1),
            ('a', 1),
            ('b', 1),
            ('b', 1),
            ('b', 1),
        ]
    )
    result = rdd.groupBy(lambda x: x[0])
    print(result.map(lambda x: (x[0], list(x[1]))).collect())
    # [('a', [('a', 1), ('a', 1)]), ('b', [('b', 1), ('b', 1), ('b', 1)])]


def rdd_groupByKey(spark):
    """
    groupByKey的主要作用是将RDD中的值按照key进行分组,key相同的值会放到一个迭代器里。它的输入和输出都是一个key-value对的RDD。
    grouped_rdd中的每个元素格式为(key, Iterable[value])。

    groupByKey的工作原理是:
        1. 在shuffle阶段,将相同key的元素散列到同一个partition中
        2. 在每个partition内部,将相同key的值放到一个迭代器中
        3. 将不同partition的数据按照key分组后的结果返回

    groupByKey会引起shuffle操作,性能上不如reduceByKey,但是当进行类似word count统计时,groupByKey更加灵活,可以获取每个key对应的完整value集合。

    groupByKey的注意事项:
        1. 会产生大量shuffle数据,容易造成OOM,可以先进行combineByKey或reduceByKey局部聚合
        2. 输出结果是不确定顺序的,需要后续排序
        3. 对每个key对应的value集合重新分区或聚合可以优化性能

    所以groupByKey适用于每个key对value集合进行自定义操作和多次迭代的场景,但需要注意数据规模和 shuffle 操作带来的性能影响。
    :return:
    """
    rdd = spark.parallelize(
        [
            ('a', 1),
            ('a', 1),
            ('b', 1),
            ('b', 1),
            ('b', 1),
        ]
    )
    result = rdd.groupByKey()
    print(result.map(lambda x: (x[0], sum(list(x[1])))).collect())
    # [('a', 2), ('b', 3)]


def rdd_filter(spark):
    """
    filter(func):返回一个新的RDD,该RDD仅包括使func返回True的输入RDD中的元素

    filter对RDD进行过滤,filter接受一个函数作为参数,该函数会应用到RDD的每一个元素,并返回一个新的RDD,其中只包含使得该函数返回True的元素

    filter与map的区别在于:
        1. map会将函数应用到RDD每个元素后返回新的元素,filter根据该函数返回布尔值来过滤元素
        2. map通常用来转换元素,filter用来选择符合条件的元素
        3. filter后面RDD的元素个数一定<=原RDD,map后的RDD元素个数不变
    需要注意的几点:
        1. filter中的函数需要返回布尔值,否则会过滤掉所有元素
        2. filter是lazy操作,只有遇到行动操作才会触发计算
        3. 多次filter可以结合起来连续使用,避免中间结果持久化
        4. filter后面通常需要有后续行动操作输出结果

    filter是一个非常有用的转换操作,可以用来过滤掉不符合要求的数据,减少计算和内存占用,提高操作效率,经常会与其他算子结合使用。
    :param spark:
    :return:
    """
    rdd = spark.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9])

    # 通过 filter 算子，过滤奇数
    result = rdd.filter(lambda x: x % 2 == 1)
    print(result.collect())
    # [1, 3, 5, 7, 9]


def rdd_distinct(spark):
    """
    对RDD进行去重,返回一个新的RDD,其中包含了原RDD中的不同的元素(根据元素本身比较而判断)。

    distinct实现去重的原理是通过map和reduce操作来实现的:
        1. 首先通过map将每个元素映射到一个(element, null)对
        2. 对这些键值对进行reduceByKey操作,将相同的键值合并,这样每一个键只会对应一个值
        3. 最后将键值对的键返回,就是不重复的元素

    需要注意的几点:
        1. distinct是针对RDD内部的元素去重,不会与其他RDD去重
        2. 去重后的RDD可能会改变元素的原有顺序
        3. distinct要求RDD中的元素可哈希且可比较
        4. 对于大数据集,distinct是一个比较昂贵的操作

    在数据量不大的时候,distinct可以非常方便地对RDD进行去重。但对于大数据量的RDD,如果仅需要针对某些字段去重,可以考虑通过map和filter等操作实现,而不是直接使用distinct。
    :param spark:
    :return:
    """
    rdd = spark.parallelize([1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 5, 5, 5])
    # distinct 进行RDD去重操作
    print(rdd.distinct().collect())  # [1, 2, 3, 4, 5]

    rdd2 = spark.parallelize(
        [
            ('a', 1),
            ('a', 1),
            ('a', 3)
        ]
    )
    print(rdd2.distinct().collect())  # [('a', 3), ('a', 1)]


def rdd_union(spark):
    """
    对两个RDD进行并集操作的函数。它可以将两个RDD中的元素合并后返回一个新的RDD

    需要注意的几点:
        1. union操作是lazy的,即并集的计算逻辑并不会立刻执行
        2. union后新的RDD的partition数量为两个原RDD的partition数量之和
        3. 原两个RDD的元素类型不必须相同
        4. 同一个RDD调用union会创建RDD的shallow copy
        5. 可以用union实现多个RDD的连接
    union的常见使用场景包括:
        1. 需要合并两个分别进行计算得到的RDD
        2. 需要合并 Streaming 中分批计算的多个 RDD
        3. 需要将 DataSet 转为 RDD 后和现有 RDD 合并
    union是一个非常常用的RDD转换操作,可以将多个RDD合并。但需要注意partition数量增加可能带来的性能影响。
    :param spark:
    :return:
    """
    rdd1 = spark.parallelize([1, 2, 3, 3, 4, 4])
    rdd2 = spark.parallelize(['a', 'b', 'c', 'd'])

    result = rdd1.union(rdd2)

    print(result.collect())
    # [1, 2, 3, 3, 4, 4, 'a', 'b', 'c', 'd']


def rdd_join(spark):
    """
    union(otherDataset):返回一个包含输入RDD和otherDataset中所有元素的新RDD
    对两个RDD进行join连接的操作。它类似于关系数据库中的join,可以根据两个RDD中的key进行连接
    在join时,可以指定连接类型:
        1. inner join:只保留key存在于两个RDD的元素
        2. outer join:保留两个RDD中所有的key
        3. left outer join:保留rdd1中所有的key
        4. right outer join:保留rdd2中所有的key
    join是一个比较重的操作,因为会引起shuffle来实现连接。在大数据量时,需要考虑其性能影响。
    :param spark:
    :return:
    """
    rdd1 = spark.parallelize(
        [
            (1001, '张三'),
            (1002, '李四'),
            (1003, '王武'),
            (1004, '赵四')
        ]
    )

    rdd2 = spark.parallelize(
        [
            (1001, '研发部'),
            (1002, '人事部')
        ]
    )
    # 通过join来进行rdd之间的关联
    # 对 join 算子来说，关联条件按照二元组的key进行关联
    print(rdd1.join(rdd2).collect())


def rdd_intersection(spark):
    """
    intersection(otherDataset):用于求两个RDD交集的操作。它返回一个新的RDD,其中包含了输入RDD中共有的元素。

    需要注意的几点:
        1. 计算交集会触发shuffle操作,性能上较重
        2. 两个RDD的元素类型必须可比较
        3. 交集操作不会去重,结果RDD可能包含重复元素
        4. 可以在intersection后面加distinct去重
        5. 也可以用sortBy对两个RDD提前进行排序,然后计算交集

    intersection适用于求两个数据集的共同部分。但是对于大数据集,或需要多次计算交集时,建议可以考虑使用join,在连接键上执行交集,而不是全量计算。
    另外,也可以分别对两个RDD进行filter来实现交集,性能上更优。

    所以intersection是一个直接但消耗较大的操作,在大数据量下或需要多次计算时,可以考虑其他更优的实现方式。
    :param spark:
    :return:
    """
    rdd1 = spark.parallelize(
        [
            ('a', 1),
            ('b', 1)
        ]
    )
    rdd2 = spark.parallelize(
        [
            ('a', 1),
            ('c', 1)
        ]
    )
    union_rdd = rdd1.intersection(rdd2)
    print(union_rdd.collect())


def rdd_glom(spark):
    """
    用于将每个分区中的元素转换为数组的操作。它可以将RDD内部的所有数据聚合成几个大的数据块.

    glom返回一个新的RDD,其中每个分区的数据都被转换为一个数组。

    glom操作可以把RDD内部数据聚合成少量大块,常见的使用场景包括:
        1. 与coalesce结合可以重分区和缩减分区数
        2. 计算每个分区内数据的统计量
        3. 聚合小文件使之达到输入格式的最小规模
        4. 将处理逻辑应用于整个分区的数据块

    但是glom也有可能造成数据分布不均,在使用时需要注意分区数和数据规模，可能带来的数据倾斜。
    :param spark:
    :return:
    """
    rdd = spark.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9], 2)

    print(rdd.glom().flatMap(lambda x: x).collect())


def rdd_sortByKey(spark):
    """
    sortByKey根据key对元素进行排序的操作。它会返回一个按照key排序的新的RDD。

    需要注意的几点:
        1. sortByKey是一个wide dependency,会将所有分区的数据打乱重组,是一个shuffle操作
        2. 要求RDD的键类型可排序,即实现了Ordered接口
        3. 可以通过numPartitions参数控制重新分区的数目
        4. sortByKey后面通常需要配合saveAsTextFile等输出结果

    sortByKey可以获得按键有序的数据,适用场景是需要基于键值进行有序处理,比如排序后保存。但由于涉及shuffle,对数据量较大的RDD影响较大。
    此外,sortBy也可以根据任意条件进行排序,但不会引起分区。二者根据实际需要进行选择。
    :param spark:
    :return:
    """
    rdd = spark.parallelize(
        [
            ('c', 3),
            ('f', 1),
            ('B', 11),
            ('c', 3),
            ('e', 1),
            ('C', 5),
            ('e', 1),
            ('N', 9),
            ('a', 1)
        ],
        3
    )
    print(rdd.sortByKey(ascending=True, numPartitions=1, keyfunc=lambda key: str(key).lower()).collect())


def rdd_sortBy(spark):
    """
    sortBy可以根据指定的规则对RDD中的元素进行排序。sortBy不同于sortByKey,它可以按任意字段进行排序,而不是只按键排序。

    sortBy的特点是:
        1. 可以根据任意条件进行排序,而不仅限于键
        2. sortBy是个局部操作,不会引起shuffle
        3. 调用后返回一个新的RDD,不影响原RDD
        4. 相比sortByKey开销较小
    需要注意的问题:
        1. sortBy每个分区内部排序,分区间顺序无保证
        2. sortBy要求RDD元素可比较
        3. 排序字段可能会有重复值

    sortBy适用于轻量按自定义字段排序的场景。但由于分区间顺序无保证,如需全局排序,还需要配合repartition等操作引入shuffle。
    :param spark:
    :return:
    """
    rdd = spark.parallelize(
        [
            ('c', 3),
            ('f', 1),
            ('b', 11),
            ('c', 3),
            ('e', 1),
            ('c', 5),
            ('e', 1),
            ('n', 9),
            ('a', 1)
        ],
        3
    )
    # 注意：如果需要全局有序，排序分区数量设置为1
    print(rdd.sortBy(lambda x: x[1], ascending=True, numPartitions=1).collect())
    # 按key 进行排序
    print(rdd.sortBy(lambda x: x[0], ascending=False, numPartitions=1).collect())


if __name__ == '__main__':
    # 初始化执行环境，构建 SparkContext 对象
    conf = SparkConf()
    conf.setAppName("RddTransformation")
    conf.setMaster("local[*]")

    sc = SparkContext(conf=conf)

    rdd_map(spark=sc)
    rdd_flatMap(spark=sc)
    rdd_reduceByKey(spark=sc)
    rdd_mapValues(spark=sc)
    rdd_groupBy(spark=sc)
    rdd_groupByKey(spark=sc)
    rdd_sortByKey(spark=sc)
    rdd_sortBy(spark=sc)
    rdd_filter(spark=sc)
    rdd_distinct(spark=sc)
    rdd_union(spark=sc)
    rdd_join(spark=sc)
    rdd_intersection(spark=sc)
    rdd_glom(spark=sc)
