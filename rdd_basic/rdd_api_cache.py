#!/usr/bin/python
# -*- coding:UTF-8 -*-
import time

from pyspark import SparkConf, SparkContext, StorageLevel


def rdd_cache(spark):
    """
    StorageLevel 的类型：
        1. NONE - 不持久化,RDD每次操作都会重新计算
        2. DISK_ONLY - 仅将RDD数据持久化到磁盘
        3. DISK_ONLY_2 - 将RDD数据复制到2个节点的磁盘上
        4. DISK_ONLY_3 - 将RDD数据复制到3个节点的磁盘上
        5. MEMORY_ONLY - 仅将RDD数据持久化到内存中
        6. MEMORY_ONLY_2 - 将RDD数据复制到2个节点的内存中
        7. MEMORY_AND_DISK - 将RDD数据持久化到内存和磁盘上
        8. MEMORY_AND_DISK_2 - 将RDD数据复制到2个节点的内存和磁盘上
        9. OFF_HEAP - 在off-heap上分配内存来持久化RDD数据
        10. MEMORY_AND_DISK_DESER - 将反序列化的RDD数据持久化到内存和磁盘上
    :param spark:
    :return:
    """
    rdd1 = spark.textFile('/Users/oscar/projects/big_data/pyspark-basic-introduction/data/words.txt')
    rdd2 = rdd1.flatMap(lambda x: x.split(' '))
    rdd3 = rdd2.map(lambda x: (x, 1))

    rdd4 = rdd3.reduceByKey(lambda a, b: a + b)
    print(rdd4.collect())

    rdd3.cache()
    rdd3.persist(storageLevel=StorageLevel.MEMORY_ONLY)

    # rdd3 多次使用
    rdd5 = rdd3.groupByKey()
    rdd6 = rdd5.mapValues(lambda x: sum(x))
    print(rdd6.collect())

    # 释放缓存
    rdd3.unpersist()

    time.sleep(100000)


def rdd_checkpoint(spark):
    """

    :param spark:
    :return:
    """
    # 设置 Checkpoint 保存路径
    # 如果是Local 模式可以用本地文件系统，如果是集群，则用HDFS
    spark.setCheckpointDir('hdfs://Oscar-MacPro:8020/ck')

    rdd1 = spark.textFile('/Users/oscar/projects/big_data/pyspark-basic-introduction/data/words.txt')
    rdd2 = rdd1.flatMap(lambda x: x.split(' '))
    rdd3 = rdd2.map(lambda x: (x, 1))
    # 用的时候直接调用 checkpoint 算子即可。
    rdd3.checkpoint()

    # rdd3 多次使用
    rdd5 = rdd3.groupByKey()
    rdd6 = rdd5.mapValues(lambda x: sum(x))
    print(rdd6.collect())


if __name__ == '__main__':
    # 初始化执行环境，构建 SparkContext 对象
    conf = SparkConf()
    conf.setAppName("RddTransformation")
    conf.setMaster("local[*]")

    sc = SparkContext(conf=conf)

    # rdd_cache(spark=sc)

    rdd_checkpoint(spark=sc)

    """
    NONE
    DISK_ONLY
    DISK_ONLY_2
    DISK_ONLY_3
    MEMORY_ONLY
    MEMORY_ONLY_2
    MEMORY_AND_DISK
    MEMORY_AND_DISK_2
    OFF_HEAP
    MEMORY_AND_DISK_DESER
    """
