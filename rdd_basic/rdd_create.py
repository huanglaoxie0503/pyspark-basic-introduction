#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyspark import SparkConf, SparkContext


def rdd_create_parallelize(spark):
    # parallelize方法，没有给定分区数，默认人分区数是多少？
    rdd_1 = spark.parallelize(
        [1, 2, 3, 4, 5, 6, 7, 8, 9]
    )

    print("默认分区数：", rdd_1.getNumPartitions())
    # 默认分区数： 8，和本机CPU核数有关系

    rdd_2 = spark.parallelize([1, 2, 3], 3)
    print("rdd_2的分区数：", rdd_2.getNumPartitions())

    # collect()方法是将RDD（分布式对象）中每一个分区的数据都发送到Driver中，形成一个python list 对象
    # collect：分布式转本地集合
    print('rdd_2的内容', rdd_2.collect())


def rdd_create_text_file(spark):
    """
    读取文件创建RDD
    :return:
    """
    file_name = '/Users/oscar/projects/big_data/pyspark-basic-introduction/data/words.txt'
    rdd = spark.textFile(file_name)
    print("rdd的分区数：", rdd.getNumPartitions())
    print('rdd的内容', rdd.collect())

    rdd_2 = spark.textFile(file_name, 3)
    # 最小分区数是参考值，spark有自己的判断
    rdd_3 = spark.textFile(file_name, 100)
    print("rdd_2的分区数：", rdd_2.getNumPartitions())
    print("rdd_3的分区数：", rdd_3.getNumPartitions())
    """
    rdd_2的分区数： 3
    rdd_3的分区数： 69
    """
    # 读取HDFS文件
    hdfs_rdd = spark.textFile('hdfs://Oscar-MacPro:8020/sparkdata/salecourse.log')
    print('rdd 的内容', hdfs_rdd.collect())

    # 适合读取一堆小文件，小文件专用
    small_file_rdd = spark.wholeTextFiles('/Users/oscar/projects/big_data/pyspark-basic-introduction/data')
    print('small_file_rdd 的内容', small_file_rdd.collect())


if __name__ == '__main__':
    # 初始化执行环境，构建 SparkContext 对象
    conf = SparkConf().setAppName("RddCreate").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # rdd_create_parallelize(spark=sc)

    rdd_create_text_file(spark=sc)
