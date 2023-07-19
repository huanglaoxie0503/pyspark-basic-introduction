#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyspark import SparkContext, SparkConf


def rdd_broadcast(spark):
    """

    :return:
    """
    stu_info_list = [(1, '张大仙', 11),
                     (2, '王晓晓', 13),
                     (3, '张甜甜', 11),
                     (4, '王大力', 11)]

    # 1. 将本地Python List对象标记为广播变量
    broadcast = spark.broadcast(stu_info_list)

    score_info_rdd = spark.parallelize([
        (1, '语文', 99),
        (2, '数学', 99),
        (3, '英语', 99),
        (4, '编程', 99),
        (1, '语文', 99),
        (2, '编程', 99),
        (3, '语文', 99),
        (4, '英语', 99),
        (1, '语文', 99),
        (3, '英语', 99),
        (2, '编程', 99)
    ])

    def map_func(data):
        id = data[0]
        name = ""
        # 匹配本地list和分布式rdd中的学生ID  匹配成功后 即可获得当前学生的姓名
        # 2. 在使用到本地集合对象的地方, 从广播变量中取出来用即可
        for stu_info in broadcast.value:
            stu_id = stu_info[0]
            if id == stu_id:
                name = stu_info[1]

        return name, data[1], data[2]

    print(score_info_rdd.map(map_func).collect())


def rdd_accumulator(spark):
    """

    :param spark:
    :return:
    """
    rdd = spark.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 2)

    # Spark提供的累加器变量, 参数是初始值
    acc = spark.accumulator(0)

    def map_func(data):
        global acc
        acc += 1
        print(acc)

    rdd2 = rdd.map(map_func)
    rdd2.cache()
    print(rdd2.collect())

    rdd3 = rdd2.map(lambda x: x)
    print(rdd3.collect())
    print(acc)


if __name__ == '__main__':
    # 初始化执行环境，构建 SparkContext 对象
    conf = SparkConf()
    conf.setAppName("SharedVariables")
    conf.setMaster("local[*]")

    sc = SparkContext(conf=conf)

    # rdd_broadcast(spark=sc)
    rdd_accumulator(spark=sc)
