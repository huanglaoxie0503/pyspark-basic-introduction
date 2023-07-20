#!/usr/bin/python
# -*- coding:UTF-8 -*-
import re

from pyspark import SparkConf, SparkContext


def share_variables_example(spark, acc):
    """

    :param spark:上下文
    :param acc: 累加器
    :return:
    """
    # 1. 读取数据文件
    file_rdd = spark.textFile("/Users/oscar/projects/big_data/pyspark-basic-introduction/data/accumulator_broadcast_data.txt")

    # 特殊字符的list定义
    abnormal_char = [",", ".", "!", "#", "$", "%"]

    # 2. 将特殊字符list 包装成广播变量
    broadcast = spark.broadcast(abnormal_char)

    # 3. 数据处理, 先处理数据的空行, 在Python中有内容就是True None就是False
    lines_rdd = file_rdd.filter(lambda line: line.strip())

    # 4. 去除前后的空格
    data_rdd = lines_rdd.map(lambda line: line.strip())

    # 5. 对数据进行切分, 按照正则表达式切分, 因为空格分隔符某些单词之间是两个或多个空格
    # 正则表达式 \s+ 表示 不确定多少个空格, 最少一个空格
    words_rdd = data_rdd.flatMap(lambda line: re.split(r"\s+", line))

    # 6. 当前words_rdd中有正常单词 也有特殊符号.
    # 现在需要过滤数据, 保留正常单词用于做单词计数, 在过滤 的过程中 对特殊符号做计数
    def filter_func(data):
        """过滤数据, 保留正常单词用于做单词计数, 在过滤 的过程中 对特殊符号做计数"""
        global acc
        # 取出广播变量中存储的特殊符号list
        abnormal_chars = broadcast.value
        if data in abnormal_chars:
            # 表示这个是 特殊字符
            acc += 1
            return False
        else:
            return True

    normal_words_rdd = words_rdd.filter(filter_func)
    # 7. 正常单词的单词计数逻辑
    result_rdd = normal_words_rdd.map(lambda x: (x, 1)). \
        reduceByKey(lambda a, b: a + b)

    print("正常单词计数结果: ", result_rdd.collect())
    print("特殊字符数量: ", acc)


if __name__ == '__main__':
    # 初始化执行环境，构建 SparkContext 对象
    conf = SparkConf()
    conf.setAppName("SharedVariablesExample")
    conf.setMaster("local[*]")

    sc = SparkContext(conf=conf)

    # 对特殊字符出现次数做累加, 累加使用累加器最好
    acc = sc.accumulator(0)

    share_variables_example(spark=sc, acc=acc)
