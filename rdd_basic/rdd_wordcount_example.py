#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyspark import SparkConf, SparkContext


def rdd_wordcount(spark):
    # 读取文件获取数据 构建RDD
    file_name = '/Users/oscar/projects/big_data/pyspark-basic-introduction/data/words.txt'
    file_rdd = spark.textFile(file_name)

    # 通过 flatMap API 取出所有单词
    word_rdd = file_rdd.flatMap(lambda x: x.split(' '))

    # 将单词转换为元组，key是单词，value是1
    word_with_one_rdd = word_rdd.map(lambda word: (word, 1))

    # 用 reduceByKey 对单词进行分组并进行value 的聚合
    result_rdd = word_with_one_rdd.reduceByKey(lambda x,y: x+y)
    # 通过collect()算子，将rdd的数据收集到Driver中，并进行打印
    print(result_rdd.collect())


if __name__ == '__main__':
    # 初始化执行环境，构建 SparkContext 对象
    conf = SparkConf().setAppName("RddWordCount").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd_wordcount(spark=sc)

