#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyspark import SparkConf, SparkContext


def stock_tick_time(spark):
    """
    读取股票分时数据，计算每一只股票当天的成交量
    :param spark:
    :return:
    """
    file_name = '/Users/oscar/projects/big_data/data/stock_tick_time.csv'
    file_rdd = spark.textFile(file_name)

    list_rdd = file_rdd.flatMap(lambda line: line.split(', '))
    print(list_rdd.collect())


if __name__ == '__main__':
    # 初始化执行环境，构建 SparkContext 对象
    conf = SparkConf().setAppName("TickTime").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    stock_tick_time(spark=sc)
