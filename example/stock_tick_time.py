#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyspark import SparkConf, StorageLevel
from pyspark.sql import SparkSession


def trade_tick_time(session):
    file_name = '/Users/oscar/projects/big_data/pyspark-basic-introduction/data/stock_tick_time.json'
    df = session.read.format('json') \
        .load(file_name) \
        .select('symbol', 'name', 'trade_date', 'tick_time', 'price', 'volume', 'prev_price')

    # 缓存到内存
    df.persist(storageLevel=StorageLevel.MEMORY_ONLY)

    # TODO 统计每日每只股票的成交量之和
    stock_volume_df = df.groupBy('symbol', 'trade_date') \
        .sum('volume') \
        .withColumnRenamed('sum(volume)', 'volume_count').orderBy('volume_count', ascending=False)
    stock_volume_df.show()

    # 释放缓存
    df.unpersist()


if __name__ == '__main__':
    # 原文件有150M，本地开发模式需设置：spark.driver.memory、spark.executor.memory 否则会报java.lang.OutOfMemoryError: Java heap space
    conf = SparkConf()
    conf.set("spark.driver.memory", "2g")
    conf.set("spark.executor.memory", "1g")

    spark_session = SparkSession.builder.config(conf=conf).appName("StockTickTime").master('local[*]').getOrCreate()

    trade_tick_time(session=spark_session)
