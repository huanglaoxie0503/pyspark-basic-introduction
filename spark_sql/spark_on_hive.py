#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyspark.sql import SparkSession


if __name__ == '__main__':
    # 0. 构建执行环境入口对象SparkSession
    spark = SparkSession.builder.\
        appName("Spark On Hive").\
        master("local[*]").\
        config("spark.sql.shuffle.partitions", 2).\
        config("spark.sql.warehouse.dir", "hdfs://node01:8020/user/hive/warehouse").\
        config("hive.metastore.uris", "thrift://node01:9083").\
        enableHiveSupport().\
        getOrCreate()
    sc = spark.sparkContext

    spark.sql("SELECT * FROM student").show()


