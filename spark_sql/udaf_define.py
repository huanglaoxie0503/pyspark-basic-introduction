#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark_session = SparkSession.builder.appName("UDAF").master('local[*]').getOrCreate()
    sc = spark_session.sparkContext

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9], 3)
    df = rdd.map(lambda x: [x]).toDF(['num'])

    # PySpark 中不能实现UDAF函数，只能使用折中的方案
    # 使用RDD的mapPartitions 算子完成聚合操作，如果使用mapPartitions API 完成 UDAF聚合，必须使用单分区
    single_partition_rdd = df.rdd.repartition(1)
    print(single_partition_rdd.collect())

    def process(rows):
        count = 0
        for row in rows:
            count += row['num']
        # 一定要嵌套list，因为 mapPartitions 方法要求返回值是 list 对象
        return [count]


    print(single_partition_rdd.mapPartitions(process).collect())
