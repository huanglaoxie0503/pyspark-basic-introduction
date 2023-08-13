#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyspark.sql import functions as F
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


def iceberg_simple(spark_session):
    # TODO 2、读取表
    spark_session.read.format('iceberg').load('hdfs://node01:8020/warehouse/spark-iceberg/default/a').show()
    # 仅支持Spark3.0以上
    spark_session.table("iceberg_hadoop.default.a").show()

    # 时间旅行：指定时间查询
    spark_session \
        .read \
        .option("as-of-timestamp", "499162860000") \
        .format("iceberg") \
        .load("hdfs://node01:8020/warehouse/spark-iceberg/default/a") \
        .show()
    # 时间旅行：指定快照id查询
    spark_session \
        .read \
        .option("snapshot-id", 4315130219338750441) \
        .format("iceberg") \
        .load("hdfs://node01:8020/warehouse/spark-iceberg/default/a") \
        .show()
    # 增量查询
    spark_session \
        .read \
        .format("iceberg") \
        .option("start-snapshot-id", 10963874102873) \
        .option("end-snapshot-id", 63874143573109) \
        .load("hdfs://node01:8020/warehouse/spark-iceberg/default/a") \
        .show()
    # TODO 检查表
    # 查询元数据
    spark_session.read.format("iceberg").load("iceberg_hadoop.default.a.files").show()

    spark_session.read.format("iceberg").load("hdfs://node01:8020/warehouse/spark-iceberg/default/a#files").show()
    # 元数据表时间旅行查询
    spark_session \
        .read \
        .format("iceberg") \
        .option("snapshot-id", 4315130219338750441) \
        .load("iceberg_hadoop.default.a.files") \
        .show()

    # TODO 写入表
    # 1、创建样例类，准备DF
    data = [(1, "Oscar", "深圳"), (2, "Tom", "北京"), (3, "Jim", "上海")]

    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("address", StringType(), True)
    ])

    df = spark_session.createDataFrame(data, schema)

    # 插入数据并建表
    df.writeTo(table="iceberg_hadoop.default.student").create()
    # 设置分区属性
    df.write.format("iceberg") \
        .option("iceberg.warehouse", "iceberg_hadoop.default") \
        .option("write.format.default", "orc") \
        .partitionBy("id") \
        .saveAsTable("student_info")

    # append追加
    df.writeTo("iceberg_hadoop.default.student").append()
    # 动态分区覆盖
    df.writeTo("iceberg_hadoop.default.student").overwritePartitions()
    # 静态分区覆盖
    df.write.format("iceberg") \
        .option("iceberg.warehouse", "iceberg_hadoop.default") \
        .mode("overwrite") \
        .saveAsTable("student")
    # 静态分区覆盖
    df.filter(F.col("id") == "1") \
        .write.format("iceberg") \
        .mode("append") \
        .saveAsTable("student")

    # 插入分区表且分区内排序
    df.sortWithinPartitions(F.col("id")) \
        .write.format("iceberg") \
        .option("iceberg.warehouse", "iceberg_hadoop.default") \
        .mode("append") \
        .saveAsTable("student")


if __name__ == '__main__':
    # TODO 1、创建 Catalog
    conf = SparkConf()  # 指定 hive catalog，catalog 名称为：iceberg_hive
    conf.set('spark.sql.catalog.iceberg_hive', 'org.apache.iceberg.spark.SparkCatalog')
    conf.set('spark.sql.catalog.iceberg_hive.type', 'hive')
    conf.set('spark.sql.catalog.iceberg_hive.uri', 'thrift://node01:9083')
    conf.set('iceberg.engine.hive.enabled', 'true')
    # 指定hadoop catalog ，catalog 名称为：iceberg_hadoop
    conf.set('spark.sql.catalog.iceberg_hadoop', 'org.apache.iceberg.spark.SparkCatalog')
    conf.set('spark.sql.catalog.iceberg_hadoop.type', 'hadoop')
    conf.set('spark.sql.catalog.iceberg_hadoop.warehouse', 'hdfs://node01:8020/warehouse/spark-iceberg')
    # 添加 jar 包
    conf.set('spark.jars', '/Users/oscar/software/jars/iceberg-spark-runtime-3.4_2.12-1.3.1.jar')

    conf.set("spark.hadoop.fs.defaultFS", "hdfs://node01:8020")

    spark = SparkSession \
        .builder \
        .appName("SparkIceberg") \
        .master('local[*]').config(conf=conf) \
        .getOrCreate()

    iceberg_simple(spark_session=spark)
