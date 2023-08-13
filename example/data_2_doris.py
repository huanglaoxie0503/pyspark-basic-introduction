#!/usr/bin/python
# -*- coding:UTF-8 -*-
import findspark
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, LongType

findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode, when


def kafka_2_doris(spark):
    """
    spark structured streaming实时读取kafka写Doris
    :param spark:
    :return:
    """
    raw_df = spark.readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', 'node01:9092') \
        .option('subscribe', 'paimon_canal_1') \
        .option('failOnDataLoss', False) \
        .option('fetchOffset.numRetries', 3) \
        .option('maxOffsetsPerTrigger', 1) \
        .option('startingOffsets', 'latest') \
        .load()

    schema = StructType([
        StructField("database", StringType()),
        StructField("table", StringType()),
        StructField("data", ArrayType(StructType([
            StructField("id", StringType()),
            StructField("ts", StringType()),
            StructField("vc", StringType())
        ]))),
        StructField("type", StringType()),
        StructField("ts", LongType()),
        StructField("sql", StringType()),
        StructField("pkNames", ArrayType(StringType()))
        # 其他字段
    ])
    # 解析value中的JSON
    df = raw_df.withColumn("json", from_json(col("value").cast("string"), schema=schema))

    df = df.withColumn("database", col("json.database")) \
        .withColumn("table", col("json.table"))\
        .withColumn('data', col("json.data"))\
        .withColumn('type', col("json.type"))\
        .withColumn('pkName', col("json.pkNames"))

    df = df.filter(col("table") == "ws3")

    df = df.select(
        col("data")[0].getItem("id").alias("id"),
        col("data")[0].getItem("ts").alias("ts"),
        col("data")[0].getItem("vc").alias("vc")
    )

    # query = df.select("database", "table", "data", "type", "pkName").writeStream.outputMode("append").format(
    # "console").start()
    # query = df.select("id", "ts", "vc").writeStream.outputMode("append").format(
    #     "console").start()
    #
    # query.awaitTermination()

    df.writeStream \
        .option("checkpointLocation", "hdfs://Oscar-MacPro:8020/spark-checkpoints") \
        .outputMode('append') \
        .format("doris") \
        .option("doris.table.identifier", "ods.ws3") \
        .option("doris.fenodes", "119.91.147.68:8031") \
        .option("user", "root") \
        .option("password", "") \
        .start() \
        .awaitTermination()


if __name__ == '__main__':
    spark_session = SparkSession \
        .builder \
        .appName("StructuredKafkaWordCount") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
        .getOrCreate()

    kafka_2_doris(spark=spark_session)
