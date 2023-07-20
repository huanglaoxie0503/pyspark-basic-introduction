#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def sparkSql_word_count(spark, session):
    """
    Spark SQL 方式实现词频统计
    :param spark:
    :param session:
    :return:
    """
    file_name = '/Users/oscar/projects/big_data/pyspark-basic-introduction/data/words.txt'
    # TODO 1: SQL 风格进行处理
    rdd = spark.textFile(file_name).flatMap(lambda x: x.split(" ")).map(lambda x: [x])

    df = rdd.toDF(["word"])

    # 注册DF为表格
    df.createTempView("words")

    session.sql("SELECT word, COUNT(*) AS cnt FROM words GROUP BY word ORDER BY cnt DESC").show()

    # TODO 2: DSL 风格处理
    df = session.read.format("text").load(file_name)

    # withColumn方法
    # 方法功能: 对已存在的列进行操作, 返回一个新的列, 如果名字和老列相同, 那么替换, 否则作为新列存在
    df2 = df.withColumn("value", F.explode(F.split(df['value'], " ")))
    df2.groupBy("value"). \
        count(). \
        withColumnRenamed("value", "word"). \
        withColumnRenamed("count", "cnt"). \
        orderBy("cnt", ascending=False). \
        show()


if __name__ == '__main__':
    spark_session = SparkSession.builder.appName("SparkSQLWordCount").master('local[*]').getOrCreate()
    sc = spark_session.sparkContext

    sparkSql_word_count(spark=sc, session=spark_session)
