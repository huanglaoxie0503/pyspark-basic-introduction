#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyspark.sql import SparkSession


if __name__ == '__main__':
    # 构建SparkSession执行环境入口对象
    spark_session = SparkSession.builder.appName('SparkSessionCreate').master('local[*]').getOrCreate()
    # 通过SparkSession对象，获取SparkContext对象
    sc = spark_session.sparkContext

    file_name = '/Users/oscar/projects/big_data/pyspark-basic-introduction/data/stu_score.txt'
    df = spark_session.read.csv(file_name, sep=',', header=False)
    df2 = df.toDF('id', 'name', 'score')
    df2.printSchema()
    df2.show()

    df2.createTempView('score')

    # Spark SQL
    spark_session.sql("select * from score where name='语文' limit 5").show()
    # DSL 风格
    df2.where("name='语文'").limit(5).show()