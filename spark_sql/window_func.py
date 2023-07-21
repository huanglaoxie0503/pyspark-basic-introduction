#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType


def window_functions(spark, session):
    rdd = spark.parallelize(
        [
            ('张三1', 'class_1', 99),
            ('张三2', 'class_2', 89),
            ('张三3', 'class_3', 79),
            ('张三4', 'class_4', 69),
            ('张三5', 'class_5', 59),
            ('张三6', 'class_6', 49),
        ]
    )
    schema = StructType().add('name', StringType(), nullable=False)\
        .add('class', StringType(), nullable=False)\
        .add('score', IntegerType(), nullable=False)
    df = rdd.toDF(schema=schema)

    df.createTempView('student')

    # TODO 聚合窗口函数
    session.sql("select *, avg(score) over() as avg_score from student").show()

    # TODO 排序相关窗口函数
    session.sql(
        """
        select *, row_number() over(order by score desc) as row_number_rank,
        dense_rank() over(partition by class order by score desc) as dense_rank,
        rank() over(order by score) as rank
        from student
        """
    ).show()

    # TODO ntile
    session.sql(
        """
        select *, NTILE(6) over(order by score desc) from student
        """
    ).show()


if __name__ == '__main__':
    spark_session = SparkSession.builder.appName("WindowFunctions").master('local[*]').getOrCreate()
    sc = spark_session.sparkContext

    window_functions(spark=sc, session=spark_session)
