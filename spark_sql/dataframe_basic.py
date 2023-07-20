#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyspark.sql import SparkSession


def dataframe_dsl(session):
    """
    DataFrame DSL
    :param session:
    :return:
    """
    file_name = '/Users/oscar/projects/big_data/pyspark-basic-introduction/data/stu_score.txt'
    df = session.read.format('csv').schema('id int, subject string, score int').load(file_name)

    # Column 对象获取
    id_column = df['id']
    subject_column = df['subject']
    score_column = df['score']
    # DSL 风格
    df.select('id', 'subject', 'score').show()
    df.select(['id', 'subject', 'score']).show()
    df.select(id_column, subject_column, score_column).show()

    # filter API
    df.filter('score < 99').show()
    df.filter(df['score'] < 99).show()

    # where API
    df.where('score < 99').show()
    df.where(df['score'] < 99).show()

    # group by API
    df.groupBy('subject').count().show()
    df.groupBy(df['subject']).count().show()


def dataframe_sql(session):
    """
    DataFrame SQL
    :param session:
    :return:
    """
    file_name = '/Users/oscar/projects/big_data/pyspark-basic-introduction/data/stu_score.txt'
    df = session.read.format('csv').schema('id int, subject string, score int').load(file_name)

    df.createTempView('score')  # 注册一个临时视图（表）
    df.createOrReplaceTempView('score_2')  # 注册一个临时视图（表），如果存在则替换
    df.createGlobalTempView('score_3')  # 注册一个全局视图（表）
    df.createOrReplaceGlobalTempView('score_4')  # 注册一个全局视图（表），如果存在则替换

    session.sql("select subject, count(*) as cnt from score group by subject").show()
    session.sql("select subject, count(*) as cnt from score_2 group by subject").show()
    # 全局
    session.sql("select subject, count(*) as cnt from global_temp.score_3 group by subject").show()
    session.sql("select subject, count(*) as cnt from global_temp.score_4 group by subject").show()


if __name__ == '__main__':
    spark_session = SparkSession.builder.appName("DataFrameBasic").master('local[*]').getOrCreate()
    sc = spark_session.sparkContext

    dataframe_dsl(session=spark_session)

    dataframe_sql(session=spark_session)
