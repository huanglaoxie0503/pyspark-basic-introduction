#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, IntegerType


def movie_demo(session):
    """
    DateFrame 基础综合使用案例
    :param session:
    :return:
    """
    file_name = '/Users/oscar/projects/big_data/pyspark-basic-introduction/data/u.data'
    # 1、读取数据集
    schema = StructType().add('user_id', StringType(), nullable=True) \
        .add('movie_id', IntegerType(), nullable=True) \
        .add('rank', IntegerType(), nullable=True) \
        .add('ts', StringType(), nullable=True)

    df = session.read.format('csv') \
        .option('sep', '\t') \
        .option('header', False) \
        .option('encoding', 'utf-8') \
        .schema(schema=schema) \
        .load(file_name)
    # TODO 1、用户平均分
    df.groupBy("user_id"). \
        avg("rank"). \
        withColumnRenamed("avg(rank)", "avg_rank"). \
        withColumn("avg_rank", F.round("avg_rank", 2)). \
        orderBy("avg_rank", ascending=False). \
        show()

    # TODO 2: 电影的平均分查询
    df.createTempView("movie")
    session.sql("""
            SELECT movie_id, ROUND(AVG(rank), 2) AS avg_rank FROM movie GROUP BY movie_id ORDER BY avg_rank DESC
        """).show()

    # TODO 3: 查询大于平均分的电影的数量 # Row
    print("大于平均分电影的数量: ", df.where(df['rank'] > df.select(F.avg(df['rank'])).first()['avg(rank)']).count())

    # TODO 4: 查询高分电影中(>3)打分次数最多的用户, 此人打分的平均分
    # 先找出这个人
    user_id = df.where("rank > 3"). \
        groupBy("user_id"). \
        count(). \
        withColumnRenamed("count", "cnt"). \
        orderBy("cnt", ascending=False). \
        limit(1). \
        first()['user_id']
    # 计算这个人的打分平均分
    df.filter(df['user_id'] == user_id). \
        select(F.round(F.avg("rank"), 2)).show()

    # TODO 5: 查询每个用户的平局打分, 最低打分, 最高打分
    df.groupBy("user_id"). \
        agg(
        F.round(F.avg("rank"), 2).alias("avg_rank"),
        F.min("rank").alias("min_rank"),
        F.max("rank").alias("max_rank")
    ).show()

    # TODO 6: 查询评分超过100次的电影, 的平均分 排名 TOP10
    df.groupBy("movie_id"). \
        agg(
        F.count("movie_id").alias("cnt"),
        F.round(F.avg("rank"), 2).alias("avg_rank")
    ).where("cnt > 100"). \
        orderBy("avg_rank", ascending=False). \
        limit(10). \
        show()

    # time.sleep(10000)


if __name__ == '__main__':
    spark_session = SparkSession.builder.appName("SparkSQLMovie").master('local[*]').getOrCreate()
    sc = spark_session.sparkContext

    movie_demo(session=spark_session)

    """
    1. agg: 它是GroupedData对象的API, 作用是 在里面可以写多个聚合
    2. alias: 它是Column对象的API, 可以针对一个列 进行改名
    3. withColumnRenamed: 它是DataFrame的API, 可以对DF中的列进行改名, 一次改一个列, 改多个列 可以链式调用
    4. orderBy: DataFrame的API, 进行排序, 参数1是被排序的列, 参数2是 升序(True) 或 降序 False
    5. first: DataFrame的API, 取出DF的第一行数据, 返回值结果是Row对象.
    # Row对象 就是一个数组, 你可以通过row['列名'] 来取出当前行中, 某一列的具体数值. 返回值不再是DF 或者GroupedData 或者Column而是具体的值(字符串, 数字等)
    """
