#!/usr/bin/python
# -*- coding:UTF-8 -*-
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, StringType


def rdd_to_dataframe(spark, session):
    """
    将RDD转换为DataFrame方式：RDD转换为DataFrame
    :param spark:
    :param session:
    :return:
    """
    # 基于RDD转换为DataFrame
    file_name = '/Users/oscar/projects/big_data/pyspark-basic-introduction/data/people.txt'
    rdd = spark.textFile(file_name).map(lambda x: x.split(',')).map(lambda x: (x[0], int(x[1])))
    # 构建DataFrame对象
    df = session.createDataFrame(rdd, schema=['name', 'age'])

    # 打印DataFrame的表结构
    df.printSchema()
    # 打印df中的数据
    df.show()

    # 将DataFrame对象转换为临时视图，可供SQL语句查询
    df.createOrReplaceTempView('people')
    session.sql('select * from people where age<30;').show()


def structType_to_dataframe(spark, session):
    """
    将RDD转换为DataFrame方式：StructType对象来定义Dataframe的表结构
    :param spark:
    :param session:
    :return:
    """
    file_name = '/Users/oscar/projects/big_data/pyspark-basic-introduction/data/people.txt'
    rdd = spark.textFile(file_name).map(lambda x: x.split(',')).map(lambda x: (x[0], int(x[1])))

    # StructType 类
    schema = StructType().add('name', StringType(), nullable=False). \
        add('age', IntegerType(), nullable=True)

    df = session.createDataFrame(rdd, schema=schema)
    df.printSchema()
    df.show()


def toDF_to_dataframe(spark):
    """
    使用 RDD 的 toDF() 方法转换 RDD
    :param spark:
    :return:
    """
    file_name = '/Users/oscar/projects/big_data/pyspark-basic-introduction/data/people.txt'
    rdd = spark.textFile(file_name).map(lambda x: x.split(',')).map(lambda x: (x[0], int(x[1])))

    # toDF 方式构建DataFrame
    df1 = rdd.toDF(['name', 'age'])
    df1.printSchema()
    df1.show()

    # toDF 方式2 通过StructType来构建
    schema = StructType().add('name', StringType(), nullable=True).add('age', IntegerType(), nullable=False)
    df2 = rdd.toDF(schema=schema)
    df2.printSchema()
    df2.show()


def pandaDF_to_dataframe(session):
    # 基于 pandas 的 DataFrame 对象转换为Spark SQL 的 DataFrame对象
    pdf = pd.DataFrame(
        {
            'id': [1, 2, 3],
            'name': ['Oscar', 'Tom', 'Jim'],
            'age': [11, 12, 13]
        }
    )
    df = session.createDataFrame(pdf)
    df.printSchema()
    df.show()


def read_text_to_dataframe(session):
    """
    通过Spark SQL 的统一API进行数据读取构建DataFrame：text
    :param session:
    :return:
    """
    file_name = '/Users/oscar/projects/big_data/pyspark-basic-introduction/data/people.txt'
    # 构建 StructType，text 数据源，读取数据的特点是将一整行只做为一个列，默认列名是value，类型是string
    schema = StructType().add('data', StringType(), nullable=True)
    df = session.read.format('text').schema(schema=schema).load(file_name)
    df.printSchema()
    df.show()


def read_json_to_dataframe(session):
    """
    通过Spark SQL 的统一API进行数据读取构建DataFrame：json
    :param session:
    :return:
    """
    file_name = '/Users/oscar/projects/big_data/pyspark-basic-introduction/data/people.json'
    # json 数据源自带 schema 信息
    df = session.read.format('json').load(file_name)
    df.printSchema()
    df.show()


def read_csv_to_dataframe(session):
    """
    通过Spark SQL 的统一API进行数据读取构建DataFrame：csv
    :param session:
    :return:
    """
    file_name = '/Users/oscar/projects/big_data/pyspark-basic-introduction/data/people.csv'
    df = session.read.format('csv').option('sep', ';').option('header', True).option('encoding', 'utf-8').schema('name string, age int, job string').load(file_name)
    df.printSchema()
    df.show()


def read_parquet_to_dataframe(session):
    """
    通过Spark SQL 的统一API进行数据读取构建DataFrame：parquet
    :param session:
    :return:
    """
    file_name = '/Users/oscar/projects/big_data/pyspark-basic-introduction/data/users.parquet'
    df = session.read.format('parquet').load(file_name)
    df.printSchema()
    df.show()


if __name__ == '__main__':
    spark_session = SparkSession.builder.appName("DataFrameCreate").master('local[*]').getOrCreate()
    sc = spark_session.sparkContext

    rdd_to_dataframe(spark=sc, session=spark_session)

    structType_to_dataframe(spark=sc, session=spark_session)

    toDF_to_dataframe(spark=sc)

    pandaDF_to_dataframe(session=spark_session)

    read_text_to_dataframe(session=spark_session)

    read_json_to_dataframe(session=spark_session)

    read_csv_to_dataframe(session=spark_session)

    read_parquet_to_dataframe(session=spark_session)
