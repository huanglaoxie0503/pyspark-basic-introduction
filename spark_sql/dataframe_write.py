#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, IntegerType, StringType


def dataframe_write_file(session):
    """
    Spark SQL 统一标准API写出数据到文件
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
    # write  text data，只能写出一列数据，需要将df 转换为单列 df
    save_text_file_name = '/Users/oscar/projects/big_data/pyspark-basic-introduction/data/output/text'
    df.select(F.concat_ws('---', 'user_id', 'movie_id', 'rank', 'ts')).write.mode('overwrite').format('text').save(
        save_text_file_name)

    # write csv
    save_csv_file_name = '/Users/oscar/projects/big_data/pyspark-basic-introduction/data/output/csv'
    df.write.mode('overwrite').format('csv').option('sep', ';').option('header', True).save(save_csv_file_name)

    # write json
    save_json_file_name = '/Users/oscar/projects/big_data/pyspark-basic-introduction/data/output/json'
    df.write.mode('overwrite').format('json').save(save_json_file_name)

    # write parquet
    save_parquet_file_name = '/Users/oscar/projects/big_data/pyspark-basic-introduction/data/output/parquet'
    df.write.mode('overwrite').format('parquet').save(save_parquet_file_name)


def dataframe_write_jdbc(session):
    """
    Spark SQL 统一标准API写出数据到MySQL数据库
        jdbc 写出会自动创建表，因为 DataFrame 中有表结构信息，StructType 记录的各个字段名、类型、是否允许为空
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
    # df 写出道 mysql 数据库中
    df.write.mode('overwrite').format('jdbc')\
        .option('url', 'jdbc:mysql://node01:3306/crawl?useSSL=false&useUnicode=true')\
        .option('user', 'root')\
        .option('password', 'Oscar&0503')\
        .option('dbtable', 'movie').save()

    # 读取数据
    df2 = session.read.format('jdbc')\
        .option('url', 'jdbc:mysql://node01:3306/crawl?useSSL=false&useUnicode=true')\
        .option('user', 'root')\
        .option('password', 'Oscar&0503')\
        .option('dbtable', 'movie').load()

    df2.printSchema()
    df2.show()


if __name__ == '__main__':
    spark_session = SparkSession.builder.appName("SparkSQLWordCount").master('local[*]').getOrCreate()
    sc = spark_session.sparkContext

    # dataframe_write_file(session=spark_session)

    dataframe_write_jdbc(session=spark_session)
