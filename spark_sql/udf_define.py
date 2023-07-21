#!/usr/bin/python
# -*- coding:UTF-8 -*-
import string

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, ArrayType, StringType, StructType


def udf_create(spark, session):
    """
    UDF 函数创建方式
    :param spark:
    :param session:
    :return:
    """
    rdd = spark.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9]).map(lambda x: [x])
    df = rdd.toDF(['num'])

    # TODO 方式一、SparkSession.udf.register() DSL和SQL都可以使用
    def num_ride_10(num):
        return num * 10

    # 参数一：注册UDF的名称，这个UDF的名称仅可以用于SQL
    # 参数二：UDF的处理逻辑，是一个单独的方法
    # 参数三：UDF的返回值类型，特别注意，UDF注册的时候，必须声明返回值类型，并且UDF的真实返回值必须和声明的一致
    # 当前这种方式定义的UDF，可以通过参数一的参数名称用于SQL风格，返回值对象用于DSL风格
    udf2 = session.udf.register('udf1', num_ride_10, IntegerType())

    # SQL 风格
    # selectExpr  以 select 的表达式执行
    df.selectExpr("udf1(num)").show()

    # DSL 风格
    # 返回值 UDF 对象如果作为方法使用，传入参数一定是Column对象
    df.select(udf2(df['num'])).show()

    # TODO 方式二、仅能用于DSL
    udf3 = F.udf(num_ride_10, IntegerType())
    df.select(udf3(df['num'])).show()


def udf_array(spark, session):
    """
    注册一个ArrayType类型的返回值UDF
    :param spark:
    :param session:
    :return:
    """
    rdd = spark.parallelize(
        [
            ['hadoop spark flink'],
            ['python java golang'],
            ['doris mysql redis']
        ]
    )
    df = rdd.toDF(['line'])

    # 注册UDF，UDF的执行函数定义
    def split_line(data):
        # 返回一个 array 对象
        return data.split(' ')

    # TODO 方式一、构建UDF
    udf2 = session.udf.register('udf1', split_line, ArrayType(StringType()))
    # DSL 风格
    df.select(udf2(df['line'])).show(truncate=False)
    # SQL 风格
    df.createTempView('lines')
    session.sql('select udf1(line) from lines').show(truncate=False)

    # TODO 方式二、构建UDF
    udf3 = F.udf(split_line, ArrayType(StringType()))
    df.select(udf3(df['line'])).show(truncate=False)


def udf_dict(spark, session):
    """
    注册一个字典(dict)类型的返回值UDF
    :param spark:
    :param session:
    :return:
    """
    rdd = spark.parallelize(
        [
            [10],
            [20],
            [30]
        ]
    )
    df = rdd.toDF(['num'])

    # TODO 方式一、注册UDF，功能是将数字都乘以10
    def get_ascii_letters(num):
        return {'num': num, 'letter_str': string.ascii_letters[num]}
    # UDF 的返回值是字典的话，需要用StructType来接受
    struct_type = StructType().add('num', IntegerType(), nullable=True)\
        .add('letter_str', StringType(), nullable=True)
    #
    udf2 = session.udf.register('udf1', get_ascii_letters, struct_type)
    df.select(udf2(df['num'])).show()
    df.selectExpr('udf1(num)').show()

    #
    udf3 = F.udf(get_ascii_letters, struct_type)
    df.select(udf3(df['num'])).show(truncate=False)


if __name__ == '__main__':
    spark_session = SparkSession.builder.appName("SparkSQLUDF").master('local[*]').getOrCreate()
    sc = spark_session.sparkContext

    udf_create(spark=sc, session=spark_session)

    udf_array(spark=sc, session=spark_session)

    udf_dict(spark=sc, session=spark_session)
