#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyspark.sql import SparkSession


def clear_data(session):
    file_name = '/Users/oscar/projects/big_data/pyspark-basic-introduction/data/people.csv'
    df = session.read.format('csv').option('sep', ';').option('header', True).load(file_name)

    # 数据去重
    df.dropDuplicates().show()

    df.dropDuplicates(['age', 'job']).show()
    df.dropDuplicates(['name']).show()

    # TODO 缺失值处理：删除
    df.dropna().show()
    # thresh=3:最少满足3个有效列，不满足则删除当前行数据
    df.dropna(thresh=3).show()
    df.dropna(thresh=2, subset=['name', 'age']).show()

    # TODO 缺失值处理：填充
    # 对缺失对列进行填充
    df.fillna('NA').show()
    # 指定列进行填充
    df.fillna('N/A', subset=['job']).show()
    # 设定一个字典，对指定列提供填充规则
    df.fillna(
        {
            'name': '未知姓名',
            'age': 1000,
            'job': 'work'
        }
    ).show()


if __name__ == '__main__':
    spark_session = SparkSession.builder.appName("SparkSQLWordCount").master('local[*]').getOrCreate()
    sc = spark_session.sparkContext

    clear_data(session=spark_session)
