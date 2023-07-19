#!/usr/bin/python
# -*- coding:UTF-8 -*-
import jieba
from operator import add

from pyspark import SparkConf, SparkContext, StorageLevel


def content_jieba(data):
    words = jieba.cut_for_search(data)
    rows = []
    for word in words:
        rows.append(word)
    return rows


def filter_words(data):
    return data not in ['容', '帮', '客']


def append_words(data):
    if data == '传智播': data = '传智播客'
    if data == '院校': data = '院校帮'
    if data == '博学': data = '博学谷'
    return data, 1


def extract_user_and_content(data):
    """
    传图对数据是元组 (1, Flink入门)
    :param data:
    :return:
    """
    user_id = data[0]
    content = data[1]
    # 对 content 进行分词
    words = content_jieba(content)
    result_list = []
    for word in words:
        if filter_words(word):
            result_list.append((user_id + '_' + append_words(word)[0], 1))
    return result_list


def sou_gou_demo(spark):
    # 读取数据文件
    file_name = '/Users/oscar/projects/big_data/pyspark-basic-introduction/data/Sogou.txt'
    file_rdd = spark.textFile(file_name)
    # 对数据进行切分
    split_rdd = file_rdd.map(lambda x: x.split("\t"))
    # 因为需要做多个需求，split_rdd 作为基础rdd 会被多次使用
    split_rdd.persist(storageLevel=StorageLevel.DISK_ONLY)

    # TODO 需求1、用户搜索关键词分析
    # 主要分析热词，将所有的搜索内容取出
    # print(split_rdd.takeSample(True, 3))
    content_rdd = split_rdd.map(lambda x: x[2])

    # 将搜索内容进行分词分析
    word_rdd = content_rdd.flatMap(content_jieba)

    # print(word_rdd.collect())
    filter_rdd = word_rdd.filter(filter_words)
    # print(filter_rdd.collect())
    final_rdd = filter_rdd.map(append_words)
    # print(final_rdd.collect())
    # 对单词进行分组、聚合、排序、求和
    result = final_rdd.reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[1], ascending=False, numPartitions=1).take(5)
    print('需求1结果：', result)

    # TODO 需求2：用户和关键组合分析
    user_content_rdd = split_rdd.map(lambda x: (x[1], x[2]))
    # 对用户对搜索内容进行分词，分词后和用户ID再次组合
    user_word_with_rdd = user_content_rdd.flatMap(extract_user_and_content)
    # 对内容进行分组、聚合、排序、求前5
    result = user_word_with_rdd.reduceByKey(
        lambda a, b: a + b).sortBy(lambda x: x[1], ascending=False, numPartitions=1).take(5)
    print('需求2结果：', result)

    # TODO 热门搜索时间段分析
    # 取出所有时间
    time_rdd = split_rdd.map(lambda x: x[0])
    hour_with_one_rdd = time_rdd.map(lambda x: (x.split(':')[0], 1))
    result_3 = hour_with_one_rdd.reduceByKey(add).sortBy(lambda x: x[1], ascending=False, numPartitions=1).collect()
    print('需求3结果：', result_3)


if __name__ == '__main__':
    # 初始化执行环境，构建 SparkContext 对象
    conf = SparkConf()
    conf.setAppName("RddTransformation")
    conf.setMaster("local[*]")

    sc = SparkContext(conf=conf)

    sou_gou_demo(spark=sc)
