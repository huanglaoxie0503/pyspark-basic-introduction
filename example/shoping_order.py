#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel
from pyspark.sql.types import StringType


def save_to_mysql(df, tb_name):
    """
    写出到MySQL
    :param df: DataFrame
    :param tb_name: Table Name
    :return:
    """
    df.write.mode('overwrite').format('jdbc') \
        .option('url', 'jdbc:mysql://node01:3306/crawl?useSSL=false&useUnicode=true&characterEncoding=utf8') \
        .option('dbtable', tb_name) \
        .option('user', 'root') \
        .option('password', 'Oscar&0503') \
        .option('encoding', 'utf-8').save()


def save_to_hive(df, db_name, tb_name):
    """
    写出到Hive表
    :param df: DataFrame
    :param db_name: DB Name
    :param tb_name: Table Name
    :return:
    """
    # 写出到Hive表，saveAsTable 可以写出表，要求提前配置好Spark On Hive，配置好后会将数据写到Hive数仓
    df.write.mode('overwrite').saveAsTable('{0}.{1}'.format(db_name, tb_name), 'parquet')


def shop_order(session):
    # 读取数据
    file_name = '/Users/oscar/projects/big_data/pyspark-basic-introduction/data/mini.json'
    # 省份信息缺失值过滤，同时省份信息中会有“null”字符串
    # 订单金额，数据集中有的订单单笔超过10000，这事测试数据
    # 列值裁剪
    df = session.read.format('json') \
        .load(file_name) \
        .dropna(thresh=1, subset=['storeProvince']) \
        .filter("storeProvince != 'null'") \
        .filter('receivable < 10000') \
        .select('storeProvince', 'storeID', 'receivable', 'dateTS', 'payType')

    # TODO 1、各省的销售额统计
    province_sale_df = df.groupBy('storeProvince').sum('receivable') \
        .withColumnRenamed('sum(receivable)', 'money') \
        .withColumn('money', F.round('money', 2)) \
        .orderBy('money', ascending=False)

    # province_sale_df.show()
    # 写出到MySQL
    save_to_mysql(df=province_sale_df, tb_name='province_sale')
    # 写出到Hive表
    save_to_hive(df=province_sale_df, db_name='default', tb_name='province_sale')

    # TODO 2、Top3 销售省份中，有多少店铺达到日销售额1000+
    # 2.1、先找到Top3的销售省份
    top3_province_df = province_sale_df.limit(3).select('storeProvince').withColumnRenamed('storeProvince',
                                                                                           'top3_storeProvince')
    # top3_province_df.show()
    # 2.2、和 原始的DF进行内关联,数据关联后就是Top3省份的数据
    top3_province_joined_df = df.join(top3_province_df,
                                      on=df['storeProvince'] == top3_province_df['top3_storeProvince'])

    # 缓存
    top3_province_joined_df.persist(StorageLevel.MEMORY_AND_DISK)

    province_hot_store_count_df = top3_province_joined_df.groupBy(
        'storeProvince', 'storeID', F.from_unixtime(df['dateTS'].substr(0, 10), 'yyyy-MM-dd').alias('day')
    ).sum('receivable').withColumnRenamed('sum(receivable)', 'money') \
        .filter('money > 1000') \
        .dropDuplicates(subset=['storeID']) \
        .groupBy('storeProvince').count()

    # 写出到MySQL
    save_to_mysql(df=province_hot_store_count_df, tb_name='province_hot_store_count')
    # 写出到Hive表
    save_to_hive(df=province_hot_store_count_df, db_name='default', tb_name='province_hot_store_count')

    # TODO 3、Top3 省份中，各省的平均单单价
    top3_province_order_avg_df = top3_province_joined_df.groupBy('storeProvince').avg('receivable') \
        .withColumnRenamed('avg(receivable)', 'money') \
        .withColumn('money', F.round('money', 2)) \
        .orderBy('money', ascending=False)

    # top3_province_order_avg_df.show()
    # 写出到MySQL
    save_to_mysql(df=top3_province_order_avg_df, tb_name='province_order_avg')
    # 写出到Hive表
    save_to_hive(df=top3_province_order_avg_df, db_name='default', tb_name='province_order_avg')

    # TODO 4、Top3 省份中，各省的支付类型比率
    top3_province_joined_df.createTempView('province_pay')

    def udf_func(percent):
        return str(round(percent * 100, 2)) + '%'

    # 注册UDF
    percent_udf = F.udf(udf_func, StringType())

    pay_type_df = session.sql(
        """
        select storeProvince, payType, (count(payType) / total) as percent
        from
        (select storeProvince, payType, count(*) over(partition by storeProvince) as total from province_pay) as sub
        group by storeProvince, payType, total
        """
    ).withColumn('percent', percent_udf('percent'))

    pay_type_df.show()
    # 写出到MySQL
    save_to_mysql(df=pay_type_df, tb_name='pay_type_percent')
    # 写出到Hive表
    save_to_hive(df=pay_type_df, db_name='default', tb_name='pay_type_percent')

    # 释放缓存
    top3_province_joined_df.unpersist()


if __name__ == '__main__':
    # 构建SparkSession执行环境入口对象
    spark_session = SparkSession.builder \
        .appName('ShopOrder') \
        .master('local[*]') \
        .config('spark.sql.shuffle.partitions', 2) \
        .config('spark.sql.warehouse.dir', 'hdfs://node01:8020/user/hive/warehouse') \
        .config('hive.metastore.uris', 'thrift://node01:9083') \
        .enableHiveSupport() \
        .getOrCreate()
    # 通过SparkSession对象，获取SparkContext对象
    sc = spark_session.sparkContext

    """
    1、各省的销售额统计
    2、Top3 销售省份中，有多少店铺达到日销售额1000+
    3、Top3 省份中，各省的平均单单价
    4、Top3 省份中，各省的支付类型比率
    
    receivable：订单金额
    storeProvince：店铺省份
    dateTS：订单销售日期
    payType：支付类型
    storeID：店铺ID
    
    2个操作：
    1、写出结果到MySQL
    2、写出结果到Hive
    """

    shop_order(session=spark_session)
