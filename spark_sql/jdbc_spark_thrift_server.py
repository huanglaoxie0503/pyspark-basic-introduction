#!/usr/bin/python
# -*- coding:UTF-8 -*-
from pyhive import hive


if __name__ == '__main__':
    # 获取 Hive（Spark ThriftServer的连接）
    conn = hive.Connection(
        host='node01',
        port=10000,
        username='root'
    )

    # 获取一个游标对象
    cursor = conn.cursor()

    # 执行SQL
    cursor.execute('show databases')
    # 通过 fetchall API 获取返回值
    result = cursor.fetchall()
    print(result)