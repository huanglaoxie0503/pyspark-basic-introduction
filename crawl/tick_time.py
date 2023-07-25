#!/usr/bin/python
# -*- coding:UTF-8 -*-
import json
import random
import time
import requests

from utils.log import log
from utils.settings import headers, cookies
from db.mysqldb import MySQLDB


def random_pause():
    pause_time = random.randint(0, 5)
    print(f"Pausing for {pause_time} seconds...")
    time.sleep(pause_time)


def spider(symbol, day, conn):
    """
    抓取新浪财经A股分时数据
    :param symbol: 股票代码
    :param day: 交易日
    :param conn: mysql 连接
    :return:
    """
    sql = "insert ignore into stock_tick_time (symbol, name, trade_date, tick_time, price, volume, prev_price) values(%s,%s,%s,%s,%s,%s,%s)"
    insert_batch_rows = []
    params = {
        'symbol': symbol,
        # 'num': '60',
        'page': '1',
        'sort': 'ticktime',
        # 'asc': '0',
        # 'volume': '40000',
        # 'amount': '0',
        # 'type': '0',
        'day': day,
    }
    try:
        rows = requests.get(
            'https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_Bill.GetBillList',
            params=params,
            cookies=cookies,
            headers=headers,
        ).json()
        items = []
        log.info('{0}-total_num:{1}'.format(symbol, len(rows)))
        for row in rows:
            symbol = row.get('symbol')
            name = row.get('name')
            trade_date = day
            tick_time = '{0} {1}'.format(day, row.get('ticktime'))
            price = row.get('price')
            volume = row.get('volume')
            prev_price = row.get('prev_price')
            params = [symbol, name, trade_date, tick_time, price, volume, prev_price]
            items.append(row)
            insert_batch_rows.append(params)

        affect_count = conn.add_batch(sql=sql, rows=insert_batch_rows)
        log.info('提交：{0}条数据.'.format(affect_count))

    except Exception as exp:
        print(exp)

    return insert_batch_rows


def write_json_file(file_name, data):
    json_str = json.dumps(data, ensure_ascii=False)
    with open(file_name, 'w', encoding='utf-8') as f:
        f.write(json_str)


def get_data_to_json(conn):
    sql = "select symbol, name, trade_date, tick_time, price, volume, prev_price from stock_tick_time where trade_date='2023-07-24';"
    result = conn.query(sql=sql, limit=0, to_json=True)
    # print(result)
    file_name = '/Users/oscar/projects/big_data/pyspark-basic-introduction/data/stock_tick_time.json'
    write_json_file(file_name=file_name, data=result)


def run():
    conn = MySQLDB(
        host='node01',
        port=3306,
        user='root',
        passwd='Oscar&0503',
        db='crawl'
    )
    get_data_to_json(conn=conn)
    # 查询股票列表，拼接查询参数：sz000504
    # sql = "select ts_code, symbol, name from stock_tick_time;"
    # stock_list = conn.query(sql=sql, limit=0)
    # i = 0
    # for stock in stock_list:
    #     i = i + 1
    #     info = stock[0].split('.')
    #     symbol = '{0}{1}'.format(info[1].lower(), info[0])
    #     print('{0}抓取完成'.format(stock[0]))
    #     spider(symbol=symbol, day='2023-07-25', conn=conn)
    # if i % 2 == 0:
    #     random_pause()
    # kafka_producer_demo(rows=items)


if __name__ == '__main__':
    run()
