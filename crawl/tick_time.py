#!/usr/bin/python
# -*- coding:UTF-8 -*-
import requests

from settings import headers, cookies
from utils.db import MySQLInit
from utils.kafka_producer import kafka_producer_demo


def spider(symbol, day, conn):
    """
    抓取新浪财经A股分时数据
    :param symbol: 股票代码
    :param day: 交易日
    :param conn: mysql 连接
    :return:
    """
    params = {
        'symbol': symbol,
        # 'num': '60',
        'page': '1',
        'sort': 'ticktime',
        'asc': '0',
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
        for row in rows:
            symbol = row.get('symbol')
            name = row.get('name')
            trade_date = day
            tick_time = '{0} {1}'.format(day, row.get('ticktime'))
            price = row.get('price')
            volume = row.get('volume')
            prev_price = row.get('prev_price')
            params = (symbol, name, trade_date, tick_time, price, volume, prev_price)
            items.append(row)
            conn.do_insert(params=params)
        return items
    except Exception as exp:
        print(exp)


def run():
    mysql_init = MySQLInit(
        host='node01',
        port=3306,
        user='root',
        passwd='Oscar&0503',
        db_name='crawl'
    )
    stock_list = mysql_init.query(
        'ts_code, symbol, name',
        'stock_basic'
    )
    for stock in stock_list:
        symbol = stock[0]
        items = spider(symbol=symbol, day='2023-07-21', conn=mysql_init)
        # kafka_producer_demo(rows=items)


if __name__ == '__main__':
    run()
