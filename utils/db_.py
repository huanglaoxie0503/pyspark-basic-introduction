#!/usr/bin/python
# -*- coding:UTF-8 -*-
import logging

import pymysql


class MySQLInit(object):
    def __init__(self, host, port, user, passwd, db_name):
        self.conn = pymysql.connect(
            host=host,
            user=user,
            passwd=passwd,
            db=db_name,
            port=port,
            charset="utf8",
            use_unicode=True,
        )
        self.cursor = self.conn.cursor()

    def query(self, columns, tb_name):
        sql = "select {0} from {1};".format(columns, tb_name)
        self.cursor.execute(sql)
        rows = self.cursor.fetchall()
        items = []
        for row in rows:
            ts_code = row[0]
            str_split = ts_code.split('.')
            stock_code = str_split[0]
            flag = str_split[1].lower()
            symbol = '{0}{1}'.format(flag, stock_code)
            stock_name = row[2]
            result = (symbol, stock_name)
            items.append(result)
        return items

    def do_insert(self, params):
        try:
            insert_sql = """
            insert into stock_tick_time (symbol, name, trade_date, tick_time, price, volume, prev_price) values(
            %s,%s,%s,%s,%s,%s,%s)
            """
            self.cursor.execute(insert_sql, params)
            self.conn.commit()
            logging.info("----------------{0}：insert success-----------".format(params[1]))
        except pymysql.Error as e:
            print(e)


if __name__ == '__main__':
    init = MySQLInit(
        host='node01',
        port=3306,
        user='root',
        passwd='Oscar&0503',
        db_name='crawl'
    )
    r = init.query(
        'ts_code, symbol, name',
        'stock_basic'
    )
    print(r)
