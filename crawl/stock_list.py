#!/usr/bin/python
# -*- coding:UTF-8 -*-
import os
import tushare as ts
from sqlalchemy import create_engine


def get_stock_list():
    """
    查询当前所有正常上市交易的股票列表
    :return:
    """
    token = os.environ["TUSHARE_TOKEN"]
    ts.set_token(token=token)
    pro = ts.pro_api()
    # 拉取数据
    df = pro.stock_basic(**{
        "ts_code": "",
        "name": "",
        "exchange": "",
        "market": "",
        "is_hs": "",
        "list_status": "",
        "limit": "",
        "offset": ""
    }, fields=[
        "ts_code",
        "symbol",
        "name",
        "area",
        "industry",
        "market",
        "list_date"
    ])
    df.to_sql(name='stock_basic', con=engine, if_exists='append', index=False, index_label=False)
    print(df)


if __name__ == '__main__':
    engine = create_engine("mysql+pymysql://root:Oscar&0503@node01:3306/crawl?charset=utf8")
    get_stock_list()



