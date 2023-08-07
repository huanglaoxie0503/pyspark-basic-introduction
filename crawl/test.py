#!/usr/bin/python
# -*- coding:UTF-8 -*-
import requests

cookies = {
    'UOR': 'finance.sina.com.cn,vip.stock.finance.sina.com.cn,',
    'SINAGLOBAL': '113.89.234.249_1667201515.160382',
    'SUB': '_2AkMUnzSFf8NxqwFRmP8dy2jlaYh_yQDEieKiw8VeJRMyHRl-yD9kql4JtRB6Px8aagVIiHr7rkogU4kZVU4kpKnxRQOI',
    'SR_SEL': '1_511',
    'SFA_version6.28.0': '2023-07-16%2000%3A24',
    'SFA_version7.0.0_click': '1',
    'U_TRS1': '000000e7.e4205a7fd.64be0898.9ed75486',
    'SFA_version7.0.0': '2023-08-03%2001%3A42',
    'Apache': '112.97.80.253_1690998447.78655',
    'MONEY-FINANCE-SINA-COM-CN-WEB5': '',
    'ULV': '1690998361805:14:2:2:112.97.80.253_1690998447.78655:1690998356911',
    'hqEtagMode': '1',
    'FIN_ALL_VISITED': 'sz301315%2Csh603315%2Csh688152%2Csh688092%2Csz000001%2Csh600050%2Csh601138%2Csh600487',
    'rotatecount': '3',
    'FINA_V_S_2': 'sz301315,sh603315,sh688152,sh600151,sh688092,sz000001,sh600050,sh601138,sh600487',
}

headers = {
    'authority': 'vip.stock.finance.sina.com.cn',
    'accept': '*/*',
    'accept-language': 'zh-CN,zh;q=0.9',
    'cache-control': 'no-cache',
    # 'cookie': 'UOR=finance.sina.com.cn,vip.stock.finance.sina.com.cn,; SINAGLOBAL=113.89.234.249_1667201515.160382; SUB=_2AkMUnzSFf8NxqwFRmP8dy2jlaYh_yQDEieKiw8VeJRMyHRl-yD9kql4JtRB6Px8aagVIiHr7rkogU4kZVU4kpKnxRQOI; SR_SEL=1_511; SFA_version6.28.0=2023-07-16%2000%3A24; SFA_version7.0.0_click=1; U_TRS1=000000e7.e4205a7fd.64be0898.9ed75486; SFA_version7.0.0=2023-08-03%2001%3A42; Apache=112.97.80.253_1690998447.78655; MONEY-FINANCE-SINA-COM-CN-WEB5=; ULV=1690998361805:14:2:2:112.97.80.253_1690998447.78655:1690998356911; hqEtagMode=1; FIN_ALL_VISITED=sz301315%2Csh603315%2Csh688152%2Csh688092%2Csz000001%2Csh600050%2Csh601138%2Csh600487; rotatecount=3; FINA_V_S_2=sz301315,sh603315,sh688152,sh600151,sh688092,sz000001,sh600050,sh601138,sh600487',
    'pragma': 'no-cache',
    'referer': 'https://vip.stock.finance.sina.com.cn/quotes_service/view/vMS_tradedetail.php?symbol=sz301315',
    'sec-ch-ua': '"Not/A)Brand";v="99", "Google Chrome";v="115", "Chromium";v="115"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
}

params = {
    'symbol': 'sz301315',
    'num': '60',
    'page': '1',
    'sort': 'ticktime',
    'asc': '0',
    'volume': '40000',
    'amount': '0',
    'type': '0',
    'day': '',
}

response = requests.get(
    'https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_Bill.GetBillList',
    params=params,
    cookies=cookies,
    headers=headers,
)

print(response.json())