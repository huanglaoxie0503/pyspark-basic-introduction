#!/usr/bin/python
# -*- coding:UTF-8 -*-

import os

# redis 表名
# 任务表模版
TAB_REQUESTS = "{redis_key}:z_requests"
# 任务失败模板
TAB_FAILED_REQUESTS = "{redis_key}:z_failed_requests"
# 数据保存失败模板
TAB_FAILED_ITEMS = "{redis_key}:s_failed_items"
# 爬虫状态表模版
TAB_SPIDER_STATUS = "{redis_key}:h_spider_status"
# 用户池
TAB_USER_POOL = "{redis_key}:h_{user_type}_pool"

# MYSQL
MYSQL_IP = os.getenv("MYSQL_IP")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))
MYSQL_DB = os.getenv("MYSQL_DB")
MYSQL_USER_NAME = os.getenv("MYSQL_USER_NAME")
MYSQL_USER_PASS = os.getenv("MYSQL_USER_PASS")

# MONGODB
MONGO_IP = os.getenv("MONGO_IP", "localhost")
MONGO_PORT = int(os.getenv("MONGO_PORT", 27017))
MONGO_DB = os.getenv("MONGO_DB")
MONGO_USER_NAME = os.getenv("MONGO_USER_NAME")
MONGO_USER_PASS = os.getenv("MONGO_USER_PASS")

# 内存任务队列最大缓存的任务数，默认不限制；仅对AirSpider有效。
TASK_MAX_CACHED_SIZE = 0

# REDIS
# ip:port 多个可写为列表或者逗号隔开 如 ip1:port1,ip2:port2 或 ["ip1:port1", "ip2:port2"]
REDIS_DB_IP_PORTS = os.getenv("REDISDB_IP_PORTS")
REDIS_DB_USER_PASS = os.getenv("REDISDB_USER_PASS")
REDIS_DB_DB = int(os.getenv("REDISDB_DB", 0))
# 连接redis时携带的其他参数，如ssl=True
REDIS_DB_KWARGS = dict()
# 适用于redis哨兵模式
REDIS_DB_SERVICE_NAME = os.getenv("REDISDB_SERVICE_NAME")

# 日志
LOG_NAME = os.path.basename(os.getcwd())
LOG_PATH = "log/%s.log" % LOG_NAME  # log存储路径
LOG_LEVEL = os.getenv("LOG_LEVEL", "DEBUG")  # 日志级别
LOG_COLOR = True  # 是否带有颜色
LOG_IS_WRITE_TO_CONSOLE = True  # 是否打印到控制台
LOG_IS_WRITE_TO_FILE = False  # 是否写文件
LOG_MODE = "w"  # 写文件的模式
LOG_MAX_BYTES = 10 * 1024 * 1024  # 每个日志文件的最大字节数
LOG_BACKUP_COUNT = 20  # 日志文件保留数量
LOG_ENCODING = "utf8"  # 日志文件编码

# 是否详细的打印异常
PRINT_EXCEPTION_DETAILS = True
# 设置不带颜色的日志格式
LOG_FORMAT = "%(threadName)s|%(asctime)s|%(filename)s|%(funcName)s|line:%(lineno)d|%(levelname)s| %(message)s"
# 设置带有颜色的日志格式
os.environ["LOGURU_FORMAT"] = (
    "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
    "<level>{level: <8}</level> | "
    "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>line:{line}</cyan> | <level>{message}</level>"
)
OTHERS_LOG_LEVAL = "ERROR"  # 第三方库的log等级

cookies = {
    'UOR': 'finance.sina.com.cn,vip.stock.finance.sina.com.cn,',
    'SINAGLOBAL': '113.89.234.249_1667201515.160382',
    'SUB': '_2AkMUnzSFf8NxqwFRmP8dy2jlaYh_yQDEieKiw8VeJRMyHRl-yD9kql4JtRB6Px8aagVIiHr7rkogU4kZVU4kpKnxRQOI',
    'SR_SEL': '1_511',
    'SFA_version6.28.0': '2023-07-16%2000%3A24',
    'SFA_version7.0.0': '2023-07-24%2013%3A09',
    'Apache': '112.97.86.231_1690175624.195459',
    'MONEY-FINANCE-SINA-COM-CN-WEB5': '',
    'SFA_version7.0.0_click': '1',
    'hqEtagMode': '0',
    'ULV': '1690175629277:8:6:2:112.97.86.231_1690175624.195459:1690175623328',
    'U_TRS1': '000000e7.e4205a7fd.64be0898.9ed75486',
    'U_TRS2': '000000e7.e42c5a7fd.64be0898.609917a8',
    'FIN_ALL_VISITED': 'sh688092%2Csz000001%2Csh600050%2Csh601138%2Csh600487',
    'rotatecount': '3',
    'FINA_V_S_2': 'sh688092,sz000001,sh600050,sh601138,sh600487',
}

headers = {
    'authority': 'vip.stock.finance.sina.com.cn',
    'accept': '*/*',
    'accept-language': 'zh-CN,zh;q=0.9',
    'cache-control': 'no-cache',
    # 'cookie': 'UOR=finance.sina.com.cn,vip.stock.finance.sina.com.cn,; SINAGLOBAL=113.89.234.249_1667201515.160382; SUB=_2AkMUnzSFf8NxqwFRmP8dy2jlaYh_yQDEieKiw8VeJRMyHRl-yD9kql4JtRB6Px8aagVIiHr7rkogU4kZVU4kpKnxRQOI; SR_SEL=1_511; SFA_version6.28.0=2023-07-16%2000%3A24; SFA_version7.0.0=2023-07-24%2013%3A09; Apache=112.97.86.231_1690175624.195459; MONEY-FINANCE-SINA-COM-CN-WEB5=; SFA_version7.0.0_click=1; hqEtagMode=0; ULV=1690175629277:8:6:2:112.97.86.231_1690175624.195459:1690175623328; U_TRS1=000000e7.e4205a7fd.64be0898.9ed75486; U_TRS2=000000e7.e42c5a7fd.64be0898.609917a8; FIN_ALL_VISITED=sh688092%2Csz000001%2Csh600050%2Csh601138%2Csh600487; rotatecount=3; FINA_V_S_2=sh688092,sz000001,sh600050,sh601138,sh600487',
    'pragma': 'no-cache',
    'referer': 'https://vip.stock.finance.sina.com.cn/quotes_service/view/vMS_tradedetail.php?symbol=sh688092',
    'sec-ch-ua': '"Not/A)Brand";v="99", "Google Chrome";v="115", "Chromium";v="115"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
}