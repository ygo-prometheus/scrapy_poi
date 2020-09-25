import pymongo
import datetime
from ..settings import *

__all__ = ['mongodb_client', 'connection_mongodb']

mongodb_url = globals().get('MONGODB_URL')
mongodb_url_unsafe = globals().get('MONGODB_URL')


def connection_mongodb(mongodb_uri):
    client = pymongo.MongoClient(mongodb_uri)
    return client

# 单例mongodb客户端
mongodb_client = connection_mongodb(mongodb_url)
mongodb_client_unsafe = connection_mongodb(mongodb_url_unsafe)


# 日期生成工具，方便按照日期分表分库
def get_today_year_month_day():
    today = datetime.date.today()
    return str(today.year), str(today.month), str(today.day), today.strftime("%W")

if __name__ == '__main__':
    print(get_today_year_month_day())
