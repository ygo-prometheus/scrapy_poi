# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import logging
import time
import pymongo

from datetime import datetime
import datetime as dt
from datetime import timedelta

import oss2 as oss
import pytz
import requests.exceptions
# import spider_sysmon
from scrapy import logformatter
from .preset_items import ErrorItem

from .connection_mongodb import mongodb_client

from scrapy.exceptions import DropItem

_tz = pytz.timezone('Asia/Shanghai')


class PoliteLogFormatter(logformatter.LogFormatter):
    def dropped(self, item, exception, response, spider):
        return {
            'level': logging.DEBUG,
            'msg': logformatter.DROPPEDMSG,
            'args': {
                'exception': exception,
                'item': '====drop html item:{}======='.format(item.get('url')),
            }
        }


class MongodbPipelineV2:
    collection_name_index_list = {}  # 索引创建配置
    # example
    # collection_name_index_list = {
    #     "collection_name": [{"field_name": "", "index_type": ""}]
    # }

    collection_separate_table = {}  # 分表分库配置
    __collection_separate_table = {}
    # example
    # 自动分表分库　Ｙ,M,W,D，分别是按年分表，按月分表，按照星期分表，按照天分表
    # collection_separate_table = \
    #     {
    #         'TrainsPrice': 'D',
    #         'TrainsLeftTicket': 'D',
    #         "TrainsInfo": 'W',
    #         "TrainsInfoV2": 'W',
    #     }

    _tz = pytz.timezone('Asia/Shanghai')

    mongodb_database = None

    def __init__(self):
        self.mongodb = None
        self.client = None
        self.db = None
        self.collection = None

    def open_spider(self, spider):
        _ = spider
        self.mongodb = self.mongodb_database or spider.crawler.settings.get(
            'MONGODB_DATABASE', '')
        self.client = mongodb_client
        self.db = self.client[self.mongodb]

        # 帮忙创建索引
        if self.collection_name_index_list:
            self.create_index()

    def close_spider(self, spider):
        _ = spider
        self.client.close()

    def create_index(self, tz=None):

        tz = tz or self._tz

        for collection_name_, field_name_index_list in self.collection_name_index_list.items():

            for field_name_index in field_name_index_list:
                index_type = field_name_index["index_type"]
                field_name = field_name_index["field_name"]

                if index_type == 'unique':
                    collection_name = self._separate_table(collection_name_, tz=tz)
                    self.db[collection_name].ensure_index(field_name, unique=True)
                else:
                    collection_name = self._separate_table(collection_name_, tz=tz)
                    self.db[collection_name].ensure_index(field_name)

    def _separate_table(self, table_name_o, tz=None):

        if not self.collection_separate_table:
            return table_name_o
        t = self.collection_separate_table[table_name_o]
        table_name = self.separate_table_base(table_name_o, t[0], tz=tz)

        self.__collection_separate_table[table_name_o] = table_name

        return table_name

    def separate_table_base(self, table_name, t, tz=None):
        """
        :param table_name: 表名称
        :param t: 参数为　Ｙ,M,W,D，分别是按年分表，按月分表，按照星期分表，按照天分表
        :return:
        """

        today = dt.datetime.now(tz or self._tz)
        y = str(today.year).rjust(2, '0')
        m = str(today.month).rjust(2, '0')
        w = today.strftime("%W").rjust(2, '0')
        d = str(today.day).rjust(2, '0')

        if t == 'Y':
            return '{}_{}'.format(table_name, y)
        elif t == 'M':
            return '{}_{}_{}'.format(table_name, y, m)
        elif t == 'W':
            return '{}_{}_{}'.format(table_name, y, w)
        elif t == 'D':
            return '{}_{}_{}_{}'.format(table_name, y, m, d)
        elif t == '':
            return table_name

    def separate_table_dynamic(self, table_name_o, tz=None):
        """
        动态生成表名称,可以规避长时间运行不切库的问题,但是消耗资源比较大,而且会出现动态库没有自动创建索引的问题
        :param table_name_o:
        :param tz:
        :return:
        """
        tz = tz or self._tz
        return self._separate_table(table_name_o, tz=tz)

    def separate_table(self, table_name):
        """
        静态生成表名称,但是存在长时间运行不切库的问题
        :param table_name: 表名称
        :return:
        """
        _ = self
        if not self.collection_separate_table or not self.__collection_separate_table:
            return table_name
        t = self.__collection_separate_table[table_name]
        return t

    @staticmethod
    def add_ts(item):
        """
        为item　添加同一个的时间字段
        :param item:
        :return:
        """
        i = dict()
        i['created_time'] = datetime.utcnow()
        i['created_time_ts'] = int(time.time() * 1000)
        item.update(i)
        return item

    @staticmethod
    def insert_one_ignor_duplicate_key_error(collection_, data):
        """
        忽略唯一键错误
        :param collection_: 表对象
        :param data: 数据
        :return:
        """
        try:
            return collection_.insert_one(data)
        except pymongo.errors.DuplicateKeyError:
            pass


class MongodbPipeline(object):
    collection_name_list = {}  # 索引创建配置
    collection_separate_table = {}  # 分表分库配置
    mongodb_database = None

    def __init__(self):
        self.mongodb = None
        self.client = None
        self.db = None
        self.collection = None

    def open_spider(self, spider):
        _ = spider
        self.mongodb = self.mongodb_database or spider.crawler.settings.get(
            'MONGODB_DATABASE', '')
        self.client = mongodb_client
        self.db = self.client[self.mongodb]

    def close_spider(self, spider):
        _ = spider
        self.client.close()

    def create_index(self, tz=None):
        for collection_name_, v in self.collection_name_list.items():

            for k, k_type in v:
                if k_type == 'unique':
                    collection_name = self._separate_table(collection_name_, tz=tz)
                    self.db[collection_name].ensure_index(k, unique=True)
                else:
                    collection_name = self._separate_table(collection_name_, tz=tz)
                    self.db[collection_name].ensure_index(k)

    def _separate_table(self, table_name, tz=None):

        if not self.collection_separate_table:
            return table_name
        t = self.collection_separate_table[table_name]
        table_name = self.separate_table_base(table_name, t[0], tz=tz)

        if len(t) == 2:
            t[1] = table_name
        elif len(t) == 1:
            t.append(table_name)
        return t[1]

    @staticmethod
    def separate_table_base(table_name, t, tz=None):
        """
        :param table_name: 表名称
        :param t: 参数为　Ｙ,M,W,D，分别是按年分表，按月分表，按照星期分表，按照天分表
        :return:
        """

        today = dt.datetime.now(tz)
        y = str(today.year).rjust(2, '0')
        m = str(today.month).rjust(2, '0')
        w = today.strftime("%W").rjust(2, '0')
        d = str(today.day).rjust(2, '0')

        if t == 'Y':
            return '{}_{}'.format(table_name, y)
        elif t == 'M':
            return '{}_{}_{}'.format(table_name, y, m)
        elif t == 'W':
            return '{}_{}_{}'.format(table_name, y, w)
        elif t == 'D':
            return '{}_{}_{}_{}'.format(table_name, y, m, d)
        elif t == '':
            return table_name

    def separate_table(self, table_name):
        """
        :param table_name: 表名称
        :param t: 参数为　Ｙ,M,W,D，分别是按年分表，按月分表，按照星期分表，按照天分表
        :return:
        """
        _ = self
        if not self.collection_separate_table:
            return table_name
        t = self.collection_separate_table[table_name]
        return t[1]

    @staticmethod
    def add_ts(item):
        """
        为item　添加同一个的时间字段
        :param item:
        :return:
        """
        i = dict()
        i['created_time'] = datetime.utcnow()
        i['created_time_ts'] = int(time.time() * 1000)
        item.update(i)
        return item

    @staticmethod
    def insert_one_ignor_duplicate_key_error(collection_, args=None, kwargs=None):
        try:
            if not kwargs:
                kwargs = {}
            return collection_.insert_one(*args, **kwargs)
        except pymongo.errors.DuplicateKeyError:
            pass


class MongodbPipelineBySpiderName(MongodbPipelineV2):
    spider_table = {}  # 爬虫名称对应表名称{"spider.name": "表名称"}

    def process_item_before(self, item, spider):
        pass

    def process_item_one(self, name, item, spider):
        self.process_item_before(item, spider)
        item = dict(item)
        self.insert(item, self.spider_table[name])
        self.process_item_after(item, spider)
        return item

    def process_item(self, item, spider):
        return self.process_item_one(spider.name, item, spider)

    def process_item_after(self, item, spider):
        pass


class MongodbPipelineByItemClassName(MongodbPipelineBySpiderName):

    def process_item(self, item, spider):
        return self.process_item_one(item.__class__.__name__, item, spider)


class MongodbPipelineBySpiderClassName(MongodbPipelineBySpiderName):

    def process_item(self, item, spider):
        return self.process_item_one(spider.__class__.__name__, item, spider)


class AddTime:

    def process_item(self, item: dict, spider):
        """
        为所有的item加上统一的时间字段
        :param item:
        :param spider:
        :return:
        """
        _ = spider
        _ = self
        i = dict()
        i['created_time'] = datetime.utcnow()
        i['created_time_ts'] = int(time.time() * 1000)
        item.update(i)
        return item

def _retry_oss_on_error(exception):
    if isinstance(exception, (
            requests.exceptions.Timeout,
            requests.exceptions.HTTPError,
            requests.exceptions.ConnectionError,
    )):
        return True

    if isinstance(exception, oss.exceptions.ServerError):
        if exception.code == 'InternalError':
            # oss 内部错误
            return True

    return False


# class MonitorPipeline(MongodbPipeline, spider_sysmon.SysmonPipeline):
#     collection_name_list = {}
#     ck_count = {}
#     timedelta_hours = 0
#
#     def status(self, spider):
#         if not self.collection_name_list or not self.ck_count or not self.timedelta_hours:
#             return
#         now = datetime.now(_tz)
#         hour = timedelta(hours=self.timedelta_hours)
#         dt = now - hour
#
#         # db = client.database
#         col_count = []
#         caution = ''
#         for k, v in self.collection_name_list.items():
#             if spider.name == k:
#                 # if isinstance(v, list):
#                 for i in v:
#                     self.db[i].ensure_index('modified')
#                     count = self.db[i].find({'modified': {'$gte': dt}}).count()
#                     col_count.append(
#                         {i: {'crawl': count, 'ck': self.ck_count[i], 'crawl/ck': round(count / self.ck_count[i], 2)}})
#                     # if count / ck_count[i] >= 1.2 or count / ck_count[i] <= 0.8:
#                     if count / self.ck_count[i] <= 0.5:
#                         # if count/ck_count[i] >= 1 or count/ck_count[i] < 1:
#                         caution = '警告！！！！！！！！！！！！！！！！！'
#         return '{}{}爬取的数据表:数据量{}'.format(caution, spider.name, col_count)


class DropItemLogPipeline(object):

    def process_item(self, item, spider):
        pass


# 错误收集管道
class RequestErrorMongodbPipeline(MongodbPipeline):

    def process_item(self, item, spider):

        if isinstance(item, ErrorItem):

            self.db['RequestError'].insert(dict(item))
            raise DropItem()
        else:
            return item
