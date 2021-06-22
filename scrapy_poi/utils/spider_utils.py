# coding:utf-8

from scrapy.utils.project import get_project_settings
from ..settings import *
from scrapy.utils.python import to_unicode
from scrapy.utils.datatypes import CaselessDict
from scrapy.utils.reqser import request_to_dict
settings = get_project_settings()


def to_unicode_dict(d):
    """ Return headers as a CaselessDict with unicode keys
    and unicode values. Multiple values are joined with ','.
    """
    return CaselessDict(
        (to_unicode(key, encoding='utf-8'),
         to_unicode(b','.join(value), encoding='utf-8'))
        for key, value in d.items())


def request_to_dict_cus(request, spider):
    req = request_to_dict(request, spider=spider)

    # 由于返回的headers的key为比特，无法直接储存需要转换
    headers = req['headers'].copy()
    headers = to_unicode_dict(headers)

    req['headers'] = headers
    return req


class CustomSettings(object):
    """此类的唯一目的就是为你便捷的定义自定义设置，有代码提示哦
    使用方法
    c = CustomSettings()
    c.CUS_DOWNLOAD_DELAY = 63
    custom_settings = c()
    """
    file_name = settings['BOT_NAME']

    def __init__(self):
        self.ITEM_PIPELINES_CUS = {}
        self.SPIDER_MIDDLEWARES_CUS = {}
        self.DOWNLOADER_MIDDLEWARES_CUS = {}
        self.EXTENSIONS_CUS = {}

        self.DOWNLOAD_DELAY_CUS = 'default'
        self.CONCURRENT_REQUESTS_PER_DOMAIN_CUS = 16
        self.CONCURRENT_REQUESTS_PER_IP_CUS = 16
        self.CONCURRENT_REQUESTS_CUS = 16
        self.DOWNLOAD_TIMEOUT_CUS = 10
        self.TIMEOUT_CUS = 10

        self.COOKIES_ENABLED_CUS = 'default'
        self.COOKIES_DEBUG_CUS = 'default'

        self.RETRY_ENABLED_CUS = 'default'
        self.RETRY_HTTP_CODES_CUS = 'default'
        self.RETRY_TIMES_CUS = 16
        self.RETRY_PRIORITY_ADJUST_CUS = 'default'

        self.EnRedis = False

        self.SCHEDULER_CUS = 'default'
        self.DUPEFILTER_CLASS_CUS = 'default'
        self.REDIS_START_URLS_AS_SET_CUS = 'default'
        self.SCHEDULER_QUEUE_CLASS_CUS = 'default'
        self.REDIS_ITEMS_KEY_CUS = 'default'
        self.REDIS_ITEMS_SERIALIZER_CUS = 'default'
        self.REDIS_START_URLS_KEY_CUS = 'default'
        self.SCHEDULER_IDLE_BEFORE_CLOSE_CUS = 'default'
        self.SCHEDULER_PERSIST_CUS = 'default'

        self.EnFakeUserAgent = False
        self.RANDOM_UA_TYPE_CUS = {"engine": "Windows", "types": "Computer", "limit": 200}

        self.IDLE_NUMBER_CUS = 12 * 6
        self.EnMongoDB = True

    def __call__(self, *args, **kwargs):
        result = dict()

        # 启用redis，分布式爬虫
        if self.EnRedis:
            self.SCHEDULER_CUS = "scrapy_redis.scheduler.Scheduler"
            self.DUPEFILTER_CLASS_CUS = "scrapy_redis.dupefilter.RFPDupeFilter"

        if self.EnFakeUserAgent:
            self.DOWNLOADER_MIDDLEWARES_CUS.update({
                'scrapy.contrib.downloadermiddleware.useragent.UserAgentMiddleware': None,
                "{}.utils.preset_middlewares.RandomUserAgentMiddleware".format(self.file_name): 400

            })

        if self.EnMongoDB:
            self.ITEM_PIPELINES_CUS.update({
                "{}.utils.preset_pipelines.RequestErrorMongodbPipeline".format(self.file_name): 198,  # 下载错误收集管道
            })


        # 默认内置管道
        self.ITEM_PIPELINES_CUS.update({
            "{}.utils.preset_pipelines.AddTime".format(self.file_name): 197,  # 自动添加时间戳字段
            "{}.utils.preset_pipelines.DropItemLogPipeline".format(self.file_name): 1000,  # 抛弃item的输出日志
        })

        for key_, value_ in self.__dict__.items():

            # 过滤出相关属性
            if key_.endswith('_CUS') or key_.startswith('En'):

                key_ = key_.replace('_CUS', '')

                default = globals().get(key_)
                if isinstance(default, dict):

                    # 和默认配置合并
                    result[key_] = default.copy()
                    result[key_].update(value_)

                elif value_ and value_ != 'default':
                    result[key_] = value_

                elif default:
                    result[key_] = default
                elif key_.startswith('En'):
                    result[key_] = value_

        return result
