from scrapy_redis.spiders import RedisCrawlSpider as RCS, RedisSpider as RSD
from scrapy.spiders import CrawlSpider as CS, Spider as SD


import scrapy
import json
from functools import partial

from .spider_utils import request_to_dict_cus
from .utils import UtilsPKG
from .shortcut import ShortCut
from .preset_items import ErrorItem
from .preset_pipelines import MongodbPipeline

from scrapy.core.downloader.middleware import DownloaderMiddlewareManager,\
    defer, Request, Response, mustbe_deferred

import six
from .log import init_logger


def download(self, download_func, request, spider):

    def save_error(reason):
        request_dict = request_to_dict_cus(request, spider)

        item_ = ErrorItem(reason=reason,
                          request=request_dict,
                          traceback=spider.ShortCut.format_exc())

        MongodbPipeline.add_ts(item_)
        spider.ShortCut.thread_mongodb_insert(spider.crawler.settings.get(
            'MONGODB_DATABASE', ''), 'RequestError', dict(item_))

    @defer.inlineCallbacks
    def process_request(request):
        for method in self.methods['process_request']:

            response = yield method(request=request, spider=spider)

            assert response is None or isinstance(response, (Response, Request)), \
                'Middleware %s.process_request must return None, Response or Request, got %s' % \
                (six.get_method_self(method).__class__.__name__, response.__class__.__name__)
            if response:
                defer.returnValue(response)
        defer.returnValue((yield download_func(request=request, spider=spider)))

    @defer.inlineCallbacks
    def process_response(response):
        assert response is not None, 'Received None in process_response'
        if isinstance(response, Request):
            defer.returnValue(response)

        for method in self.methods['process_response']:
            try:
                response = yield method(request=request, response=response,
                                        spider=spider)
            except Exception as e:
                save_error('process_response_error')
                raise

            assert isinstance(response, (Response, Request)), \
                'Middleware %s.process_response must return Response or Request, got %s' % \
                (six.get_method_self(method).__class__.__name__, type(response))
            if isinstance(response, Request):
                defer.returnValue(response)
        defer.returnValue(response)

    @defer.inlineCallbacks
    def process_exception(_failure):
        exception = _failure.value
        for method in self.methods['process_exception']:

            try:
                response = yield method(request=request, exception=exception,
                                        spider=spider)
            except Exception as e:
                _ = e
                """错误搜集函数"""
                save_error('process_exception_error')

                raise

            assert response is None or isinstance(response, (Response, Request)), \
                'Middleware %s.process_exception must return None, Response or Request, got %s' % \
                (six.get_method_self(method).__class__.__name__, type(response))
            if response:
                defer.returnValue(response)

        defer.returnValue(_failure)

    deferred = mustbe_deferred(process_request, request)
    deferred.addErrback(process_exception)
    deferred.addCallback(process_response)
    return deferred

DownloaderMiddlewareManager.download = download


# @zs_logging.log_to_fluentd()
class BaseSpider(object):

    UtilsPKG = UtilsPKG
    ShortCut = ShortCut

    def __init__(self, *args, **kwargs):
        # self.logger = init_logger(__name__)

        self._mode = kwargs.get('mode', 'normal')
        # 对不同模式下的爬虫进行识别调整设置

        self._kpl_time = None
        if self._mode == 'KPL':
            self._kpl_time = int(kwargs.get('kpl_time', '600'))

        def to_json(self_, text=None):
            if not text:
                return json.loads(self_.text)
            else:
                return json.loads(text)

        def retry_(response, retry_times=10, priority=10):
            meta = response.meta
            times_ = meta.get('retry_times_cus', 0)

            if retry_times == -1:
                times_ += 1
                resquest_ = response.request
                resquest_.dont_filter = True
                resquest_.meta['retry_times_cus'] = times_
                resquest_.priority = priority

                self.logger.info('retry times: {}, {}'.format(times_, response.url))
                return resquest_

            if times_ <= retry_times:

                times_ += 1
                resquest_ = response.request
                resquest_.dont_filter = True
                resquest_.meta['retry_times_cus'] = times_
                resquest_.priority = priority

                self.logger.info('retry times: {}, {}'.format(times_, response.url))
                return resquest_
            else:
                self.logger.info('retry times: {} too many {}'.format(times_, response.url))

                req = request_to_dict_cus(response.request, self)

                item = ErrorItem(
                    reason='cus_retry too many',
                    request=req,
                )

                return item

        scrapy.http.Response.json = to_json
        scrapy.http.Response.retry = retry_

        if hasattr(self, 'parse'):
            scrapy.Request = partial(scrapy.Request, errback=self._error_back, callback=self.parse)
        else:
            scrapy.Request = partial(scrapy.Request, errback=self._error_back)

        assert not self.__dict__.get('custom_settings'), "请设置custom_settings"
        # 自动集成储存源数据系统

        # 开启线程池
        if self.custom_settings.get('EnThreadPool', False):
            self.executor = self.ShortCut.futures.ThreadPoolExecutor(max_workers=self.custom_settings['max_workers'])
            self.executor.shutdown()

    def _error_back(self, failure):
        self.logger.error('_error_back, error_url:{}, reason:{}'.format(failure.request.url, failure.value))
        self.logger.error('{}. _error_back'.format(failure))
        self.logger.error('end')


        req = request_to_dict_cus(failure.request, self)

        item = ErrorItem(
            reason=str(failure.value),
            request=req,
            failure=self.failure_to_dict(failure),
        )
        yield item

    @staticmethod
    def failure_to_dict(failure):
        d = failure.__dict__.copy()
        del_list_k = ['value', 'type', 'request', 'tb']
        for k in del_list_k:
            try:
                del d[k]
            except:
                pass
        return d


class CrawlSpider(BaseSpider, CS):

    def __init__(self, *args, **kwargs):
        BaseSpider.__init__(self, *args, **kwargs)
        CS.__init__(self, *args, **kwargs)


class CrawlSpiderRedis(BaseSpider, RCS):

    def __init__(self, *args, **kwargs):
        BaseSpider.__init__(self, *args, **kwargs)
        RCS.__init__(self, *args, **kwargs)


class Spider(BaseSpider, SD):

    def __init__(self, *args, **kwargs):
        BaseSpider.__init__(self, *args, **kwargs)
        SD.__init__(self, *args, **kwargs)


class SpiderRedis(BaseSpider, RSD):

    def __init__(self, *args, **kwargs):
        BaseSpider.__init__(self, *args, **kwargs)
        RSD.__init__(self, *args, **kwargs)
