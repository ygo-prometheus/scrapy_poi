import pprint
import furl
import traceback
from concurrent import futures

from .connection_mongodb import mongodb_client


class ShortCut:
    pprint = pprint.pprint
    furl = furl.furl

    print_exc = traceback.print_exc
    format_exc = traceback.format_exc

    futures = futures

    mongodb_client = mongodb_client

    # 系统内置线程池
    sys_executor = futures.ThreadPoolExecutor(max_workers=10)

    @classmethod
    def thread_mongodb_insert(cls, db_name, collection_name, *args, **kwargs):
        cls.sys_executor.submit(cls._thread_mongodb_insert_task, db_name, collection_name, *args, **kwargs)

    @classmethod
    def _thread_mongodb_insert_task(cls, db_name, collection_name, *args, **kwargs):
        db = mongodb_client[db_name]
        collection = db[collection_name]
        collection.insert(*args, **kwargs)
