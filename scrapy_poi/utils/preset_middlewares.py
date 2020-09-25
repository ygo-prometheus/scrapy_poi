import os
import random

from user_agent import generate_user_agent
from scrapy.downloadermiddlewares.retry import RetryMiddleware


# 随机生成user-agent
class RandomUserAgentMiddleware(object):

    def process_request(self, request, spider):
        request.headers["User-Agent"] = generate_user_agent()


