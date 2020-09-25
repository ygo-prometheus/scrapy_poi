# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy

class ErrorItem(scrapy.Item):
    created_time = scrapy.Field()
    created_time_ts = scrapy.Field()
    reason = scrapy.Field()
    request = scrapy.Field()
    traceback = scrapy.Field()
    response_text = scrapy.Field()
    failure = scrapy.Field()
