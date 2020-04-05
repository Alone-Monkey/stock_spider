# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class StockdfcfItem(scrapy.Item):
    # define the fields for your item here like:
    code = scrapy.Field()
    name = scrapy.Field()
    time = scrapy.Field()
    open = scrapy.Field()
    close = scrapy.Field()
    high = scrapy.Field()
    low = scrapy.Field()
    volume = scrapy.Field()
    turnover = scrapy.Field()
