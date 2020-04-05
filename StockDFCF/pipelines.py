# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import time
import pymysql
from StockDFCF.settings import *


class StockdfcfPipeline(object):
    def process_item(self, item, spider):

        return item



class StockDFCFMysqlPipeline(object):

    def open_spider(self,spider):
        print("我是open——spider函数")
        self.db = pymysql.connect(
            MYSQL_HOST,
            MYSQL_USER,
            MYSQL_PWD,
            MYSQL_DB,
            charset = 'utf8'
        )
        self.cursor = self.db.cursor()

    def process_item(self,item,spider):
        print(item)
        try:
            if item['code'][:3] == '60':
                table_name = 'SH' + item['code']
                ins = f'insert into {table_name}(code,name,time,open,close,high,low,volume,turnover) ' \
                      'values(%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            else:
                table_name = 'SZ' + item['code']
                ins = f'insert into {table_name}(code,name,time,open,close,high,low,volume,turnover) ' \
                      'values(%s,%s,%s,%s,%s,%s,%s,%s,%s)'


            data = [
                item['code'],
                item['name'],
                item['time'],
                item['open'],
                item['close'],
                item['high'],
                item['low'],
                item['volume'],
                item['turnover']
            ]


            self.cursor.execute(ins,data)
            self.db.commit()
            print("数据存储成功！")
        except Exception as e:
            print(e)
        return item

    def close_spider(self,spider):
        print("我是close——spider函数")
        self.cursor.close()
        self.db.close()