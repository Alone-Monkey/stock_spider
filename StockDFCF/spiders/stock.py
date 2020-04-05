# -*- coding: utf-8 -*-
import scrapy
from StockDFCF.settings import *
from StockDFCF.items import StockdfcfItem
import time
import json


class StockSpider(scrapy.Spider):
    name = 'stock'
    allowed_domains = ['http://30.push2his.eastmoney.com']
    # start_urls = ['http://http://30.push2his.eastmoney.com/']

    def start_requests(self):
        # start_urls = ['https://maoyan.com/board/4?offset=0']

        for code in STOCK_CODES:
            t = time.time() * 1000
            if code[0:2] == "30":
                url = f"http://85.push2his.eastmoney.com/api/qt/stock/kline/get?cb=jQuery112400505215890069574_{t}&secid=0.{code}&ut" \
                      f"=fa5fd1943c7b386f172d6893dbfba10b&fields1=f1%2Cf2%2Cf3%2Cf4%2Cf5&fields2=f51%2Cf52%2Cf53%2Cf54%2Cf55%2Cf56%2" \
                      f"Cf57%2Cf58&klt=60&fqt=0&end=20500101&lmt=120&_={t}"
                yield scrapy.Request(
                    url=url,
                    callback=self.parse
                )
            elif code[0:2] == "60":
                url = f"http://58.push2his.eastmoney.com/api/qt/stock/kline/get?cb=jQuery1124041580509674373456_{t}&secid=1.{code}&" \
                      f"ut=fa5fd1943c7b386f172d6893dbfba10b&fields1=f1%2Cf2%2Cf3%2Cf4%2Cf5&fields2=f51%2Cf52%2Cf53%2Cf54%2Cf55%2Cf56%" \
                      f"2Cf57%2Cf58&klt=60&fqt=0&end=20500101&lmt=120&_={t}"
                yield scrapy.Request(
                    url=url,
                    callback=self.parse
                )
            elif code[0:2] == "00":
                url = f"http://11.push2his.eastmoney.com/api/qt/stock/kline/get?cb=jQuery112406231364400858717_{t}8&secid=0.{code}&ut" \
                      f"=fa5fd1943c7b386f172d6893dbfba10b&fields1=f1%2Cf2%2Cf3%2Cf4%2Cf5&fields2=f51%2Cf52%2Cf53%2Cf54%2Cf55%2Cf56%2C" \
                      f"f57%2Cf58&klt=60&fqt=0&end=20500101&lmt=120&_={t}"
                yield scrapy.Request(
                    url=url,
                    callback=self.parse
                )
            elif code[0:2] == "68":
                url = f"http://48.push2his.eastmoney.com/api/qt/stock/kline/get?secid=1.{code}&fields1=f1,f2,f3,f4,f5&fields2=f51,f52," \
                      f"f53,f54,f55,f56,f57&klt=60&fqt=1&end=20500101&ut=fa5fd1943c7b386f172d6893dbfba10b&cb=cb83292097192194"
                yield scrapy.Request(
                    url=url,
                    callback=self.parse
                )

    def parse(self, response):
        item = StockdfcfItem()
        data = json.loads(response.text.split('(')[1].replace(");", ''))

        for line in data['data']["klines"]:
            item['code'] = data["data"]["code"]
            item['name'] = data["data"]["name"]
            item['time'] = line.split(',')[0]
            item['open'] = float(line.split(',')[1])
            item['close'] = float(line.split(',')[2])
            item['high'] = float(line.split(',')[3])
            item['low'] = float(line.split(',')[4])
            item['volume'] = int(line.split(',')[5])
            item['turnover'] = int(line.split(',')[6][:-3])

            yield item

