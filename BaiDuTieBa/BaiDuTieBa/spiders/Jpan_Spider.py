# -*- coding: utf-8 -*-
#---------------------------------------
#   程序：百度贴吧爬虫
#   版本：0.5
#   作者：Henry.Zhou
#   日期：2015.04.03
#   语言：Python 2.7
#   功能：全量获取贴吧列表信息并保存至 Redisco
#---------------------------------------
from BaiDuTieBa.Utility.Request_Utility import Request_Utility
from BaiDuTieBa.Utility.JRedisco_Utility import JRedisco_Utility
from BaiDuTieBa.Utility.MySql_Utility import MySql_Utility

__author__ = 'Administrator'
import scrapy
import redis
from scrapy.selector import Selector
from urllib2 import Request, urlopen, URLError, HTTPError
from pyquery import PyQuery as pq
from BaiDuTieBa.items import JpanList_Items

class Jpan_Spider(scrapy.Spider):
    name = "Jpan"
    allowed_domains = ["tieba.baidu.com"]
    start_urls = [
        "http://tieba.baidu.com/f?kw=%E6%97%A5%E6%9C%AC%E7%95%99%E5%AD%A6&ie=utf-8&pn=0", ]
    #设置头部信息
    headers = Request_Utility().Get_Request_Headers()

    def parse(self, response):
        items = []
        sel = Selector(response)
        sites_even = sel.css('a.j_th_tit')
        furl = sel.xpath("/html/head/meta[2][@furl]")
        for site in sites_even:
            item = JpanList_Items()
            item['title'] = site.xpath('text()').extract()
            item["link"] = site.xpath('@href').extract()
            item["furl"] = furl.x('@furl').extract()
            item['bid'] = item["link"][0].replace("/p/", '')
            # print item;
            items.append(item)
        values = []
        for item in items:
            #保存到Redisco中,方便后续队列下载读取
            JRedisco_Utility().Insert(item["bid"])
            values.append((item["title"],item["link"],item["furl"],item["bid"]))
        #保存至MySql数据库
        MySql_Utility().Insert_JpanList(values)

        try:
            #获取下一页
            p = pq(response.body)
            nextUrl = "http://tieba.baidu.com" + \
            p("a.next").attr("href")
            print 'Next Page :' + nextUrl
            # self.insert(items)
            request = scrapy.Request(nextUrl,
                                     callback=self.parse)
            return request
        except Exception, e:
                print p.html()
                print Exception,":",e


