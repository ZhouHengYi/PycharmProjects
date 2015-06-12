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
import sys
import scrapy
from pyquery import PyQuery as pq
from BaiDuTieBa.items import JpanItem_Items
import MySQLdb
import MySQLdb.cursors
import string
import json
import time

class Jpan_DownLoad_Spider(scrapy.Spider):
    name = "Download.Jpan"
    allowed_domains = ["tieba.baidu.com"]
    start_urls = [
        "http://tieba.baidu.com/f?kw=%E6%97%A5%E6%9C%AC%E7%95%99%E5%AD%A6&ie=utf-8&pn=0", ]
    #设置头部信息
    headers = Request_Utility().Get_Request_Headers()

    def parse(self, response):
        #随即取出一条信息
        self.bid = JRedisco_Utility().Get()
        purl = 'http://tieba.baidu.com/p/'+self.bid
        request = scrapy.Request(purl,
                             callback=self.tlist)
        return request

    def tlist(self,response):
        bid = self.bid
        items = []
        p = pq(response.body)
        #print response.body
        sites_even = p("div.l_post")
        h = pq(response.body)

        furl = h("[furl*='tieba']").attr("furl")
        bid = h("[rel*='canonical']").attr("href")

        if(bid == None):
            return self.nextItem()
        else:
            bid = bid.replace("http://tieba.baidu.com/p/",'')
            for site in sites_even:
                #评论处理
                item = JpanItem_Items()
                item["bid"] = bid
                item["userid"] = pq(site).find(".d_name").attr("data-field")
                item["userid"] = item["userid"].replace("{\"user_id\":",'').replace("}",'')
                item["userhead"] = pq(site).find(".p_author_face img").attr("src")
                item["username"] = pq(site).find(".p_author_face img").attr("username")
                item["userhome"] = pq(site).find(".p_author_face").attr("href")
                item["contentid"] = pq(site).find("[id*='post_content_']").attr("id")
                item["contentid"] = item["contentid"].replace('post_content_','')
                item["content"] = pq(site).find("[id*='post_content_']").html()
                item["turl"] = response.url
                item["furl"] = furl

                entity = json.loads(pq(site).attr("data-field"))
                item["publisherDate"] = entity["content"]["date"]
                items.append(item)

            #处理评论信息
            allHtml = h.html()
            startIndex = allHtml.index('_.Module.use("ppb/widget/postList"')
            endIndex = allHtml.index(',    "isAdThread" : ""')
            str = allHtml[startIndex:endIndex]

            startIndex2 = str.index('"commentList" : ')
            replyStr = str[startIndex2 + 16:]
            rEntity = json.loads(replyStr)
            replyValues = []
            for r in rEntity:
                for re in rEntity[r]:
                    x = time.localtime(re["now_time"])
                    replyValues.append((r,re["comment_id"],re["user_id"],re["content"],time.strftime('%Y-%m-%d %H:%M:%S',x)))

            MySql_Utility().Insert_ReplyItem(replyValues)
            #保存文件至本地
            #f = open(bid+'.txt','w+')
            #f.writelines(str2)
            #f.close()

            values = []
            for ins in items:
                values.append((ins["bid"],ins["userid"],ins["username"],ins["userhead"],ins["userhome"],ins["contentid"],ins["content"],ins["turl"],ins["furl"],ins["publisherDate"]))

            #print values
            #保存至MySql数据库
            MySql_Utility().Insert_JpanItem(values)

            next = p('.pb_list_pager a').filter(lambda i: pq(this).text() == u'\u4e0b\u4e00\u9875').attr('href')
            if next <> None:
                print 'Next Page:',next
                request = scrapy.Request('http://tieba.baidu.com' + next,
                                 callback=self.tlist)
                return request
            else:
                return self.nextItem()

    def nextItem(self):
        self.bid = JRedisco_Utility().Get()
        purl = 'http://tieba.baidu.com/p/'+self.bid
        request = scrapy.Request(purl, callback=self.tlist)
        return request