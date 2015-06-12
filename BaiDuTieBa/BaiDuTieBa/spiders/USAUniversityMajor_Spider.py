# -*- coding: utf-8 -*-
#---------------------------------------
#   程序：抓取美国专业数据
#   版本：0.1
#   作者：Henry.Zhou
#   日期：2015.05.28
#   语言：Python 2.7
#   功能：抓取尚友美国学校数据
#   http://www.sharewithu.com/college/ranks.php
#---------------------------------------
import scrapy
from scrapy.selector import Selector
from urllib2 import Request, urlopen, URLError, HTTPError
from pyquery import PyQuery as pq
from BaiDuTieBa.Utility.Request_Utility import Request_Utility
from BaiDuTieBa.Utility.USATypeConvert_Utility import USATypeConvert_Utility
from BaiDuTieBa.Utility.SqlServer_Utility import SqlServer_Utility
from BaiDuTieBa.items import USAUniversity_Items
import string

class USAUniversityMajor_Spider(scrapy.Spider):
    name = "USA.Major"
    allowed_domains = ["http://www.sharewithu.com/"]
    start_urls = [
        "http://www.sharewithu.com/college/ranks.php", ]
    #设置头部信息
    headers = Request_Utility().Get_Request_Headers()

    def parse(self, response):
        items = []
        p = pq(response.body)
        list = p.find(".us_div td")
        for u in list:
            parentName = pq(u).find("h3").html();
            type = SqlServer_Utility().InsertUniversity_USAMajor(parentName.strip(),0,parentName.strip(),'0')
            for a in pq(u).find("a"):
                rurl = pq(a).attr("href").replace("rank.php?id=",'')
                SqlServer_Utility().InsertUniversity_USAMajor(pq(a).html().strip(),type,parentName.strip(),rurl)
            #print 'parentName:',parentName,'\r\n mName:',mName

