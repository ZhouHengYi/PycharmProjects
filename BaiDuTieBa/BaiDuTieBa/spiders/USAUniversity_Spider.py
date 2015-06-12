# -*- coding: utf-8 -*-
#---------------------------------------
#   程序：抓取美国学校数据
#   版本：0.1
#   作者：Henry.Zhou
#   日期：2015.05.28
#   语言：Python 2.7
#   功能：抓取尚友美国学校数据
#   http://www.sharewithu.com/college/search.php?country=20001
#---------------------------------------
from BaiDuTieBa.Utility.Request_Utility import Request_Utility
from BaiDuTieBa.Utility.USATypeConvert_Utility import USATypeConvert_Utility
from BaiDuTieBa.Utility.SqlServer_Utility import SqlServer_Utility
from BaiDuTieBa.Utility.Log_Utility import Log_Utility as Log

import scrapy
from scrapy.selector import Selector
from urllib2 import Request, urlopen, URLError, HTTPError
from pyquery import PyQuery as pq
from BaiDuTieBa.items import USAUniversity_Items
import string
import sys
sys.stdout = open('usa_output.txt', 'w')

class USAUniversity_Spider(scrapy.Spider):

    name = "USA"
    allowed_domains = ["http://www.sharewithu.com/"]
    start_urls = [

        ]
    p = 1
    while p <= 230:
        url = 'http://www.sharewithu.com/college/search.php?country=20001&p='+str(p)
        start_urls.append(url)
        p = p+1
    print start_urls
    #设置头部信息
    headers = Request_Utility().Get_Request_Headers_USA()
    Log("-------Start-------")
    def parse(self, response):

        sel = Selector(response)
        p = pq(response.body)
        list = p.find("div.cf_college")

        for u in list:
            try:
                item = USAUniversity_Items()
                item["name"] = pq(u).find(".cf_c_n_name").html()
                item["englishName"] = pq(u).find(".cf_c_n_englishname").html()
                item["xiangqu"] = pq(u).find(".cf_c_number").eq(0).html()
                item["shenqing"] = pq(u).find(".cf_c_number").eq(1).html()
                item["zaidu"] = pq(u).find(".cf_c_number").eq(2).html()
                item["logo"] = pq(u).find(".cf_c_image").attr("src")
                item["ranking"] = pq(u).find(".cf_c_m_ranking").html().strip()
                typeStr = pq(u).find(".cf_c_message div").html()
                #print '-----',typeStr
                if len(typeStr.strip().split(' ')) == 1:
                    item["classes"] = USATypeConvert_Utility().Converts(typeStr,1)
                    item["requestType"] = USATypeConvert_Utility().Converts(typeStr,2)
                else:
                    item["classes"] = USATypeConvert_Utility().Convert(typeStr,1)
                    item["requestType"] = USATypeConvert_Utility().Convert(typeStr,2)
                item["rurl"] = pq(u).find("a").attr("href").replace("/college/college.php?id=",'');
                #处理专业信息
                major = pq(u).find(".cf_c_profession span")
                majorStr = ""
                for m in major:
                    majorStr += pq(m).html().decode('gbk') + ","
                if len(majorStr) > 0:
                    majorStr = majorStr[0:len(majorStr) - 1]
                item["major"] = majorStr
                SqlServer_Utility().InsertUniversity_USA(item)
            except Exception, e:
                Log((Exception,":",e))
        #**
        # try:
            #获取下一页
        #    next = 'http://www.sharewithu.com' + p.find(".p_next").attr("href")
        #    print 'Next Page:',next
        #    return scrapy.Request(next,
        #                     callback=self.parse)
        #except Exception, e:
        #        print p.html()
        #        print Exception,":",e