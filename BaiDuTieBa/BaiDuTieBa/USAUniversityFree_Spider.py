# -*- coding: utf-8 -*-
#---------------------------------------
#   程序：抓取美国学校费用数据
#   版本：0.1
#   作者：Henry.Zhou
#   日期：2015.05.30
#   语言：Python 2.7
#   功能：抓取美国学校费用数据，作为搜索条件使用$10,000以下 $10,001-$20,000 $20,001-$30,000 $30,000以上
#   http://www.sharewithu.com/college/search.php?country=20001&fee=0-10000&is_expend=1  $10,000以下 - 1
#   http://www.sharewithu.com/college/search.php?country=20001&fee=10001-20000&is_expend=1  $10,001-$20,000 - 2
#   http://www.sharewithu.com/college/search.php?country=20001&fee=20001-30000&is_expend=1  $20,001-$30,000 - 3
#   http://www.sharewithu.com/college/search.php?country=20001&fee=30001-&is_expend=1  $30,000以上 - 4
#---------------------------------------
from BaiDuTieBa.Utility.Request_Utility import Request_Utility
from BaiDuTieBa.Utility.USATypeConvert_Utility import USATypeConvert_Utility
from BaiDuTieBa.Utility.SqlServer_Utility import SqlServer_Utility
from BaiDuTieBa.Utility.Log_Utility import Log_Utility as Log

from scrapy.selector import Selector
import urllib2
from pyquery import PyQuery as pq
from BaiDuTieBa.items import USAUniversity_Items
import string
import sys

class USAUniversityFree_Spider(object):

    def parse(self, url,free):
        myPage = urllib2.urlopen(url).read()
        p = pq(myPage)
        list = p.find("div.cf_college")
        #print 111
        for u in list:
            try:
                item = USAUniversity_Items()
                item["name"] = pq(u).find(".cf_c_n_name").html()
                item["englishName"] = pq(u).find(".cf_c_n_englishname").html()
                item["rurl"] = pq(u).find("a").attr("href").replace("/college/college.php?id=",'')
                sysno = SqlServer_Utility().ByRUrlGetUniversityUSASysno(item["rurl"])
                #print item["rurl"],free
                SqlServer_Utility().UpdateUniversity_USAFree(free,sysno)
            except Exception, e:
                print ((Exception,":",e))
        #下一页
        #print len(p.find(".p_next"))
        if len(p.find(".p_next")) == 0:
            return
        else:
            #print "http://www.sharewithu.com"+p.find(".p_next").attr("href")
            Log("http://www.sharewithu.com"+p.find(".p_next").attr("href"))
            if p.find(".p_next").attr("href") == "javascript:;":
                return
            else:
                self.parse("http://www.sharewithu.com"+p.find(".p_next").attr("href"),free)
    def M1(self):
        url = 'http://www.sharewithu.com/college/search.php?country=20001&fee=0-10000&is_expend=1'
        print '正在获取【$10,000以下】大学数据.....'
        self.parse(url,1)
    def M2(self):
        url = 'http://www.sharewithu.com/college/search.php?country=20001&fee=10001-20000&is_expend=1'
        print '正在获取【$10,001-$20,000】大学数据.....'
        self.parse(url,2)
    def M3(self):
        url = 'http://www.sharewithu.com/college/search.php?country=20001&fee=20001-30000&is_expend=1'
        print '正在获取【$20,001-$30,000】大学数据.....'
        self.parse(url,3)
    def M4(self):
        url = 'http://www.sharewithu.com/college/search.php?country=20001&fee=30001-&is_expend=1'
        print '正在获取【$30,000以上】大学数据.....'
        self.parse(url,4)


#-------- 程序入口处 ------------------
print u"""#---------------------------------------
#   程序：抓取美国学校排名数据
#   版本：0.1
#   作者：Henry.Zhou
#   日期：2015.05.29
#   语言：Python 2.7
#   1 "http://www.sharewithu.com/college/search.php?country=20001&fee=0-10000&is_expend=1",$10,000以下
#   2 "http://www.sharewithu.com/college/search.php?country=20001&fee=10001-20000&is_expend=1",$10,001-$20,000
#   3 "http://www.sharewithu.com/college/search.php?country=20001&fee=20001-30000&is_expend=1",$20,001-$30,000
#   4 "http://www.sharewithu.com/college/search.php?country=20001&fee=30001-&is_expend=1",$30,000以上
#   0. 1-4
#---------------------------------------
"""

print u'请输入要执行命令编号：'
id = raw_input()
if int(id) == 0:
    USAUniversityFree_Spider().M1()
    USAUniversityFree_Spider().M2()
    USAUniversityFree_Spider().M3()
    USAUniversityFree_Spider().M4()
if int(id) == 1:
    USAUniversityFree_Spider().M1()
elif int(id) == 2:
    USAUniversityFree_Spider().M2()
elif int(id) == 3:
    USAUniversityFree_Spider().M3()
elif int(id) == 4:
    USAUniversityFree_Spider().M4()