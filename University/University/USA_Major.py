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
import urllib2
from pyquery import PyQuery as pq
from Utility.MySqlUtility import MySql_Utility
import string

class USA_Major(object):
    @staticmethod
    def parse(url):
        myPage = urllib2.urlopen(url).read()
        p = pq(myPage)
        list = p.find(".us_div td")
        for u in list:
            parentName = pq(u).find("h3").html()
            type = MySql_Utility().InsertUniversity_USAMajor(parentName.strip(),0,parentName,0)
            for a in pq(u).find("a"):
                rurl = pq(a).attr("href").replace("rank.php?id=",'')
                MySql_Utility().InsertUniversity_USAMajor(pq(a).html().strip(),type["sysno"],parentName,rurl)

