# -*- coding: utf-8 -*-
#---------------------------------------
#   程序：抓取美国学校详细数据
#   版本：0.1
#   作者：Henry.Zhou
#   日期：2015.05.28
#   语言：Python 2.7
#   功能：抓取尚友美国学校数据
#   http://www.sharewithu.com/college/college_info.php?id=@sysno
#   /college/college.php?id=315
#---------------------------------------

import scrapy
import string
import sys
#sys.stdout = open('output.txt', 'w')
import urllib2

from pyquery import PyQuery as pq
from Utility.MySqlUtility import MySql_Utility
class USA_ExInfo(object):
    @staticmethod
    def run(url):
        items = MySql_Utility().GetUniversity_USASysno()
        print len(items)
        for item in items:
            url = 'http://www.sharewithu.com/college/college_info.php?id='+item.split(',')[0]
            resp = urllib2.urlopen(url)
            p = pq(resp.read())
            dInfo = p.find(".collegeInfo_content")
            if len(dInfo) > 0:
                #学校介绍
                i1 = pq(dInfo).find(".college_ext_brief").html()
                SqlServer_Utility().InsertUniversity_USA_ExInfo(item.split(',')[1],1,'学校介绍',i1)
                #基本数据
                i2 = pq(dInfo).find(".collegeStat").eq(1).html()
                SqlServer_Utility().InsertUniversity_USA_ExInfo(item.split(',')[1],2,'基本数据',i2)
                #专业介绍
                i3 = pq(dInfo).find(".collegeMajor").html()
                SqlServer_Utility().InsertUniversity_USA_ExInfo(item.split(',')[1],3,'专业介绍',i3)
                #地理位置
                i4 = pq(dInfo).find(".college_geo").html()
                SqlServer_Utility().InsertUniversity_USA_ExInfo(item.split(',')[1],4,'地理位置',i4)
                #已到学生数量
                yiyou = p.find(".college_profile2_info_visit span").html()
                SqlServer_Utility().UpdateUniversity_USAInfo(item.split(',')[1],yiyou)
            else:
                print ('信息异常：').decode('utf8'),len(dInfo)
