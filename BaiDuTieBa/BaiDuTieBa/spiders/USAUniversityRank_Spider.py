# -*- coding: utf-8 -*-
#---------------------------------------
#   程序：抓取美国学校排名数据-美国大学本科排名
#           国家级大学排名 文理院校排名
#   版本：0.1
#   作者：Henry.Zhou
#   日期：2015.05.29
#   语言：Python 2.7
#   国家级大学排名 "http://www.sharewithu.com/college/rank.php?id=5006101",  #100001
#   文理院校排名   "http://www.sharewithu.com/college/rank.php?id=5006102",  #100002
#   2013-2014泰晤士世界大学排名 "http://www.sharewithu.com/college/rank.php?id=10013",    #100003
#   2013上海交大世界大学排名    "http://www.sharewithu.com/college/rank.php?id=50002",    #100004
#   2013-2014QS世界大学排名     "http://www.sharewithu.com/college/rank.php?id=20013",    #100005
#   2013金融时报全球MBA排名     "http://www.sharewithu.com/college/rank.php?id=50004",    #100006
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
#sys.stdout = open('usa_output.txt', 'w')

class USAUniversityRank_Spider(object):
    #Log("-------Start-------")
    def parse(self, url,type,typeMsg):
        myPage = urllib2.urlopen(url).read()
        p = pq(myPage)
        list = p.find("div.cf_college")
        #print myPage
        for u in list:
            try:
                #根据Rurl到数据库查询Sysno
                rurl = pq(u).find("a").attr("href").replace("/college/college.php?id=",'')
                #排名
                rank = pq(u).find(".cf_c_name_div_bg").html().strip()
                name = pq(u).find(".cf_c_n_name a").html().strip()
                #根据rurl获取学校sysno
                sysno = SqlServer_Utility().ByRUrlGetUniversityUSASysno(rurl)
                #添加排名数据
                SqlServer_Utility().InsertUniversity_USAMajorRanks(sysno,type,typeMsg,rank)
                print '学校名称：',name,';  学校排名：',rank,'; 学校编号：',sysno
            except Exception, e:
                print (Exception,":",e)
                Log((Exception,":",e))
        #下一页
        print "http://www.sharewithu.com"+p.find(".p_next").attr("href")
        if p.find(".p_next").attr("href").index('rank.php?id') != "javascript:;":
            self.parse("http://www.sharewithu.com"+p.find(".p_next").attr("href"),type,typeMsg)

def M1():
    url = 'http://www.sharewithu.com/college/rank.php?id=5006101'
    type = 100001
    print '正在获取【国家级大学排名】排名数据.....'
    USAUniversityRank_Spider().parse(url,type,('国家级大学排名').decode('utf8'))
def M2():
    url = 'http://www.sharewithu.com/college/rank.php?id=5006102'
    type = 100002
    print '正在获取【文理院校排名】排名数据.....'
    USAUniversityRank_Spider().parse(url,type,('文理院校排名').decode('utf8'))
def M3():
    url = 'http://www.sharewithu.com/college/rank.php?id=10013'
    type = 100003
    print '正在获取【2013-2014泰晤士世界大学排名】排名数据.....'
    USAUniversityRank_Spider().parse(url,type,('2013-2014泰晤士世界大学排名').decode('utf8'))
def M4():
    url = 'http://www.sharewithu.com/college/rank.php?id=50002'
    type = 100004
    print '正在获取【2013上海交大世界大学排名】排名数据.....'
    USAUniversityRank_Spider().parse(url,type,('2013上海交大世界大学排名').decode('utf8'))
def M5():
    url = 'http://www.sharewithu.com/college/rank.php?id=20013'
    type = 100005
    print '正在获取【2013-2014QS世界大学排名】排名数据.....'
    USAUniversityRank_Spider().parse(url,type,('2013-2014QS世界大学排名').decode('utf8'))
def M6():
    url = 'http://www.sharewithu.com/college/rank.php?id=50004'
    type = 100006
    print '正在获取【2013金融时报全球MBA排名】排名数据.....'
    USAUniversityRank_Spider().parse(url,type,('2013金融时报全球MBA排名').decode('utf8'))
#-------- 程序入口处 ------------------
print u"""#---------------------------------------
#   程序：抓取美国学校排名数据
#   版本：0.1
#   作者：Henry.Zhou
#   日期：2015.05.29
#   语言：Python 2.7
#   1. 国家级大学排名 "http://www.sharewithu.com/college/rank.php?id=5006101",  #100001
#   2. 文理院校排名   "http://www.sharewithu.com/college/rank.php?id=5006102",  #100002
#   3. 2013-2014泰晤士世界大学排名 "http://www.sharewithu.com/college/rank.php?id=10013",    #100003
#   4. 2013上海交大世界大学排名    "http://www.sharewithu.com/college/rank.php?id=50002",    #100004
#   5. 2013-2014QS世界大学排名     "http://www.sharewithu.com/college/rank.php?id=20013",    #100005
#   6. 2013金融时报全球MBA排名     "http://www.sharewithu.com/college/rank.php?id=50004",    #100006
#   0. 1-7
#---------------------------------------
"""

print u'请输入要执行命令编号：'
id = raw_input()
if int(id) == 0:
    USAUniversityRank_Spider.M1()
    USAUniversityRank_Spider.M2()
    USAUniversityRank_Spider.M3()
    USAUniversityRank_Spider.M4()
    USAUniversityRank_Spider.M5()
    USAUniversityRank_Spider.M6()
if int(id) == 1:
    USAUniversityRank_Spider.M1()
elif int(id) == 2:
    USAUniversityRank_Spider.M2()
elif int(id) == 3:
    USAUniversityRank_Spider.M3()
elif int(id) == 4:
    USAUniversityRank_Spider.M3()
elif int(id) == 5:
    USAUniversityRank_Spider.M5()
elif int(id) == 6:
    USAUniversityRank_Spider.M6()