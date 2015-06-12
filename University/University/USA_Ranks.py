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
from Utility.USATypeConvert_Utility import USATypeConvert_Utility
from Utility.MySqlUtility import MySql_Utility
from Utility.Log_Utility import Log_Utility

import urllib2
from pyquery import PyQuery as pq
from Items.USA_Item import USAUniversity_Itemsn
from Items.USA_Item import ContentItem
import string
import sys
#sys.stdout = open('usa_output.txt', 'w')
import json
MySql_Utility = MySql_Utility()
Log = Log_Utility()
class USA_Ranks(object):
    #Log("-------Start-------")
    def parse(self, url,type,typeMsg):
        myPage = urllib2.urlopen(url).read()
        p = pq(myPage)
        list = p.find("div.cf_college")
        try:
            #print myPage
            for u in list:
                #根据Rurl到数据库查询Sysno
                rurl = pq(u).find("a").attr("href").replace("/college/college.php?id=",'')
                print rurl
                #排名
                rank = pq(u).find(".cf_c_name_div_bg").html().strip()
                name = pq(u).find(".cf_c_n_name a").html().strip()
                #根据rurl获取学校sysno
                sysno = MySql_Utility.ByRUrlGetUniversityUSASysno(rurl)
                if int(sysno) > -1:
                    #添加排名数据
                    MySql_Utility.InsertUniversity_USAMajorRanks(sysno,2015,type,typeMsg,rank)
                    print '学校名称：',name,';  学校排名：',rank,'; 学校编号：',sysno
                else:
                    print '非美国学校数据不进行处理'

            #下一页
            print "http://www.sharewithu.com"+p.find(".p_next").attr("href")
            if p.find(".p_next").attr("href").index('rank.php?id') != "javascript:;":
                self.parse("http://www.sharewithu.com"+p.find(".p_next").attr("href"),type,typeMsg)
        except Exception, e:
            #Log.Log(e)
            print e
    def parse2(self, url):
        myPage = urllib2.urlopen(url).read()
        p = pq(myPage)
        list = p.find("div.cf_college")
        #print 111
        for u in list:
            #try:
                ranking = pq(u).find(".cf_c_m_ranking").html().strip()
                rurl = pq(u).find("a").attr("href").replace("/college/college.php?id=",'')
                entity = MySql_Utility.ByRUrlGetUniversityUSAInfo(rurl)
                print entity.Items["classes"]


                #MySql_Utility.UpdateUniversity_USARanking(d["Items"],entity.sysno)
            #except Exception, e:
            #    print ((Exception,":",e))
        #下一页
        #print len(p.find(".p_next"))
        if len(p.find(".p_next")) == 0:
            return
        else:
            #print "http://www.sharewithu.com"+p.find(".p_next").attr("href")
            Log.Log("http://www.sharewithu.com"+p.find(".p_next").attr("href"))
            if p.find(".p_next").attr("href") == "javascript:;":
                return
            else:
                self.parse2("http://www.sharewithu.com"+p.find(".p_next").attr("href"))

    def dict2object(self,d):
        #convert dict to object
        if'__class__' in d:
            class_name = d.pop('__class__')
            module_name = d.pop('__module__')
            module = __import__(module_name)
            class_ = getattr(module,class_name)
            args = dict((key.encode('ascii'), value) for key, value in d.items()) #get args
            inst = class_(**args) #create new instance
        else:
            inst = d
        return inst

    def M1(self):
        url = 'http://www.sharewithu.com/college/rank.php?id=5006101'
        type = 100001
        print '正在获取【国家级大学排名】排名数据.....'
        USA_Ranks().parse(url,type,('国家级大学排名').decode('utf8'))
    def M2(self):
        url = 'http://www.sharewithu.com/college/rank.php?id=5006102'
        type = 100002
        print '正在获取【文理院校排名】排名数据.....'
        USA_Ranks().parse(url,type,('文理院校排名').decode('utf8'))
    def M3(self):
        url = 'http://www.sharewithu.com/college/rank.php?id=10013'
        type = 100003
        print '正在获取【2013-2014泰晤士世界大学排名】排名数据.....'
        USA_Ranks().parse(url,type,('2013-2014泰晤士世界大学排名').decode('utf8'))
    def M4(self):
        url = 'http://www.sharewithu.com/college/rank.php?id=50002'
        type = 100004
        print '正在获取【2013上海交大世界大学排名】排名数据.....'
        USA_Ranks().parse(url,type,('2013上海交大世界大学排名').decode('utf8'))
    def M5(self):
        url = 'http://www.sharewithu.com/college/rank.php?id=20013'
        type = 100005
        print '正在获取【2013-2014QS世界大学排名】排名数据.....'
        USA_Ranks().parse(url,type,('2013-2014QS世界大学排名').decode('utf8'))
    def M6(self):
        url = 'http://www.sharewithu.com/college/rank.php?id=50004'
        type = 100006
        print '正在获取【2013金融时报全球MBA排名】排名数据.....'
        USA_Ranks().parse(url,type,('2013金融时报全球MBA排名').decode('utf8'))
    def M7(self):
        items = MySql_Utility.GetUniversityMajorInfo()
        for item in items:
            print item
            url = 'http://www.sharewithu.com/college/rank.php?id=' + str(item["rsysno"])
            type = item["rsysno"]
            #print ('正在获取专业排名数据(').decode('utf8') + ent[3] + (')').decode('utf8') + ent[0] + '--' + ent[1] + '----' + ent[2]
            #Log(('正在获取专业排名数据(').decode('utf8') + ent[3] + (')').decode('utf8') + ent[0] + '--' + ent[1] + '----' + ent[2])
            USA_Ranks().parse(url,type,(item["name"] + '-' +item["typename"] + ('排名').decode('utf8')))
    def M8(self):
        USA_Ranks().parse2("http://www.sharewithu.com/college/search.php?country=20001&is_expend=1")
    @staticmethod
    def run():

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
        #   7. 专业排名
        #   8. 排名介绍--学校列表展现
        #   0. 1-7
        #---------------------------------------
        """
        print u'请输入要执行命令编 号：'
        #id = raw_input()
        id=8
        rank = USA_Ranks()
        if int(id) == 0:
            rank.M1()
            rank.M2()
            rank.M3()
            rank.M4()
            rank.M5()
            rank.M6()
            rank.M7()
            rank.M8()
        if int(id) == 1:
            rank.M1()
        elif int(id) == 2:
            rank.M2()
        elif int(id) == 3:
            rank.M3()
        elif int(id) == 4:
            rank.M3()
        elif int(id) == 5:
            rank.M5()
        elif int(id) == 6:
            rank.M6()
        elif int(id) == 7:
            rank.M7()
        elif int(id) == 8:
            rank.M8()