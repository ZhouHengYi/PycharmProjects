
# -*- coding: utf-8 -*-

from Utility.MySqlUtility import MySql_Utility
from Items.UK_Item import UK_ItemBasic
import urllib2
from pyquery import PyQuery as pq

class UK_University(object):

    @staticmethod
    def parse(url):
        print url
        myPage = urllib2.urlopen(url).read()
        p = pq(myPage)
        list = p.find("div.school_list")
        print len(list)
        #print myPage
        for u in list:
            #try:
                item = UK_ItemBasic()
                title = pq(u).find(".schooll_content a").eq(0).attr("title")
                print title
                item.name = title.split('-')[0]
                item.englishName = title.split('-')[1]
                item.xiangqu = pq(u).find(".college_name_en span").html().decode('utf8')
                item.logo = pq(u).find(".schooll_badge img").attr("src")
                item.classes = pq(u).find(".college_info li").eq(1).find("span").html()
                item.rurl = pq(u).find(".college_name_en a").eq(0).attr("href")

                item.JianCheng = pq(u).find(".college_info li").eq(0).find("span").html()
                item.JianXiaoShiJian = pq(u).find(".college_info li").eq(2).find("span").html()
                item.SuoZaiChengShi = pq(u).find(".college_info li").eq(3).find("span").html()
                #判断是否存在
                id = MySql_Utility().ByNameGetUniversityUKSysno(item.name)
                if(id != "-1"):
                    MySql_Utility().InsertUniversity_UK(item)
                else:
                    print '发现重复数据:' + item.name
            #except Exception, e:
            #    print(str(e).decode('utf8') + ' Url:' + url)