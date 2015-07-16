# -*- coding: utf-8 -*-
from Utility.MySqlUtility import MySql_Utility
from UK.UK_Convert import UK_Convert
import urllib2
from pyquery import PyQuery as pq
import json
import string

class UK_Ranks(object):

    @staticmethod
    def run(type,brand,year,url):
        cols = UK_Ranks.getColns(url)
        myPage = urllib2.urlopen(url)
        text = unicode(myPage.read(), "utf-8")
        p = pq(text)
        list = p.find("#rk tbody tr")
        print type + " - " + brand + " - " + str(year) + " - " + url
        for u in list:
            item = {}
            tr = pq(u)
            item["Brand"] = brand
            item["Type2"] = type
            item["Year"] = year
            item["Ranking"] = tr.find("td").eq(0).html()

            index = 2
            for c in cols:
                if index < len(cols) and cols[index] != "Free":
                    item[cols[index]] = tr.find("td").eq(index).html()
                index += 1
            aename = tr.find("td").eq(1).find("a")
            if len(aename) == 0:
                item["EnglishName"] = tr.find("td").eq(1).html()
                item["UniversityName"] = tr.find("td").eq(1).html()
                item["status"] = "99"

            else:
                item["EnglishName"] = tr.find("td").eq(1).find("a").attr("href").replace("http://school.promisingedu.com/","")
                item["EnglishName"] = UK_Convert().ConvertEnglishName2(item["EnglishName"])
                name = MySql_Utility().ByEnglishNameGetUniversity_UKName(item["EnglishName"])
                if len(name) > 0:
                    item["UniversityName"] = name[0]["name"] + name[0]["englishName"]
                    item["EnglishName"] = name[0]["englishName"]
                    item["status"] = "1"
                else:
                    item["UniversityName"] = item["EnglishName"]
                    item["status"] = "99"
            if item["UniversityName"] != None:
                MySql_Utility().InsertUniversity_UK_Rank(item)
            else:
                print '数据错误:' + str(item)

    @staticmethod
    def runMajor(type,brand,year,major,url):
        cols = UK_Ranks.getColns(url)
        myPage = urllib2.urlopen(url)
        text = unicode(myPage.read(), "utf-8")
        p = pq(text)
        list = p.find("#rk tbody tr")
        print type + " - " + brand + " - " + str(year) + " - " + url
        for u in list:
            item = {}
            tr = pq(u)
            item["Brand"] = brand
            item["Type"] = type
            item["Year"] = year
            item["Ranking"] = tr.find("td").eq(0).html()

            index = 2
            for c in cols:
                if index < len(cols) and cols[index] != "Free":
                    item[cols[index]] = tr.find("td").eq(index).html()
                index += 1

            aename = tr.find("td").eq(1).find("a")
            if len(aename) == 0:
                item["EnglishName"] = tr.find("td").eq(1).html()
                item["UniversityName"] = tr.find("td").eq(1).html()
                item["status"] = "99"
            else:
                item["EnglishName"] = tr.find("td").eq(1).find("a").attr("href").replace("http://school.promisingedu.com/","")
                item["EnglishName"] = UK_Convert().ConvertEnglishName2(item["EnglishName"])
                name = MySql_Utility().ByEnglishNameGetUniversity_UKName(item["EnglishName"])
                if len(name) > 0:
                    item["UniversityName"] = name[0]["name"] + name[0]["englishName"]
                    item["EnglishName"] = name[0]["englishName"]
                    item["status"] = "1"
                else:
                    item["UniversityName"] = item["EnglishName"]
                    item["status"] = "99"
            if item["UniversityName"] != None:
                MySql_Utility().InsertUniversity_UK_Rank(item)
            else:
                print '数据错误:' + str(item)


    @staticmethod
    def getColns(url):
        myPage = urllib2.urlopen(url).read()
        p = pq(myPage)
        ths = p.find("#rk thead tr th")
        cols = []
        for th in ths:
            html = str(pq(th).html())
            if html.find("<") > - 1:
                cols.append(html[0:html.find("<")])
            else:
                cols.append(html)
        return cols

    @staticmethod
    def getRanks():
        UK_Ranks.run('综合排名','Aleve',2013,"http://ranking.promisingedu.com/a-level")
        UK_Ranks.run('综合排名','Aleve',2009,"http://ranking.promisingedu.com/2009-a-level-en-middle-school")
        #UK_Ranks.run('专业排名',"TIMES",2014,"会计与金融","accounting_and_finance","http://ranking.promisingedu.com/2010-guardian-en-undergraduate-agriculture_forestry_and_food")

        #UK_Ranks.run('综合排名','TIMES',2015,"http://ranking.promisingedu.com/")
        #UK_Ranks.run('综合排名','TIMES',2014,"http://ranking.promisingedu.com/2014-times-en-undergraduate")
        #UK_Ranks.run('综合排名','TIMES',2013,"http://ranking.promisingedu.com/2013-times-en-undergraduate")
        #UK_Ranks.run('综合排名','TIMES',2012,"http://ranking.promisingedu.com/2012-times-en-undergraduate")
        #UK_Ranks.run('综合排名','TIMES',2011,"http://ranking.promisingedu.com/2011-times-en-undergraduate")
        #UK_Ranks.run('综合排名','TIMES',2010,"http://ranking.promisingedu.com/2010-times-en-undergraduate")
        #UK_Ranks.run('综合排名','TIMES',2009,"http://ranking.promisingedu.com/2009-times-en-undergraduate")
        #UK_Ranks.run('综合排名','TIMES',2008,"http://ranking.promisingedu.com/2008-times-en-undergraduate")

        #UK_Ranks.run('综合排名','GUARDIAN',2016,"http://ranking.promisingedu.com/2016-guardian-en-undergraduate")
        #UK_Ranks.run('综合排名','GUARDIAN',2015,"http://ranking.promisingedu.com/2015-guardian-en-undergraduate")
        #UK_Ranks.run('综合排名','GUARDIAN',2014,"http://ranking.promisingedu.com/2014-guardian-en-undergraduate")
        #UK_Ranks.run('综合排名','GUARDIAN',2013,"http://ranking.promisingedu.com/2013-guardian-en-undergraduate")
        #UK_Ranks.run('综合排名','GUARDIAN',2012,"http://ranking.promisingedu.com/2012-guardian-en-undergraduate")
        #UK_Ranks.run('综合排名','GUARDIAN',2011,"http://ranking.promisingedu.com/2011-guardian-en-undergraduate")
        #UK_Ranks.run('综合排名','GUARDIAN',2010,"http://ranking.promisingedu.com/2010-guardian-en-undergraduate")
        #UK_Ranks.run('综合排名','GUARDIAN',2009,"http://ranking.promisingedu.com/2009-guardian-en-undergraduate")
        #UK_Ranks.run('综合排名','GUARDIAN',2008,"http://ranking.promisingedu.com/2008-guardian-en-undergraduate")

        #UK_Ranks.run('综合排名','WORLD TIMES',2014,"http://ranking.promisingedu.com/world-times")

        #UK_Ranks.run('综合排名','CUG',2015,"http://ranking.promisingedu.com/cug")

        #UK_Ranks.run('综合排名','U.S.News',2015,"http://ranking.promisingedu.com/usnews")
        #UK_Ranks.run('综合排名','U.S.News',2014,"http://ranking.promisingedu.com/2014-usnews-all-undergraduate")

        #UK_Ranks.run('综合排名','Aleve',2013,"http://ranking.promisingedu.com/a-level")
        #UK_Ranks.run('综合排名','Aleve',2009,"http://ranking.promisingedu.com/2009-a-level-en-middle-school")

        #UK_Ranks.run('综合排名',"Maclean's doctor",2015,"http://ranking.promisingedu.com/maclean-doctor")
        #UK_Ranks.run('综合排名',"Maclean's",2015,"http://ranking.promisingedu.com/maclean")
        #UK_Ranks.run('综合排名',"Maclean's",2014,"http://ranking.promisingedu.com/2014-maclean-ca-undergraduate")

        #UK_Ranks.run('综合排名',"Maclean's basic",2015,"http://ranking.promisingedu.com/maclean-basic")
        #UK_Ranks.run('综合排名',"qs",2015,"http://ranking.promisingedu.com/qs")

