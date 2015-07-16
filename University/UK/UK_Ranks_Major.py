# -*- coding: utf-8 -*-
from Utility.MySqlUtility import MySql_Utility
from UK.UK_Convert import UK_Convert
import urllib2
from pyquery import PyQuery as pq
import json
import string

class UK_Ranks_Major(object):

    @staticmethod
    def run(url):

        try:
            cols = UK_Ranks_Major.getColns(url)
            myPage = urllib2.urlopen(url)
            text = unicode(myPage.read(), "utf-8")
            p = pq(text)
            list = p.find("#rk tbody tr")

            list2 = p.find(".dd .on")
            type = pq(list2[0]).html()
            brand = pq(list2[1]).html()
            year = pq(list2[2]).html()
            major = pq(list2[3]).html()
            print type + " - " + brand + " - " + str(year) + " - " + url

            for u in list:
                item = {}
                tr = pq(u)
                item["Brand"] = brand
                item["Type2"] = type
                item["Year"] = year
                item["Major"] = major
                item["MajorUrl"] = major
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
        except Exception,ex:
            print str(Exception)
    @staticmethod
    def runMajor(url):
        cols = UK_Ranks_Major.getColns(url)
        myPage = urllib2.urlopen(url)
        text = unicode(myPage.read(), "utf-8")
        p = pq(text)
        list = p.find(".dd .on")
        type = pq(list[0]).html()
        brand = pq(list[1]).html()
        year = pq(list[2]).html()
        td = p.find(".rank-list td")
        for t in td:
            item = {}
            item["Type2"] = type
            item["Brand"] = brand
            item["Year"] = year
            item["Major"] = pq(t).find("a").html()
            item["DataUrl"] = pq(t).find("a").attr("href")
            MySql_Utility().InsertUniversity_UK_Rank_Major(item)

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
        list = MySql_Utility().GetUniversity_UK_RanksMajor()
        for l in list:
            UK_Ranks_Major.run(l["DataUrl"])
        #UK_Ranks_Major.runMajor("http://ranking.promisingedu.com/major/times/2013")
        #UK_Ranks_Major.runMajor("http://ranking.promisingedu.com/major/times/2012")
        #UK_Ranks_Major.runMajor("http://ranking.promisingedu.com/major/times/2011")
        #UK_Ranks_Major.runMajor("http://ranking.promisingedu.com/major/times/2010")
        #UK_Ranks_Major.runMajor("http://ranking.promisingedu.com/major/guardian")
        #UK_Ranks_Major.runMajor("http://ranking.promisingedu.com/major/guardian/2015")
        #UK_Ranks_Major.runMajor("http://ranking.promisingedu.com/major/guardian/2014")
        #UK_Ranks_Major.runMajor("http://ranking.promisingedu.com/major/guardian/2013")
        #UK_Ranks_Major.runMajor("http://ranking.promisingedu.com/major/guardian/2012")
        #UK_Ranks_Major.runMajor("http://ranking.promisingedu.com/major/guardian/2011")
        #UK_Ranks_Major.runMajor("http://ranking.promisingedu.com/major/guardian/2010")
        #UK_Ranks_Major.runMajor("http://ranking.promisingedu.com/major/cug")
        #UK_Ranks_Major.runMajor("http://ranking.promisingedu.com/major/qs")
        #UK_Ranks_Major.runMajor("http://ranking.promisingedu.com/major/qs/2014")
        #UK_Ranks_Major.runMajor("http://ranking.promisingedu.com/major/rae")
        #UK_Ranks_Major.runMajor("http://ranking.promisingedu.com/major/ref")
        #UK_Ranks_Major.runMajor("url")


