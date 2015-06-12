
# -*- coding: utf-8 -*-

from Utility.USATypeConvert_Utility import USATypeConvert_Utility
from Utility.MySqlUtility import MySql_Utility
from Utility.Log_Utility import Log_Utility
from Items.USA_Item import USAUniversity_Items

import urllib2
from pyquery import PyQuery as pq
import string
import sys
#sys.stdout = open('usa_output.txt', 'w')

class USA_University(object):
    @staticmethod
    def parse(url):
        myPage = urllib2.urlopen(url).read()
        p = pq(myPage)
        list = p.find("div.cf_college")
        #print myPage
        for u in list:
            #try:
                item = USAUniversity_Items()
                item["name"] = pq(u).find(".cf_c_n_name").html().decode('utf8')
                item["englishName"] = pq(u).find(".cf_c_n_englishname").html().decode('utf8')
                item["xiangqu"] = pq(u).find(".cf_c_number").eq(0).html()
                item["shenqing"] = pq(u).find(".cf_c_number").eq(1).html()
                item["zaidu"] = pq(u).find(".cf_c_number").eq(2).html()
                item["logo"] = pq(u).find(".cf_c_image").attr("src")
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
                    majorStr += pq(m).html() + ","
                if len(majorStr) > 0:
                    majorStr = majorStr[0:len(majorStr) - 1]
                item["major"] = majorStr
                #print item
                MySql_Utility().InsertUniversity_USA(item)
            #except Exception, e:
            #    print(str(e).decode('utf8'))