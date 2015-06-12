# -*- coding: utf-8 -*-
import datetime
import time
from Utility.Log_Utility import Log_Utility as Log

class DateTime_Utility(object):
    #将字符串转换成datetime类型
    @staticmethod
    def strtodatetime(datestr ,format):
        return datetime.datetime.strptime(datestr,format)

    #时间转换成字符串,格式为2008-08-02
    @staticmethod
    def datetostr(date):
        return str(date)[0:10]

    #两个日期相隔多少天，例：2008-10-03和2008-10-01是相隔两天
    @staticmethod
    def datediff(self, beginDate,endDate):
        format = "%Y-%m-%d";
        bd = self.strtodatetime(beginDate,format)
        ed = self.strtodatetime(endDate,format)
        oneday = datetime.timedelta(days=1)
        count = 0
        while bd != ed:
            ed=ed-oneday
            count+=1
        return count
    #获取两个时间段的所有时间,返回list
    @staticmethod
    def getDays(self, beginDate, endDate):
        format = "%Y-%m-%d";
        bd = self.strtodatetime(beginDate, format)
        ed = self.strtodatetime(endDate, format)
        oneday = datetime.timedelta(days=1)
        num = self.datediff(self,beginDate, endDate)+1
        li = []
        for i in range(0, num):
            li.append(self.datetostr(ed))
            ed = ed-oneday
        return li
    @staticmethod
    def jieJiaRi(self,listA,listB):
        dCount = 0
        for b in listB:
            #rint b[0]
            if b[0] in set(listA):
                #Log().Log(('包含节假日：').decode('utf8') + str(b))
                dCount+=1
        return dCount