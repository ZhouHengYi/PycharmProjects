# -*- coding: utf-8 -*-
#---------------------------------------
#  程序: 公共池客户状态更新
#  版本: 1.0
#  作者: Henry.Zhou
#  日期: 2015-06-03
#  描述：
#       客户表：Account   类型字段：customerTypeCode
#       类型说明:
#         重点跟进[customerTypeCode:12]7天
#         有意向[customerTypeCode:5]15天
#         待开发[customerTypeCode:4]30天
#         长期跟进[customerTypeCode:10]60天
#		客户联系记录：select * from Activity where State ='2' and systemObjectTypeCode ='1'
#       关联字段:accountid
#---------------------------------------
from Account_Item import Account_Type
from Account_Item import Full_Item
from Utility.SqlServer_Utility import  SqlServer_Utility as Sql
from Utility.Log_Utility import Log_Utility as Log
import math
import datetime
import time
from Utility.DateTime_Utility import  DateTime_Utility as dt

Log = Log()

#初始中国节假日
CD = Sql().GetCalendarDay("1")
Log.Log(('已获取到中国节假日数据：').decode('utf8') + str(CD))
#初始日本节假日
JD = Sql().GetCalendarDay("2")
Log.Log(('已获取到日本节假日数据：').decode('utf8') + str(JD))
#初始客户类型
Types = []
Types.append(Account_Type(5, 15))
Types.append(Account_Type(4, 30))
Types.append(Account_Type(10, 60))
Types.append(Account_Type(12, 7))
Types.append(Account_Type(6, 1))

#安全客户数量
AnQuan = 0
#公共客户数量
GongGong = 0
#未联系客户数量
WeiLianXi = 0
#业务员数量
YeWuYuan = []
#循环客户类型
for t in Types:
    #根据客户类型获取客户数据
    cList = Sql().GetAccountByCustomerTypeCode2(t.TypeCode)
    Log.Log(('客户类型：').decode('utf8') + str(t.TypeCode) +  (' 安全天数：').decode('utf8') + str(t.Day) + (' 客户数量：').decode('utf8') + str(len(cList)))

    #循环客户数据
    for c in cList:
        flag = False
        if t.TypeCode == 6:
            Full_Item.AddQianYue(YeWuYuan,c.FullName)
        #没有联系记录
        elif c.ActivityTime != None:
            #最后一天与当天时间相差天数
            day = dt.datediff(dt,dt.datetostr(c.ActivityTime),dt.datetostr(datetime.datetime.now()))
            #相差天数集合
            listDay = dt.getDays(dt,dt.datetostr(c.ActivityTime),dt.datetostr(datetime.datetime.now()))
            #判断天数集合中是否存在节假日
            jDay = 0
            jMsg = ''
            if c.OwningBusinessUnit == '030-109b440b-b10c-46ee-bebd-176e443223ed':
                jMsg = '日本客户'
                jDay = dt.jieJiaRi(dt,listDay,JD)
            elif c.OwningBusinessUnit == '030-6354f9c0-2057-455d-bd93-871f3808c3a9':
                jMsg = '中国客户'
                jDay = dt.jieJiaRi(dt,listDay,CD)
            else:
                jMsg = '客户类型异常'

            if (day - jDay) > t.Day:
                Log.Log(('客户ID：').decode('utf8') + str(c.AccountId) + ('，姓名：').decode('utf8') + str(c.AccountName) + ('，业务员：').decode('utf8') + str(c.FullName) + ('，').decode('utf8') + jMsg + ('，最后联系时间：').decode('utf8') + str(c.ActivityTime) + ('，相差联系天数：').decode('utf8') + str(day) + ('，节假日天数：').decode('utf8') + str(jDay) + ('，超过安全天数：').decode('utf8') + str(t.Day))
            else:
                Log.Log(('客户ID：').decode('utf8') + str(c.AccountId) + ('，姓名：').decode('utf8') + str(c.AccountName) + ('，业务员：').decode('utf8') + str(c.FullName) + ('，').decode('utf8') + jMsg + ('，最后联系时间：').decode('utf8') + str(c.ActivityTime) + ('，相差联系天数：').decode('utf8') + str(day) + ('，节假日天数：').decode('utf8') + str(jDay) + ('，未超过安全天数：').decode('utf8') + str(t.Day))
                flag = True
        else:
            Log.Log(('客户ID：').decode('utf8') + str(c.AccountId) + ('，姓名：').decode('utf8') + str(c.AccountName)  + ('，业务员：').decode('utf8') + str(c.FullName)+ ('，未联系记录').decode('utf8'))
            WeiLianXi += 1
            Full_Item.AddWeiLianXi(YeWuYuan,c.FullName)

        if flag:
            Log.Log(('客户ID：').decode('utf8') + str(c.AccountId) + ('，业务员：').decode('utf8') + str(c.FullName) + ('，安全').decode('utf8'))
            AnQuan += 1
            Full_Item.AddAnQuan(YeWuYuan,c.FullName)
        else:
            GongGong += 1
            Full_Item.AddGongGong(YeWuYuan,c.FullName)

Log.Log(('安全客户数量：').decode('utf8') + str(AnQuan) + ('，公共客户数量').decode('utf8') + str(GongGong) + ('，未联系').decode('utf8') + str(WeiLianXi))
for y in YeWuYuan:
    Log.Log(('咨询员：').decode('utf8') + str(y.FullName) + ('，安全客户数量：').decode('utf8') + str(y.AnQuan) + ('，公共客户数量').decode('utf8') + str(y.GongGong) + ('，未联系').decode('utf8') + str(y.WeiLianxi) + ('，已签约').decode('utf8') + str(y.QianYue))