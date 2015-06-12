# -*- coding: utf-8 -*-
#---------------------------------------
#   程序：客户表
#   版本：0.1
#   作者：Henry.Zhou
#   日期：2015.06.03
#   语言：Python 2.7
#---------------------------------------

class Account_Item(object):
    # 客户标识
    AccountId = ''

    # 客户名称
    AccountName = ''

    # 客户类型
    CustomerTypeCode = 0

    # 最后一次联系时间
    ActivityTime = ''

    #部门  030-109b440b-b10c-46ee-bebd-176e443223ed  上海   030-6354f9c0-2057-455d-bd93-871f3808c3a9  日本
    OwningBusinessUnit = ''

    #业务员名称
    FullName = ''

class Account_Type(object):
    # 类型
    TypeCode = 0

    # 联系天数
    Day = 0

    def __init__(self, code , day):
        self.TypeCode = code
        self.Day = day

class Full_Item(object):
    FullName = ''

    WeiLianxi = 0

    AnQuan = 0

    GongGong = 0

    QianYue = 0

    @staticmethod
    def AddWeiLianXi(list,fullName):
        flag = False
        for item in list:
            if item.FullName == fullName:
                item.WeiLianxi += 1
                flag = True
        if flag == False:
            fi = Full_Item()
            fi.FullName = fullName
            fi.WeiLianxi = 1
            list.append(fi)
    @staticmethod
    def AddAnQuan(list,fullName):
        flag = False
        for item in list:
            if item.FullName == fullName:
                item.AnQuan += 1
                flag = True
        if flag == False:
            fi = Full_Item()
            fi.FullName = fullName
            fi.AnQuan = 1
            list.append(fi)
    @staticmethod
    def AddGongGong(list,fullName):
        flag = False
        for item in list:
            if item.FullName == fullName:
                item.GongGong += 1
                flag = True
        if flag == False:
            fi = Full_Item()
            fi.FullName = fullName
            fi.GongGong = 1
            list.append(fi)

    @staticmethod
    def AddQianYue(list,fullName):
        flag = False
        for item in list:
            if item.FullName == fullName:
                item.QianYue += 1
                flag = True
        if flag == False:
            fi = Full_Item()
            fi.FullName = fullName
            fi.QianYue = 1
            list.append(fi)

