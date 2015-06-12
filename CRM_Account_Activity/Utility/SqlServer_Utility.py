# -*- coding: utf-8 -*-
#---------------------------------------
#   程序：SqlServer数据库 操作类
#   版本：0.1
#   作者：Henry.Zhou
#   日期：2015.06.03
#   语言：Python 2.7
#---------------------------------------
import pymssql
from Account_Item import Account_Item

class SqlServer_Utility(object):

    # 根据类型获取节假日
    def GetCalendarDay(self, type):
        self.conn = pymssql.connect(host="203.156.197.157",user="sa",password="newegg@123",database="wisecrm_nbs")
        self.cur = self.conn.cursor()
        self.cur.execute("SELECT DISTINCT [start] FROM [wisecrm_nbs].[dbo].[calendar] WHERE title = %s", (type))
        list = self.cur.fetchall()
        items = []
        for (start) in list:
            items.append(start)
        self.cur.close()
        self.conn.close()
        return items

    # 根据客户类型获取客户信息
    def GetAccountByCustomerTypeCode(self, typeCode):
        self.conn = pymssql.connect(host="203.156.197.157",user="sa",password="newegg@123",database="wisecrm_nbs")
        self.cur = self.conn.cursor()
        self.cur.execute("SELECT AccountId,AccountName,CustomerTypeCode,OwningBusinessUnit FROM Account WHERE SystemObjectTypeCode ='1' AND CustomerTypeCode = %s", (typeCode))
        list = self.cur.fetchall()
        items = []
        for (AccountId, AccountName, CustomerTypeCode, OwningBusinessUnit) in list:
            item = Account_Item()
            item.AccountId = AccountId
            item.AccountName = AccountName
            item.CustomerTypeCode = CustomerTypeCode
            item.OwningBusinessUnit = OwningBusinessUnit
            items.append(item)
        self.cur.close()
        self.conn.close()
        return items

    # 根据客户类型获取客户信息
    def GetAccountByCustomerTypeCode2(self, typeCode):
        self.conn = pymssql.connect(host="203.156.197.157",user="sa",password="newegg@123",database="wisecrm_nbs")
        self.cur = self.conn.cursor()
        self.cur.execute("SELECT AccountId,AccountName,CustomerTypeCode,(SELECT TOP 1 CreatedOn FROM Activity WHERE State ='2' AND systemObjectTypeCode ='1' AND AccountId = Account.AccountId ORDER BY CreatedOn DESC) AS ActivityTime ,OwningBusinessUnit,(SELECT FullName FROM [User] WHERE UserId = Account.OwningUser) AS FullName FROM Account WHERE SystemObjectTypeCode ='1' AND isDeleted <> 1 AND CustomerTypeCode = %s", (typeCode))
        list = self.cur.fetchall()
        items = []
        for (AccountId,AccountName,CustomerTypeCode,ActivityTime,OwningBusinessUnit,FullName) in list:
            item = Account_Item()
            item.AccountId = AccountId
            item.AccountName = AccountName
            item.CustomerTypeCode = CustomerTypeCode
            item.ActivityTime = ActivityTime
            item.OwningBusinessUnit = OwningBusinessUnit
            item.FullName = FullName
            items.append(item)
        self.cur.close()
        self.conn.close()
        return items
