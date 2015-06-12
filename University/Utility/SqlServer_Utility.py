# -*- coding: utf-8 -*-
#---------------------------------------
#   程序：SqlServer数据库 操作类
#   版本：0.1
#   作者：Henry.Zhou
#   日期：2015.05.28
#   语言：Python 2.7
#---------------------------------------
import pymssql
from Items import USA_Item
from Utility.USATypeConvert_Utility import USATypeConvert_Utility

class SqlServer_Utility(object):
    #添加美国专业基础数据
    def InsertUniversity_USAMajor(self,name,type,typeName,rurl):
        conn=pymssql.connect(host="121.40.35.102",user="sa",
        password="newegg@123",database="HeFeng_School")
        cur=conn.cursor()
        cur.execute("INSERT INTO University_USAMajor VALUES(%s ,%s,%s,%s ,1,1,'Python',getdate())",(name,type,typeName,rurl))
        #print cur.fetchall()
        conn.commit()
        cur.execute("SELECT TOP 1 Sysno FROM University_USAMajor ORDER BY 1 DESC")
        sysno = cur.fetchall()[0]
        conn.close()
        print sysno
        return sysno

    #添加美国信息
    def InsertUniversity_USA(self,item):
        universitySysno = self.InsertUniversity_USAItem(item)
        for m in item["major"].split(','):
            self.InsertUniversity_USAAndMajor(universitySysno,USATypeConvert_Utility().ConvertMajor(m))

    #添加美国学校信息
    def InsertUniversity_USAItem(self,item):
        conn=pymssql.connect(host="121.40.35.102",user="sa",
        password="newegg@123",database="HeFeng_School")
        cur=conn.cursor()
        cur.execute("INSERT INTO University_USA VALUES(%s ,%s ,%s ,%s ,%s,%s ,%s ,%s ,%s ,%s ,1,'Python',GETDATE())",(item["name"],item["englishName"],item["xiangqu"],item["shenqing"],item["zaidu"],0,item["classes"],item["requestType"],item["logo"],item["rurl"]))
        #print cur.fetchall()
        conn.commit()
        cur.execute("SELECT TOP 1 Sysno FROM University_USA ORDER BY 1 DESC")
        sysno = cur.fetchall()[0]
        conn.close()
        print ('University_USA（美国大学信息表） : ').decode('utf8'),sysno,':',item["rurl"]
        return sysno

    #添加美国大学专业信息关联表
    def InsertUniversity_USAAndMajor(self,universitySysno,major):
        conn=pymssql.connect(host="121.40.35.102",user="sa",
        password="newegg@123",database="HeFeng_School")
        cur=conn.cursor()
        cur.execute("INSERT INTO University_USAAndMajor VALUES(%s ,%s ,1,'Python',GETDATE())",(universitySysno,major))
        #print cur.fetchall()
        conn.commit()
        cur.execute("SELECT TOP 1 Sysno FROM University_USAAndMajor ORDER BY 1 DESC")
        sysno = cur.fetchall()[0]
        conn.close()
        print ('University_USAAndMajor （美国大学专业信息关联表） : ').decode('utf8'),sysno
        return sysno

    #获取美国大学Sysno更新详细数据
    def GetUniversity_USASysno(self):
        conn=pymssql.connect(host="121.40.35.102",user="sa",
        password="newegg@123",database="HeFeng_School")
        cur=conn.cursor()
        cur.execute("SELECT TOP 10000 RUrl,Sysno FROM University_USA WHERE Status = 1 ORDER BY 1 DESC")
        list = cur.fetchall()
        items = []
        for (RUrl,Sysno) in list:
            items.append(RUrl + ',' + str(Sysno))
        cur.close()
        conn.close()
        print ('获取美国大学待更新详细数据 : ').decode('utf8'),len(items)
        return items

    #更新美国学校已到学生数量
    def UpdateUniversity_USAInfo(self,universitySysno,yiyou):
        conn=pymssql.connect(host="121.40.35.102",user="sa",
        password="newegg@123",database="HeFeng_School")
        cur=conn.cursor()
        cur.execute("UPDATE University_USA SET YiYou = %s WHERE Sysno = %s",(yiyou,universitySysno))
        conn.commit()
        cur.close()
        conn.close()
        print ('美国大学待更新已到学生数量 : ').decode('utf8'),universitySysno,' : ',yiyou

    #添加美国学校扩展信息
    def InsertUniversity_USA_ExInfo(self,universitySysno,type,typeMsg,content):
        conn=pymssql.connect(host="121.40.35.102",user="sa",
        password="newegg@123",database="HeFeng_School")
        cur=conn.cursor()
        cur.execute("INSERT University_USA_Ex(UniversitySysno,Type,TypeMsg,Content,Status,InUser,InDate) VALUES(%s,%s,%s,%s,1,'Python',getdate())",(universitySysno,typeMsg,type,content))
        conn.commit()
        cur.close()
        conn.close()
        print ('美国大学添加扩展信息 : ').decode('utf8'),universitySysno,' : ',type

    #根据RUrl获取Sysno
    def ByRUrlGetUniversityUSASysno(self,rurl):
        conn=pymssql.connect(host="121.40.35.102",user="sa",
        password="newegg@123",database="HeFeng_School")
        cur=conn.cursor()
        cur.execute("SELECT TOP 1 Sysno FROM University_USA WHERE RUrl = %s ORDER BY 1 DESC",(rurl))
        sysno = cur.fetchall()[0]
        cur.close()
        conn.close()
        return sysno

    def InsertUniversity_USAMajorRanks(self,universitySysno,type,typeMsg,rank):
        conn=pymssql.connect(host="121.40.35.102",user="sa",
        password="newegg@123",database="HeFeng_School")
        cur=conn.cursor()
        cur.execute("INSERT University_USAMajorRanks(UniversitySysno,Type,TypeMsg,Rank,Status,InUser,InDate) VALUES(%s,%s,%s,%s,1,'Python',getdate())",(universitySysno,type,typeMsg,rank))
        conn.commit()
        cur.close()
        conn.close()