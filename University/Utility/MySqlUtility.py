# -*- coding: utf-8 -*-
#---------------------------------------
#   程序：MySql数据库 操作类
#   版本：0.1
#   作者：Henry.Zhou
#   日期：2015.05.26
#   语言：Python 2.7
#   功能：MySql数据库 相关操作
#---------------------------------------
__author__ = 'Administrator'

import MySQLdb
import MySQLdb.cursors

from USATypeConvert_Utility import USATypeConvert_Utility
from Log_Utility import Log_Utility
from Items.USA_Item import USAUniversity_Itemsn
import json
class MySql_Utility(object):
    Log = Log_Utility()

    def GetConnection(self):
        return MySQLdb.connect(host = '192.168.0.8',
                db = 'test2',
                user = 'root',
                passwd = 'newegg@123',
                cursorclass = MySQLdb.cursors.DictCursor,
                charset = 'utf8',
                use_unicode = True,)
    #添加美国专业基础数据
    def InsertUniversity_USAMajor(self,name,type,typeName,rurl):
        conn=MySql_Utility.GetConnection(self)
        cur=conn.cursor()
        cur.execute("INSERT INTO university_major(country,rsysno,name,type,typename,content,status,inuser,indate) VALUES('usa' ,%s,%s,%s ,%s ,'usa' ,1,'Python',now())",(rurl,name,type,typeName))
        #print cur.fetchall()
        conn.commit()
        cur.execute("SELECT sysno FROM university_major ORDER BY 1 DESC LIMIT 1")
        sysno = cur.fetchall()[0]
        conn.close()
        print sysno
        return sysno

     #添加美国信息
    def InsertUniversity_USA(self,item):
        universitySysno = self.InsertUniversity_USAItem(item)
        for m in item["major"].split(','):
            self.InsertUniversity_USAAndMajor(universitySysno["sysno"],m)

    #添加美国学校信息
    def InsertUniversity_USAItem(self,item):
        conn=MySql_Utility.GetConnection(self)
        cur=conn.cursor()
        cur.execute("INSERT INTO university(rsysno,country,name,englishername,content,status,inuser,indate) VALUES(%s ,%s ,%s ,%s ,%s ,1,'Python',now())",(item["rurl"],'usa',item["name"],item["englishName"],item))
        #print cur.fetchall()
        conn.commit()
        cur.execute("SELECT sysno FROM university ORDER BY 1 DESC LIMIT 1")
        sysno = cur.fetchall()[0]
        conn.close()
        self.Log.Log('University_USA（美国大学信息表） : '+ str(sysno["sysno"]) + ':'+item["rurl"])
        return sysno

    #添加美国大学专业信息关联表
    def InsertUniversity_USAAndMajor(self,universitySysno,major):
        conn=MySql_Utility.GetConnection(self)
        cur=conn.cursor()
        cur.execute("INSERT INTO university_andmajor(universitysysno,name,status,inuser,indate) VALUES(%s ,%s ,1,'Python',now())",(universitySysno,major))
        #print cur.fetchall()
        conn.commit()
        cur.execute("SELECT sysno FROM university_andmajor ORDER BY 1 DESC LIMIT 1")
        sysno = cur.fetchall()[0]
        conn.close()
        self.Log.Log('University_USAAndMajor （美国大学专业信息关联表） : '+str(sysno["sysno"]))
        return sysno

   #获取美国大学Sysno更新详细数据
    def GetUniversity_USASysno(self):
        conn=MySql_Utility.GetConnection(self)
        cur=conn.cursor()
        cur.execute("SELECT rSysno,Sysno FROM university WHERE Status = 1 and Country = 'usa' ORDER BY 1 DESC")
        list = cur.fetchall()
        items = []
        for (rSysno,Sysno) in list:
            items.append(rSysno + ',' + str(Sysno))
        cur.close()
        conn.close()
        print ('获取美国大学待更新详细数据 : ').decode('utf8'),len(items)
        return items

    #更新美国学校已到学生数量
    def UpdateUniversity_USAInfo(self,universitySysno,yiyou):
        conn=MySql_Utility.GetConnection(self)
        cur=conn.cursor()
        cur.execute("UPDATE university SET YiYou = %s WHERE Sysno = %s",(yiyou,universitySysno))
        conn.commit()
        cur.close()
        conn.close()
        print ('美国大学待更新已到学生数量 : ').decode('utf8'),universitySysno,' : ',yiyou

    #添加美国学校扩展信息
    def InsertUniversity_USA_ExInfo(self,universitySysno,type,typeMsg,content):
        conn=MySql_Utility.GetConnection(self)
        cur=conn.cursor()
        cur.execute("INSERT university_exinfo(universitysysno,type,typeMsg,content,status,inuser,indate) VALUES(%s,%s,%s,%s,1,'Python',getdate())",(universitySysno,typeMsg,type,content))
        conn.commit()
        cur.close()
        conn.close()
        print ('美国大学添加扩展信息 : ').decode('utf8'),universitySysno,' : ',type

    #根据RUrl获取Sysno
    def ByRUrlGetUniversityUSASysno(self, rurl):
        conn = MySql_Utility.GetConnection(self)
        cur = conn.cursor()
        cur.execute("SELECT NULLIF(Sysno,-1) AS Sysno FROM university WHERE rsysno = %s and country = 'usa' ORDER BY 1 DESC LIMIT 1",(rurl))
        result= cur.fetchall()
        sysno = -1
        if len(result) > 0:
            sysno = result[0]["Sysno"]
            #print sysno
        cur.close()
        conn.close()
        return sysno

    def ByRUrlGetUniversityUSAInfo(self, rurl):
        conn = MySql_Utility.GetConnection(self)
        cur = conn.cursor()
        cur.execute("SELECT NULLIF(Sysno,-1) AS Sysno,content FROM university WHERE rsysno = %s and country = 'usa' ORDER BY 1 DESC LIMIT 1",(rurl))
        result= cur.fetchall()
        entity = USAUniversity_Itemsn(rurl,"",0)
        if len(result) > 0:
            sysno = result[0]["Sysno"]
            constr = result[0]["content"]
            print constr
            content = json.dumps(constr)
            centity = json.loads(content)
            print centity["zaidu"]
            #print content
            entity.sysno = sysno
            entity.Items =content
            #print sysno
        cur.close()
        conn.close()
        return entity

    def InsertUniversity_USAMajorRanks(self,universitySysno,year,type,typeMsg,rank):
        conn=MySql_Utility.GetConnection(self)
        cur=conn.cursor()
        cur.execute("INSERT university_ranks(country,universitysysno,year,type,typeMsg,rank,Status,InUser,InDate) VALUES('usa',%s,%s,%s,%s,%s,1,'Python',now())",(universitySysno,year,type,typeMsg,rank))
        conn.commit()
        cur.close()

    #获取专业信息
    def GetUniversityMajorInfo(self):
        conn=MySql_Utility.GetConnection(self)
        cur=conn.cursor()
        cur.execute("SELECT name,typename,rsysno,sysno FROM university_major WHERE rsysno <> 0")
        list = cur.fetchall()

        cur.close()
        conn.close()
        print ('获取美国专业数据 : ').decode('utf8'),len(list)
        return list

    #更新美国学校费用
    def UpdateUniversity_USARanking(self,content,universitySysno):
        conn=MySql_Utility.GetConnection(self)
        cur=conn.cursor()
        cur.execute("UPDATE university SET content = %s WHERE Sysno = %s",(content,universitySysno))
        conn.commit()
        cur.close()
        conn.close()
        print ('美国大学学校费用 : ').decode('utf8'),universitySysno,' : ',content