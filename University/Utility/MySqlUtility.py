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
import Items.USA_Item
import Utility.JsonUtility
import json
class MySql_Utility(object):
    Log = Log_Utility()

    def GetConnection(self):
        return MySQLdb.connect(host = '120.26.109.47',
                db = 'bigtreedb',
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
        cur.execute("SELECT id FROM university_major ORDER BY 1 DESC LIMIT 1")
        sysno = cur.fetchall()[0]
        conn.close()
        print sysno
        return sysno

     #添加美国信息
    def InsertUniversity_USA(self,item):
        universitySysno = self.InsertUniversity_USAItem(item)
        for m in item["major"].split(','):
            self.InsertUniversity_USAAndMajor(universitySysno["id"],m)

    #添加美国学校信息
    def InsertUniversity_USAItem(self,item):
        conn=MySql_Utility.GetConnection(self)
        cur=conn.cursor()
        cur.execute("INSERT INTO university(rsysno,country,name,englishername,content,Logo,status,inuser,indate) VALUES(%s ,%s ,%s ,%s ,%s,%s ,1,'Python',now())",(item["rurl"],'usa',item["name"],item["englishName"],json.dumps(item),''))
        #print cur.fetchall()
        conn.commit()
        cur.execute("SELECT id FROM university ORDER BY 1 DESC LIMIT 1")
        sysno = cur.fetchall()[0]
        conn.close()
        self.Log.Log('University_USA（美国大学信息表） : '+ str(sysno["id"]) + ':'+item["rurl"])
        return sysno

    #添加美国大学专业信息关联表
    def InsertUniversity_USAAndMajor(self,universitySysno,major):
        conn=MySql_Utility.GetConnection(self)
        cur=conn.cursor()
        cur.execute("INSERT INTO university_andmajor(universitysysno,name,status,inuser,indate) VALUES(%s ,%s ,1,'Python',now())",(universitySysno,major))
        #print cur.fetchall()
        conn.commit()
        cur.execute("SELECT id FROM university_andmajor ORDER BY 1 DESC LIMIT 1")
        sysno = cur.fetchall()[0]
        conn.close()
        self.Log.Log('University_USAAndMajor （美国大学专业信息关联表） : '+str(sysno["id"]))
        return sysno

   #获取美国大学Sysno更新详细数据
    def GetUniversity_USASysno(self):
        conn=MySql_Utility.GetConnection(self)
        cur=conn.cursor()
        cur.execute("SELECT rSysno,id FROM university WHERE Status = 1 and Country = 'usa' ORDER BY 1 DESC")
        list = cur.fetchall()
        cur.close()
        conn.close()
        print ('获取美国大学待更新详细数据 : ').decode('utf8'),len(list)
        return list
    #获取美国大学Sysno更新详细数据
    def GetUniversity_USASysno2(self):
        conn=MySql_Utility.GetConnection(self)
        cur=conn.cursor()
        cur.execute("SELECT rSysno,id,englisherName FROM university WHERE Status = 1 and Country = 'usa' ORDER BY 1 DESC")
        list = cur.fetchall()
        cur.close()
        conn.close()
        print ('获取美国大学待更新详细数据 : ').decode('utf8'),len(list)
        return list
    #更新美国学校已到学生数量
    def UpdateUniversity_USAInfo(self,universitySysno,yiyou):
        conn=MySql_Utility.GetConnection(self)
        cur=conn.cursor()
        cur.execute("UPDATE university SET YiYou = %s WHERE Id = %s",(yiyou,universitySysno))
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
        cur.execute("SELECT NULLIF(Id,-1) AS Sysno FROM university WHERE rsysno = %s and country = 'usa' ORDER BY 1 DESC LIMIT 1",(rurl))
        result= cur.fetchall()
        sysno = -1
        if len(result) > 0:
            sysno = result[0]["Id"]
            #print sysno
        cur.close()
        conn.close()
        return sysno

    def ByRUrlGetUniversityUSAInfo(self, rurl):
        conn = MySql_Utility.GetConnection(self)
        cur = conn.cursor()
        cur.execute("SELECT NULLIF(Id,-1) AS Sysno,content FROM university WHERE rsysno = %s and country = 'usa' ORDER BY 1 DESC LIMIT 1",(rurl))
        result= cur.fetchall()
        entity = USAUniversity_Itemsn(rurl,"",0)
        if len(result) > 0:
            sysno = result[0]["Id"]
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
        cur.execute("SELECT name,typename,rsysno,Id FROM university_major WHERE rsysno <> 0")
        list = cur.fetchall()

        cur.close()
        conn.close()
        print ('获取美国专业数据 : ').decode('utf8'),len(list)
        return list

    #更新美国学校费用
    def UpdateUniversity_USARanking(self,content,universitySysno):
        conn=MySql_Utility.GetConnection(self)
        cur=conn.cursor()
        cur.execute("UPDATE university SET content = %s WHERE Id = %s",(content,universitySysno))
        conn.commit()
        cur.close()
        conn.close()
        print ('美国大学学校费用 : ').decode('utf8'),universitySysno,' : ',content

    #更新美国学校Logo
    def UpdateUniversity_USALogo(self,universitySysno,logo,):
        conn=MySql_Utility.GetConnection(self)
        cur=conn.cursor()
        cur.execute("UPDATE university SET Logo = %s WHERE Id = %s",(logo,universitySysno))
        conn.commit()
        cur.close()
        conn.close()
        print ('更新美国学校Logo : ').decode('utf8'),universitySysno,' : ',logo

    #根据名称获取Sysno
    def ByNameGetUniversityUKSysno(self, name):
        conn = MySql_Utility.GetConnection(self)
        cur = conn.cursor()
        cur.execute("SELECT NULLIF(Id,-1) AS Id FROM university_uk WHERE name = %s ORDER BY 1 DESC LIMIT 1",(name))
        result= cur.fetchall()
        sysno = -1
        if len(result) > 0:
            sysno = result[0]["Id"]
            #print sysno
        cur.close()
        conn.close()
        return sysno

    #添加英国学校信息
    def InsertUniversity_UK(self,item):
        conn=MySql_Utility.GetConnection(self)
        cur=conn.cursor()
        cur.execute("INSERT INTO university_uk(rsysno,country,name,englishname,content,Logo,status,inuser,indate) VALUES(%s ,%s ,%s ,%s ,%s,%s ,1,'Python',now())",(item.rurl,'uk',item.name,item.englishName,json.dumps(JsonUtility.object2dict(item)),''))
        #print cur.fetchall()
        conn.commit()
        cur.execute("SELECT id FROM university_uk ORDER BY 1 DESC LIMIT 1")
        sysno = cur.fetchall()[0]
        conn.close()
        self.Log.Log('University_UK（英国大学信息表） : '+ str(sysno["id"]) + ':'+item.rurl)
        return sysno

    #获取英国大学Sysno更新详细数据
    def GetUniversity_UKInfo(self):
        conn=MySql_Utility.GetConnection(self)
        cur=conn.cursor()
        cur.execute("SELECT content,englishname,id FROM university_uk WHERE Status = 1 and Logo = '' ORDER BY 1 DESC")
        list = cur.fetchall()
        cur.close()
        conn.close()
        print ('获取美国大学待更新详细数据 : ').decode('utf8'),len(list)
        return list

    #更新英国学校Logo
    def UpdateUniversity_UKLogo(self,universitySysno,logo):
        conn=MySql_Utility.GetConnection(self)
        cur=conn.cursor()
        cur.execute("UPDATE university_uk SET Logo = %s WHERE Id = %s",(logo,universitySysno))
        conn.commit()
        cur.close()
        conn.close()
        print ('更新英国学校Logo : ').decode('utf8'),universitySysno,' : ',logo

    #英国学校排名
    def InsertUniversity_UK_Rank(self,item):
        try:
            conn=MySql_Utility.GetConnection(self)
            cur=conn.cursor()
            cur.execute("INSERT INTO university_uk_ranks(EnglishName,UniversityName,Ranking,Type,Brand,Year,Major,MajorUrl,Content,status,inuser,indate) VALUES( %s,%s, %s,%s,%s,%s,%s,%s,%s,%s,'Python',now())",
                        (item["EnglishName"],str(item["UniversityName"]).decode("utf8"),item["Ranking"],item["Type2"],item["Brand"],item["Year"],item["Major"],item["MajorUrl"],json.dumps(item),item["status"]))
            conn.commit()
            cur.close()
            conn.close()
            print ('英国学校排名 ').decode('utf8') + item["UniversityName"]
        except Exception, e:
            print(e + ' Item:' + item)

    #根据英文名称查找英国学校名称
    def ByEnglishNameGetUniversity_UKName(self,englishName):
        conn=MySql_Utility.GetConnection(self)
        cur=conn.cursor()
        print englishName
        cur.execute("SELECT name,englishName FROM university_uk WHERE Status = 1 and englishName = %s ORDER BY 1 DESC",(englishName))
        list = cur.fetchall()
        cur.close()
        conn.close()
        print str('根据英文名称查找英国学校名称 : ').decode('utf8') + str(list).decode('utf8')
        return list

    #根据英文名称查找英国学校名称
    def InsertUniversity_UK_Rank_Major(self,item):
        conn=MySql_Utility.GetConnection(self)
        cur=conn.cursor()
        cur.execute("INSERT INTO university_uk_majoranks(Type,Brand,Year,Major,DataUrl,status,inuser,indate) VALUES( %s,%s, %s,%s,%s,1,'Python',now())",(item["Type2"],item["Brand"],item["Year"],item["Major"],item["DataUrl"]))
        conn.commit()
        cur.close()
        conn.close()
        print str('添加英国专业排名数据 : ').decode('utf8') + str(item)
        return list

    #根据英文名称查找英国学校名称
    def GetUniversity_UK_RanksMajor(self):
        conn=MySql_Utility.GetConnection(self)
        cur=conn.cursor()
        cur.execute("SELECT DataUrl FROM university_uk_majoranks WHERE Status = 1 and id >= 291")
        list = cur.fetchall()
        cur.close()
        conn.close()
        print str('获取英国专业排名链接 : ').decode('utf8') + str(list).decode('utf8')
        return list