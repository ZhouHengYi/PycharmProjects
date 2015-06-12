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


class MySql_Utility(object):

    def Insert_JpanList(self,values):
        try:
            conn  = MySQLdb.connect(
                host = '192.168.0.8',
                db = 'baidu_jpan',
                user = 'root',
                passwd = 'newegg@123',
                cursorclass = MySQLdb.cursors.DictCursor,
                charset = 'utf8',
                use_unicode = True,
            )
            cur = conn.cursor()
            cur.executemany("""insert into jpanlist(title,link,furl,bid,indate) values (%s, %s, %s,%s, now()) """, values)
            cur.close()
            conn.commit()
            conn.close()
        except Exception, e:
            print Exception,":",e
            #errorMsg = Exception,":",e

    def Insert_JpanItem(self,values):
        try:
            conn  = MySQLdb.connect(
                host = '192.168.0.8',
                db = 'baidu_jpan',
                user = 'root',
                passwd = 'newegg@123',
                cursorclass = MySQLdb.cursors.DictCursor,
                charset = 'utf8',
                use_unicode = True,
            )
            cur = conn.cursor()
            cur.executemany("""insert into jpanitem(bid,userid,username,userhead,userhome,contentid,content,turl,furl,publisherDate,indate) values (%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, now()) """, values)
            cur.close()
            conn.commit()
            conn.close()
        except Exception, e:
            print Exception,":",e
            #errorMsg = Exception,":",e

    def Insert_ReplyItem(self,values):
        try:
            conn  = MySQLdb.connect(
                host = '192.168.0.8',
                db = 'baidu_jpan',
                user = 'root',
                passwd = 'newegg@123',
                cursorclass = MySQLdb.cursors.DictCursor,
                charset = 'utf8',
                use_unicode = True,
            )
            cur = conn.cursor()
            cur.executemany("""insert into reply(pid,contentid,userid,content,publisherDate,indate) values (%s, %s, %s,%s, %s, now()) """, values)
            cur.close()
            conn.commit()
            conn.close()
        except Exception, e:
            print Exception,":",e
            #errorMsg = Exception,":",e