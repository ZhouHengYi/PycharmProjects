# -*- coding: utf-8 -*-
#---------------------------------------
#   程序：Redisc 操作类
#   版本：0.1
#   作者：Henry.Zhou
#   日期：2015.04.05
#   语言：Python 2.7
#   功能：Redisco 相关操作
#---------------------------------------
__author__ = 'Administrator'
import scrapy
import redis

class JRedisco_Utility(object):

    def __init__(self):
        #__init__(self, host='localhost', port=6379, db=0, passwordself=None, socket_timeout=None, connection_pool=None, charset='utf-8', errors='strict', decode_responses=False, unix_socket_path=None)
        self.r = redis.Redis(host='192.168.0.8', port=6379, db=0)   #如果设置了密码，就加上password=密码

    #添加数据
    def Insert(self,bid):
        if self.r.exists(bid):
            print 'Redis Exists:',bid
        else:
            self.r.set(bid,bid)

    #移出数据
    def Remove(self,bid):
        if self.r.exists(bid):
            self.r.delete(bid)
        else:
            print 'Redis Item Not Exists:',bid

    #随即取出一条数据并删除
    def Get(self):
        k = self.r.randomkey()
        self.r.delete(k)
        return k

    #清空数据库
    def Flushdb(self):
        self.r.flushdb()