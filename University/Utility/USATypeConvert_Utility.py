# -*- coding: utf-8 -*-
#---------------------------------------
#   程序：Split
#   版本：0.1
#   作者：Henry
#   日期：2015-05-28
#   语言：Python 2.7
#---------------------------------------
import string

class USATypeConvert_Utility(object):
    #类型转换
    #@str : "公立 本科学校"
    #@type : 1类型 2申请类型
    def Convert(self, str , type):
        str = str.strip()
        types = str.split(" ")

        if type == 1:
            if types[0] == ("公立").decode('utf8'):
                return "1"
            elif types[0] == ("私立营利").decode('utf8'):
                return "2"
            elif types[0] == ("私立非营利").decode('utf8'):
                return "3"
            else:
                return "0"
        elif type == 2:
            if types[1] == ("本科学校").decode('utf8'):
                return "1"
            elif types[1] == ("研究生院").decode('utf8'):
                return "2"
            elif types[1] == ("商学院").decode('utf8'):
                return "3"
            elif types[1] == ("海外英语培训").decode('utf8'):
                return "4"
            elif types[1] == ("社区学校").decode('utf8'):
                return "5"
            else:
                return "0"

    def Converts(self, str, type):
        str = str.strip()
        if type == 1:
            if str == ("公立").decode('utf8'):
                return "1"
            elif str == ("私立营利").decode('utf8'):
                return "2"
            elif str == ("私立非营利").decode('utf8'):
                return "3"
            else:
                return "0"
        elif type == 2:
            if str == ("本科学校").decode('utf8'):
                return "1"
            elif str == ("研究生院").decode('utf8'):
                return "2"
            elif str == ("商学院").decode('utf8'):
                return "3"
            elif str == ("海外英语培训").decode('utf8'):
                return "4"
            elif str == ("社区学校").decode('utf8'):
                return "5"
            return "0"

    def ConvertMajor(self,str):
        if str == ("商科").decode('utf8'):
            return "1"
        elif str == ("教育学").decode('utf8'):
            return "14"
        elif str == ("工程学").decode('utf8'):
            return "25"
        elif str == ("法学").decode('utf8'):
            return "38"
        elif str == ("医学").decode('utf8'):
            return "49"
        elif str == ("科学").decode('utf8'):
            return "59"
        elif str == ("图书馆与信息研究").decode('utf8'):
            return "66"
        elif str == ("社会科学和人文科学").decode('utf8'):
            return "74"
        elif str == ("医疗保健").decode('utf8'):
            return "82"
        elif str == ("公共事务").decode('utf8'):
            return "98"
        elif str == ("艺术类").decode('utf8'):
            return "108"
