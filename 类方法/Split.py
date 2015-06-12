# -*- coding: utf-8 -*-
#---------------------------------------
#   程序：Split
#   版本：0.5
#   作者：Henry
#   日期：2015-05-28
#   语言：Python 2.7
#---------------------------------------
import string

class USATypeConvert(object):
    #类型转换
    #@str : "公立 本科学校"
    #@type : 1类型 2申请类型
    def Convert(self, str , type):
        types = str.split(" ")
        if type == 1:
            if types[0] == "公立":
                return "1"
            elif types[0] == "私立盈利":
                return "2"
            elif types[0] == "私立非盈利":
                return "3"
        elif type == 2:
            if types[1] == "本科学校":
                return "1"
            elif types[1] == "研究生院":
                return "2"
            elif types[1] == "商学院":
                return "3"
            elif types[1] == "海外英语培训":
                return "4"
            elif types[1] == "社区学校":
                return "5"

print USATypeConvert().Convert("私立盈利 商学院",2)