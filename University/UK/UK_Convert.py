
# -*- coding: utf-8 -*-
__author__ = 'henry'
class UK_Convert(object):
    def Converts(self, str):
        str = str.strip()
        if str == ("公立").decode('utf8'):
            return "1"
        elif str == ("私立").decode('utf8'):
            return "2"
        else:
            return "0"

    def ConvertEnglishName(self,englishName):
        return str(englishName).replace(" ","_")

    def ConvertEnglishName2(self,englishName):
        return str(englishName).replace("-"," ")