# -*- coding: utf-8 -*-
#---------------------------------------
#   程序：下载
#   版本：0.1
#   作者：Henry.Zhou
#   日期：2015.05.28
#   语言：Python 2.7
#   功能：下载美国大学Logo
#   http://www.sharewithu.com/dat/college/college/@sysno/logo.png
#---------------------------------------

from Utility.MySqlUtility import MySql_Utility
from Utility.DownLoadImageUtility import DownLoadImageUtility

class DownLoadLogo(object):
    @staticmethod
    def run():
        items = MySql_Utility().GetUniversity_USASysno2()
        its = []
        for item in items:
            url = 'http://www.sharewithu.com/dat/college/college/'+str(item["rSysno"])+'/logo.png'
            item["url"] = url
            item["fileName"] = "USA"
            its.append(item)

        DownLoadImageUtility.run(its)