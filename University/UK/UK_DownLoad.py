# -*- coding: utf-8 -*-
#---------------------------------------
#   程序：下载
#   版本：0.1
#   作者：Henry.Zhou
#   日期：2015.05.28
#   语言：Python 2.7
#   功能：下载美国大学Logo
#---------------------------------------
import json
from Utility.MySqlUtility import MySql_Utility
from Utility.DownLoadImageUtility import DownLoadImageUtility

class DownLoadLogo(object):
    @staticmethod
    def run():
        items = MySql_Utility().GetUniversity_UKInfo()

        task_threads=[]
        p = 1
        its = []
        for item in items:
            jEntity = json.loads(item["content"])
            item["url"] = jEntity["logo"].encode("utf-8")
            item["fileName"] = "UK"
            its.append(item)

        DownLoadImageUtility.run(its)
