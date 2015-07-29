
from Utility.ThreadUtility import ThreadUtility
import urllib
from UK.UK_Convert import UK_Convert
import os
from Utility.MySqlUtility import MySql_Utility

class DownLoadImageUtility(object):
    @staticmethod
    def run(items):
        task_threads=[]
        p = 1
        for item in items:
            url = item["url"]
            englisherName = item["englishname"]
            id = item["id"]
            fileName = item["fileName"]
            t = ThreadUtility(p, DownLoadImageUtility.down(url,englisherName,id,fileName))

            p = p + 1
            task_threads.append(t)

        for task in task_threads:
            task.start()
        for task in task_threads:
            task.stop()
        print("Over")

    @staticmethod
    def down(url,englisherName,id,fileName):
        try:
            print("fangwen:"+url)

            web = urllib.urlopen(url.encode("utf-8"))
            jpg = web.read()
            name =UK_Convert().ConvertEnglishName(englisherName)
            DstDir="/Users/henry/git/"+fileName+"/" + name + "/"

            if(os.path.exists(DstDir)!=True):
                os.makedirs(DstDir)

            FileName = DstDir + "Logo.png"
            print("Save "+FileName)
            File = open(FileName,"wb" )
            File.write(jpg)
            File.close()
            if fileName == "USA":
                MySql_Utility().UpdateUniversity_USALogo(id,(fileName + "\\" + name + '\\Logo.png'))
            elif fileName == "UK":
                MySql_Utility().UpdateUniversity_UKLogo(id,(fileName + "\\" + name + '\\Logo.png'))
            print '\n'
        except Exception:
            print '****************Down Error******************\n'
