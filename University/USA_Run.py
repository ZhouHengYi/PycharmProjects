# -*- coding: utf-8 -*-
#---------------------------------------
#   程序：抓取美国学校数据
#   版本：0.1
#   作者：Henry.Zhou
#   日期：2015.05.28
#   语言：Python 2.7
#   功能：抓取尚友美国学校数据
#   http://www.sharewithu.com/college/search.php?country=20001
#---------------------------------------
from University.USA_University import USA_University
from University.USA_Major import USA_Major
from University.USA_Ranks import USA_Ranks
#-------- 程序入口处 ------------------
print u"""#---------------------------------------
#   程序：抓取美国学校排名相关数据
#   版本：0.2
#   作者：Henry.Zhou
#   日期：2015.06.12
#   语言：Python 2.7
#   1. 美国大学数据
#   2. 美国专业数据
#   3. 美国排名数据
#   0. 1-7
#---------------------------------------
"""
print u'请输入要执行命令编号：'
#id = raw_input()
id=3
if int(id) == 1:
    #抓取美国大学数据
    p = 1
    while p <= 230:
        url = 'http://www.sharewithu.com/college/search.php?country=20001&p='+str(p)
        USA_University.parse(url)
        p = p+1
elif int(id) == 2:
    #抓取美国专业数据
    USA_Major.parse("http://www.sharewithu.com/college/ranks.php")
elif int(id) == 3:
    #抓取美国排名数据
    USA_Ranks.run()