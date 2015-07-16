# -*- coding: utf-8 -*-
#---------------------------------------
#   程序：抓取英国学校数据
#   版本：0.1
#   作者：Henry.Zhou
#   日期：2015.06.18
#   语言：Python 2.7
#   http://school.promisingedu.com/unilist.aspx?page=1&ranks=&k=&c=0&x=&t=&typeid=1&City=&letter=&SortBy=Rank
#---------------------------------------
from UK.UK_University import UK_University
from Utility.ThreadUtility import ThreadUtility
from UK.UK_DownLoad import DownLoadLogo
from UK.UK_Ranks import UK_Ranks
from UK.UK_Ranks_Major import UK_Ranks_Major
import threading
#-------- 程序入口处 ------------------
print u"""#---------------------------------------
#   程序：抓取英国学校数据
#   版本：0.2
#   作者：Henry.Zhou
#   日期：2015.06.18
#   语言：Python 2.7
#   1. 大学数据
#   2. 大学Logo
#   3. 英国大学排名
#   4. 英国大学专业排名
#   0. 1-5
#---------------------------------------
"""

print u'请输入要执行命令编号：'


id = raw_input()
task_threads=[] #存储线程
if int(id) == 1:
    p = 1
    while p <= 34:
        url = 'http://school.promisingedu.com/unilist.aspx?page='+str(p)+'&ranks=&k=&c=0&x=&t=&typeid=1&City=&letter=&SortBy=Rank'
        t = ThreadUtility(p,UK_University.parse(url))
        p = p+1
        task_threads.append(t)
    for task in task_threads:
        task.start()
    for task in task_threads:
        task.stop() #等待所有线程结束
    print("已经完成所有任务")
elif int(id) == 2:
    #抓取大学Logo
    DownLoadLogo.run()
elif int(id) == 3:
    #抓取大学Logo
    UK_Ranks.getRanks()
elif int(id) == 4:
    #抓取大学Logo
    UK_Ranks_Major.getRanks()