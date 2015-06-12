# -*- coding: utf-8 -*-
import logging
import logging.handlers  
import sys
from thread import allocate_lock
reload(sys)
sys.setdefaultencoding('utf-8')
class Log_Utility(object):

    def __init__(self):
        LOG_FILE = 'tst.log'

        handler = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes = 10240*10240, backupCount = 5) # 实例化handler
        fmt = '%(asctime)s - %(filename)s:%(lineno)s - %(name)s - %(message)s'

        formatter = logging.Formatter(fmt)   # 实例化formatter
        handler.setFormatter(formatter)      # 为handler添加formatter

        self.logger = logging.getLogger('tst')    # 获取名为tst的logger
        self.logger.addHandler(handler)           # 为logger添加handler
        self.logger.setLevel(logging.INFO)

    def Log(self,msg):
        print str(msg).decode('utf8')
        self.logger.info(msg)