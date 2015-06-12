# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

class USAUniversity_ItemBasic(object):
    name = ''
    englishName =''
    xiangqu =''
    shenqing = ''
    zaidu = ''
    logo = ''
    classes = ''
    requestType = ''
    major = ''
    rurl = ''
    ranking = ''
    sysno = 0

class USAUniversity_Items(USAUniversity_ItemBasic):
    def __init__(self):
        pass

    def __init__(self):
        self.name = ""
        self.englishName = ""
        self.xiangqu = ""
        self.shenqing = ""
        self.zaidu = ""
        self.logo = ""
        self.classes = ""
        self.requestType = ""
        self.major = ""
        self.rurl = ""
        self.ranking = ""
        self.sysno = 0
    pass

    @staticmethod
    def object2dict(obj):
        d = {}
        d['__class__'] = obj.__class__.__name__
        d['__module__'] = obj.__module__
        d.update(obj.__dict__)
        return d

    @staticmethod
    def dict2object(d):
        #convert dict to object
        if'__class__' in d:
            class_name = d.pop('__class__')
            module_name = d.pop('__module__')
            module = __import__(module_name)
            class_ = getattr(module,class_name)
            args = dict((key.encode('ascii'), value) for key, value in d.items()) #get args
            inst = class_(**args) #create new instance
        else:
            inst = d
        return inst

class USAUniversity_Itemsn(object):
    def __init__(self):
        pass

    def __init__(self,rurl,ranking,sysno):
        self.name = ""
        self.englishName = ""
        self.xiangqu = ""
        self.shenqing = ""
        self.zaidu = ""
        self.logo = ""
        self.classes = ""
        self.requestType = ""
        self.major = ""
        self.rurl = rurl
        self.ranking = ranking
        self.sysno = sysno

    Items = USAUniversity_Items()

    @staticmethod
    def object2dict(obj):
        d = {}
        d['__class__'] = obj.__class__.__name__
        d['__module__'] = obj.__module__
        d.update(obj.__dict__)
        return d

    @staticmethod
    def dict2object(d):
        #convert dict to object
        if'__class__' in d:
            class_name = d.pop('__class__')
            module_name = d.pop('__module__')
            module = __import__(module_name)
            class_ = getattr(module,class_name)
            args = dict((key.encode('ascii'), value) for key, value in d.items()) #get args
            inst = class_(**args) #create new instance
        else:
            inst = d
        return inst

    def dict2object(d):
        #convert dict to object
        if'__class__' in d:
            class_name = d.pop('__class__')
            module_name = d.pop('__module__')
            module = __import__(module_name)
            class_ = getattr(module,class_name)
            args = dict((key.encode('ascii'), value) for key, value in d.items()) #get args
            inst = class_(**args) #create new instance
        else:
            inst = d
        return inst

class ContentItem(object):
    def __init__(self,classes,englishName,logo,major,name,requestType,rurl,shenqing,xiangqu,zaidu):
        self.classes = classes
        self.engloshName = englishName
        self.logo = logo
        self.major = major
        self.name = name
        self.requestType = requestType
        self.rurl = rurl
        self.shenqing = shenqing
        self.xiangqu = xiangqu
        self.zaidu = zaidu