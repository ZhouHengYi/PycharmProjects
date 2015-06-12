__author__ = 'henry'
import urllib2
from pyquery import PyQuery as pq
class USA_Article_Spider(object):

    def run(self,url):
        myPage = urllib2.urlopen(url).read()
        p = pq(myPage)
        list = p.find("ul.news")
        for item in list:
