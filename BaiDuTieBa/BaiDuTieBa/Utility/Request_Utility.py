__author__ = 'Administrator'

class Request_Utility(object):
    def Get_Request_Headers(self):
        headers = {
        "Accept": "text/plain, */*; q=0.01",
        "Accept-Encoding": "gzip, deflate, sdch",
        "Accept-Language": "zh-CN,zh;q=0.81",
        "Connection": "keep-alive",
        "Content-Type":" application/x-www-form-urlencoded; charset=UTF-8",
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.89 Safari/537.36",
        "Referer": "http://tieba.baidu.com/",
        "Cookie" : "BAIDUID=196F1B10BCBA7DC3FA8B9B21DDB0C856:FG=1; BIDUPSID=9FB0464A296F82CA31AFC9BAD53C3757; BDRCVFR[TXZ8BX1B2fs]=mk3SLVN4HKm; TIEBA_USERTYPE=856a30f6db8a544e4f061c84; TIEBAUID=c9d8190770e5388957236bf3; BDRCVFR[feWj1Vr5u3D]=I67x6TjHwwYf0; H_PS_PSSID=1420_13074_13381_12868_13323_12691_13410_10562_12723_13439_13657_13288_13601_11520_13085_8498_13156; BDUSS=EkzZmZobm9qTnRZQWNNN3BMblFyTFpoTWlqUzdwMXEtelVyMXM2NXp4ckxoMkZWQVFBQUFBJCQAAAAAAAAAAAEAAACE6VAUeWN6aG91aHlpAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAMv6OVXL-jlVV"
        }
        return headers

    def Get_Request_Headers_USA(self):
        headers = {
        "Accept": "text/plain, */*; q=0.01",
        "Accept-Encoding": "gzip, deflate, sdch",
        "Accept-Language": "zh-CN,zh;q=0.81",
        "Connection": "keep-alive",
        "Content-Type":"text/html",
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.89 Safari/537.36",
        "Referer": "http://www.sharewithu.com/college/search.php?country=20001&p=1",
        "Cookie" : "swu_browser=pc; swu_browse_mode=desktop; pgv_pvi=6828567552; pgv_si=s7893284864; looyu_id=b71b4e93aa9ba7631870d7d3f6f03200f3_10025038%3A1; swu_anony=O%3A4%3A%22User%22%3A17%3A%7Bs%3A2%3A%22id%22%3Bi%3A1137540880%3Bs%3A6%3A%22fromId%22%3Bs%3A5%3A%22anony%22%3Bs%3A5%3A%22snsId%22%3Bi%3A0%3Bs%3A8%3A%22nickname%22%3Bs%3A6%3A%22%E6%B8%B8%E5%AE%A2%22%3Bs%3A6%3A%22gender%22%3Bi%3A0%3Bs%3A6%3A%22avatar%22%3Bs%3A26%3A%22%2Fcomments%2Fimage%2Favatar.png%22%3Bs%3A5%3A%22space%22%3Bs%3A0%3A%22%22%3Bs%3A5%3A%22email%22%3Bs%3A0%3A%22%22%3Bs%3A6%3A%22mobile%22%3Bs%3A0%3A%22%22%3Bs%3A8%3A%22realName%22%3Bs%3A0%3A%22%22%3Bs%3A8%3A%22location%22%3Bs%3A0%3A%22%22%3Bs%3A5%3A%22brief%22%3Bs%3A0%3A%22%22%3Bs%3A9%3A%22signature%22%3Bs%3A0%3A%22%22%3Bs%3A10%3A%22createTime%22%3Bs%3A0%3A%22%22%3Bs%3A4%3A%22fans%22%3Bi%3A0%3Bs%3A7%3A%22follows%22%3Bi%3A0%3Bs%3A8%3A%22articles%22%3Bi%3A0%3B%7D; iug3_7061_lastvisit=1432791483; iug3_7061_sid=HfHGgj; iug3_7061_lastact=1432795084%09hotword.php%09; __utmt=1; Hm_lvt_3010f10331769afefaedf29d46e8a27e=1432783686; Hm_lpvt_3010f10331769afefaedf29d46e8a27e=1432819448; __utma=183272292.724161953.1432783686.1432805356.1432816578.5; __utmb=183272292.11.10.1432816578; __utmc=183272292; __utmz=183272292.1432783686.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); looyu_10025038=v%3Ab71b4e93aa9ba7631870d7d3f6f03200f3%2Cref%3A%2Cr%3A%2Cmon%3Ahttp%3A//m9100.talk99.cn/monitor%2Cp0%3Ahttp%253A//www.sharewithu.com/college/ranks.php"
        }
        return headers