# -*- coding:utf-8 -*-
import os
import urllib2
import logging
import re
import Queue
import time
import hashlib

class Spider(object):
    def __init__(self,
                 startUrl,                  #开始爬行的起始页面        type: String
                 header=None,               #自定义头部字段            type: dict
                 scopeLevel=0,              #定义爬行范围，0:只在当前域名内爬行.1:同时爬行白名单列表内的.2:模糊匹配白名单列表中的域名
                 scopeList=None,            #定义爬行范围列表          type: list
                 useLogger=True,            #是否启用日志记录          type: boolean
                 logDirPath=None,           #存放log文件的目录路径     type: String
                 logLevel=logging.INFO      #记录log等级
                 ):
        '''
        :desc: 爬行当前域名的页面
        '''

        self.UserAgent          = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) ' \
                                  'Chrome/33.0.1750.117 Safari/537.36'
        self.domain             = None
        self.startUrl           = startUrl
        self.customHeader       = header
        self.scopeLevel         = scopeLevel
        self.scopeList          = scopeList
        self.useLogger          = useLogger
        self.logger             = None
        self.logDirPath         = logDirPath
        self.logLevel           = logLevel

        self.avisitedUrl        = {}
        self.avisitedPageMd5    = {}
        self.willVisitUrl       = Queue.Queue()
        self.externUrl          = Queue.Queue()

        try:
            self.check_parameter()
        except Exception, e:
            print e
            raise e

        #初始化日志
        self.logger = logging.getLogger("Spider_logger_%s" % self.domain)
        self.logger.setLevel(self.logLevel)
        formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] -> %(message)s')

        if self.logDirPath != None:
            filehandler = logging.FileHandler(self.logDirPath + "/" + "Spider_logger_%s.log" % self.domain)
            filehandler.setLevel(self.logLevel)
            filehandler.setFormatter(formatter)
            self.logger.addHandler(filehandler)

        consolehandler = logging.StreamHandler()
        consolehandler.setFormatter(formatter)
        consolehandler.setLevel(self.logLevel)
        self.logger.addHandler(consolehandler)
        self.logger.info("Init successful!")

    ####################################################################################################################
    def check_parameter(self):
        '''
        :desc: 检查输入参数
        :return: 无返回值，有误则抛出异常
        '''
        if self.startUrl is None:
            raise Exception("The variable 'startUrl' is NoneType!")
        if len(self.startUrl) <= 0:
            raise Exception("The variable 'startUrl' length is zero!")
        if self.logDirPath is not None and len(self.logDirPath) <= 0:
            raise Exception("The variable 'logDirPath' is not set!")
        if self.scopeLevel not in [0, 1, 2]:
            raise Exception("The variable 'scopeLevel' value error!")
        if self.scopeLevel == 2:
            if self.scopeList is None or len(self.scopeList) < 1:
                raise Exception("The variable 'scopeList' is not set!")

        #检查访问页面格式
        self.startUrl = self.correct_url(self.startUrl)

        #生成当前域名
        protoIdx = self.startUrl.find("://")
        if protoIdx >= 0:
            self.domain = self.startUrl[protoIdx + 3:]
        endIdx = self.domain.find("/")
        if endIdx >= 0:
            self.domain = self.domain[:endIdx]

    ####################################################################################################################
    def correct_url(self, url):
        '''
        :desc: 修正访问页面的URL格式
        :type url: String
        :param url: 将要进行修正的URL
        :type return: String
        :return: 返回修正后的URL
        '''
        if url.find("http://", 0, 7) >= 0:
            pass
        else:
            url = "http://" + url
        return url

    ####################################################################################################################
    def calc_md5(self, s):
        '''
        :desc: 计算传入字符串的MD5值
        :type s: String
        :param s: 源字符串
        :type return: String
        :return: 成功返回MD5值，失败返回None
        '''
        try:
            m = hashlib.md5()
            m.update(s)
            md5 = m.hexdigest()
        except Exception, e:
            self.logger.debug("calc_md5() error. Exception: %s" % str(e))
            return None
        return md5

    ####################################################################################################################
    def add_avisited_url(self, url):
        '''
        :desc: 添加已访问过URL列表
        :type url: String
        :param url: 访问过的URL地址
        :type return: boolean
        :return: 成功返回True,失败返回False
        '''
        try:
            self.avisitedUrl[url] = time.time()
        except Exception, e:
            self.logger.debug("add_avisited_url() error. Exception: %s" % str(e))
            return False
        return True

    ####################################################################################################################
    def add_avisited_page(self, url, pageText):
        '''
        :desc: 添加已访问过的页面列表
        :type url: String
        :param url: 已访问过的页面URL
        :type pageText: String
        :param pageText: 访问过的页面文本内容
        :type return: boolean
        :return: 成功返回True，失败返回False
        '''
        try:
            pageMd5 = self.calc_md5(pageText)
            self.avisitedPageMd5[pageMd5] = url
        except Exception, e:
            self.logger.debug("add_avisited_page() error. Exception: %s" % str(e))
            return False
        return True

    ####################################################################################################################
    def has_avisited_url(self, url):
        '''
        :desc: 在已访问过的URL列表中查询是否已包含此条URL
        :type url: String
        :param url: 将要查询的URL地址
        :type return: boolean
        :return: 返回True则表明已有记录，返回False则表明没有记录
        '''
        try:
            if url in self.avisitedUrl.keys():
                return True
            else:
                return False
        except Exception, e:
            self.logger.debug("has_avisited_url() error. Exception: %s" % str(e))
            return False

    ####################################################################################################################
    def has_avisited_page(self, url, pageText):
        '''
        :desc: 在已访问过的页面列表中查询是否已包含此页面
        :type url: String
        :param url: 将要查询页面的URL地址
        :type pageText: String
        :param pageText: 将要查询页面的文本内容
        :type return: boolean
        :return: 返回True则表明已有记录，返回False则表明没有记录
        '''
        try:
            pageMd5 = self.calc_md5(pageText)
            if pageMd5 in self.avisitedPageMd5.keys():
                return True
            else:
                return False
        except Exception, e:
            self.logger.debug("has_avisited_page() error. Exception: %s" % str(e))
            return False

    ####################################################################################################################
    def fetch_page_content(self, url):
        '''
        :desc: 获取目标页面内容
        :type url: String
        :param url: 访问页面地址
        :type return: String
        :return: 成功返回页面文本内容，失败返回None
        '''
        pageText = None
        try:
            #获取页面内容
            url = self.correct_url(url)
            request = urllib2.Request(url)
            #添加自定义HTTP头部字段
            if self.customHeader is not None:
                for key in self.customHeader.keys():
                    request.add_header(key, self.customHeader[key])
            if self.customHeader is None or "User-Agent" not in self.customHeader.keys():
                request.add_header("User-Agent", self.UserAgent)
            response = urllib2.urlopen(request, timeout=3)
            pageText = response.read()

            #检查页面是否被访问过
            if self.has_avisited_page(url, pageText) is True:
                self.logger.info("The page %s is already visited." % url)
                return None
            else:
                self.add_avisited_page(url, pageText)

            #页面处理回调函数
            try:
                self.page_hander_callback(url, pageText)
            except Exception, e:
                self.logger.info("page_hander_callback error. Exception: %s" % str(e))
        except Exception, e:
            self.logger.error("Fetch %s error. Exception: %s" % (url, str(e)))
        return pageText

    ####################################################################################################################
    def analyze_page(self, url, pageText):
        '''
        :desc: 分析当前爬行到的页面，并解析出其它链接
        :param url: 当前爬行到的页面地址
        :param pageText: 当前页面文本内容
        :return: 无返回值
        '''
        #正则匹配链接
        labelPatt = r"<.*href={1,}.*>"
        hrefPatt = r"(?<=href=\").+?(?=\")"
        labelList = re.findall(labelPatt, pageText)
        for label in labelList:
            hrefList = re.findall(hrefPatt, label)
            for href in hrefList:
                #抓取到链接
                if href.find("javascript:") >= 0:
                    continue
                #检测抓取到的链接访问协议
                if href.find("://") < 0:
                    url = "http://" + self.domain + "/" + href
                else:
                    url = href

                #检测是否爬行本域名下的连接
                protoIdx = url.find("://")      #去除协议头
                if protoIdx >= 0:
                    tmpUrl = url[protoIdx + 3:]
                else:
                    tmpUrl = url
                #截取URL域名部分
                endIdx = tmpUrl.find("/")
                if endIdx >= 0:
                    tmpUrl = tmpUrl[:endIdx]
                #去除万维网标识
                if tmpUrl.find("www.", 0, 4) >= 0:
                    tmpUrl = tmpUrl[4:]
                if self.domain.find("www.", 0, 4) >= 0:
                    pattDomain = self.domain[4:]
                else:
                    pattDomain = self.domain

                #检查是否爬行到其它域名
                domainCheck = False
                if self.scopeLevel == 0:
                    if tmpUrl == pattDomain:
                        domainCheck = True
                elif self.scopeLevel == 1:
                    if tmpUrl == pattDomain:
                        domainCheck = True
                    if domainCheck is False:
                        if tmpUrl in self.scopeList:
                            domainCheck = True
                elif self.scopeLevel == 2:
                    for domain in self.scopeList:
                        if tmpUrl.find(domain) >= 0:
                            domainCheck = True
                            break

                if domainCheck is True:
                    #爬行到当前域名的链接
                    if self.has_avisited_url(url) is False:         #检查URL是否访问过
                        self.willVisitUrl.put(url)                  #将爬取到的链接加入即将访问队列
                        self.add_avisited_url(url)                  #加入已访问URL列表
                        self.logger.info("Crawl: %s" % url)
                else:
                    #爬行到其它域名的链接
                    self.externUrl.put(url)
                    self.logger.info("Extern: %s" % url)

    ####################################################################################################################
    def page_hander_callback(self, url, pageText):
        '''
        :desc: 页面处理回调函数
        :type url: String
        :param url: 获取到页面的URL
        :type pageText: String
        :param pageText: 获取到页面的文本内容
        :return: 无返回值
        '''
        pass

    ####################################################################################################################
    def start(self):
        '''
        :desc: 开启爬行目标
        :return: 无返回值
        '''
        self.willVisitUrl.put(self.startUrl)
        while self.willVisitUrl.empty() is False:
            url = self.willVisitUrl.get()               #从将要访问的URL队列中取出一个地址
            pageText = self.fetch_page_content(url)     #获取目的URL页面内容
            if pageText is None:
                continue
            self.analyze_page(url, pageText)            #分析页面


count = 0
def page_hander_callback(url, pageText):
    global  count
    count += 1
    dir = "E:\\Python_Projects\\Spider\\test\\"
    file = open(dir + str(count) + ".html", "w")
    file.write(pageText)
    file.close()

test = Spider("http://bobao.360.cn/index/index", scopeLevel=0)
test.page_hander_callback = page_hander_callback
test.start()










