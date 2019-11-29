#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
@File  : html_util.py
@Author: 孟凡不凡
@Date  : 2019/7/9 11.json:55
@Desc  :  抓取链接
'''
import json
import random
import sys

import requests


class Html():
    def __init__(self, w=None):
        self.w = w


    def get_headers(self, host=None, referer=None, cookies=None, t=0):
        head_user_agent = ['Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.json.0) like Gecko',
                           'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1500.95 Safari/537.36',
                           'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; rv:11.json.0) like Gecko)',
                           'Mozilla/5.0 (Windows; U; Windows NT 5.2) Gecko/2008070208 Firefox/3.0.1',
                           'Mozilla/5.0 (Windows; U; Windows NT 5.1) Gecko/20070309 Firefox/2.0.0.3',
                           'Mozilla/5.0 (Windows; U; Windows NT 5.1) Gecko/20070803 Firefox/1.5.0.12',
                           'Opera/9.27 (Windows NT 5.2; U; zh-cn)',
                           'Mozilla/5.0 (Macintosh; PPC Mac OS X; U; en) Opera 8.0',
                           'Opera/8.0 (Macintosh; PPC Mac OS X; U; en)',
                           'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.12) Gecko/20080219 Firefox/2.0.0.12 Navigator/9.0.0.6',
                           'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Win64; x64; Trident/4.0)',
                           'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0)',
                           'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.2; .NET4.0C; .NET4.0E)',
                           'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Maxthon/4.0.6.2000 Chrome/26.0.1410.43 Safari/537.1 ',
                           'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.2; .NET4.0C; .NET4.0E; QQBrowser/7.3.9825.400)',
                           'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:21.0) Gecko/20100101 Firefox/21.0 ',
                           'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.92 Safari/537.1 LBBROWSER',
                           'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0; BIDUBrowser 2.x)',
                           'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.11.json (KHTML, like Geckio) Chrome/20.0.1132.11.json TaoBrowser/3.0 Safari/536.11.json']
        headers = {
            "accept": "application/json, text/plain, */*",
            "accept-encoding": "gzip, deflate, br",
            "accept-language": "zh-CN,zh;q=0.9",
            "cache-control": "max-age=0",
            "upgrade-insecure-requests": "1",
            "X-Requested-With": "XMLHttpRequest",
            "Content-Type": "application/x-www-form-urlencoded",
            "Connection": "keep-alive",
            "user-agent": head_user_agent[random.randrange(0, len(head_user_agent))]
        }
        if host is not None:
            headers["Host"] = host
        if referer is not None:
            headers["Referer"] = referer
        if cookies is not None:
            headers["Cookie"] = cookies
        if t == 1:
            headers["Content-Type"] = "application/json;charset=UTF-8"
        return headers

    def get_html(self, url, referer=None, host=None,
                 cookies=None, count=0, encoding="utf-8", t=0):

        if count > 5:
            if self.w is None:
                print("url: %s， 连续5次抓取失败，放弃抓取" % url)
            else:
                self.w.log_thread.run_("url: %s， 连续5次抓取失败，放弃抓取" % url)
            return

        try:
            resp = requests.get(
                url,
                headers=self.get_headers(t=t,
                                         referer=referer,
                                         host=host,
                                         cookies=cookies),timeout=2*60)
            print(resp.headers)
        except BaseException:
            if self.w is None:
                print(
                "url: %s, [%s] -- [%s]" %
                (url, sys.exc_info()[0], sys.exc_info()[1]))
            else:
                self.w.log_thread.run_(
                    "url: %s, [%s] -- [%s]" %
                    (url, sys.exc_info()[0], sys.exc_info()[1]))
                return self.get_html(url, referer=referer, host=host,
                                 cookies=cookies, count=count + 1, encoding=encoding, t=t)

        if resp.status_code == 200:
            resp.encoding = encoding
            return resp.text
        else:
            if self.w is None:
                print("url: %s, status: %s" %
                (url, resp.status_code))
            else:
                self.w.log_thread.run_(
                    "url: %s, status: %s" %
                    (url, resp.status_code))
            return self.get_html(url, referer=referer, host=host,
                                 cookies=cookies, count=count + 1, encoding=encoding, t=t)

    def post_html(self, url, referer=None, host=None, cookies=None,
                  count=0, encoding="utf-8", data={}, json={}, t=0):
        if count > 5:
            if self.w is None:
                print("url: %s， 连续5次抓取失败，放弃抓取" % url)
            else:
                self.w.log_thread.run_("url: %s， 连续5次抓取失败，放弃抓取" % url)
            return

        try:
            resp = requests.post(
                url,
                headers=self.get_headers(
                    referer=referer,
                    host=host,
                    cookies=cookies, t=t),
                data=data,
                json=json,timeout=2*60)
        except BaseException:
            if self.w is None:
                print("url: %s, [%s] -- [%s]" %
                                   (url, sys.exc_info()[0], sys.exc_info()[1]))
            else:
                self.w.log_thread.run_("url: %s, [%s] -- [%s]" %
                                   (url, sys.exc_info()[0], sys.exc_info()[1]))
            return self.post_html(url, referer=referer, host=host, cookies=cookies,
                                  count=count + 1, encoding=encoding, data=data, json=json, t=t)

        if resp.status_code == 200:
            resp.encoding = encoding
            return resp.text
        else:
            if self.w is None:
                print("url: %s, status: %s" %
                (url, resp.status_code))
            else:
                self.w.log_thread.run_(
                    "url: %s, status: %s" %
                    (url, resp.status_code))
            return self.post_html(url, referer=referer, host=host, cookies=cookies,
                                  count=count + 1, encoding=encoding, data=data, json=json, t=t)

    def post_json(self, url, referer=None, host=None, cookies=None,
                  count=0, encoding="utf-8", data={}, json={}, t=0):
        if count > 5:
            if self.w is None:
                print("url: %s， 连续5次抓取失败，放弃抓取" % url)
            else:
                self.w.log_thread.run_("url: %s， 连续5次抓取失败，放弃抓取" % url)
            return

        try:
            resp = requests.post(url, headers=self.get_headers(referer=referer, host=host, cookies=cookies, t=t),
                                 data=data,
                                 json=json,timeout=120)
        except BaseException:
            if self.w is None:
                print(
                "url: %s, [%s] -- [%s]" %
                (url, sys.exc_info()[0], sys.exc_info()[1]))
            else:
                self.w.log_thread.run_(
                    "url: %s, [%s] -- [%s]" %
                    (url, sys.exc_info()[0], sys.exc_info()[1]))
            return self.post_json(url, referer=referer, host=host, cookies=cookies, count=count + 1, encoding=encoding,
                                  data=data, json=json, t=t)

        if resp.status_code == 200:
            resp.encoding = encoding
            return resp.json()
        else:
            if self.w is None:
                print("url: %s, status: %s" %
                (url, resp.status_code))
            else:
                self.w.log_thread.run_(
                    "url: %s, status: %s" %
                    (url, resp.status_code))
            return self.post_json(url, referer=referer, host=host, cookies=cookies, count=count + 1, encoding=encoding,
                                  data=data, json=json, t=t)


    def get_json(self, url, referer=None, host=None,
                 cookies=None, count=0, encoding="utf-8", t=0):

        if count > 5:
            if self.w is None:
                print("url: %s， 连续5次抓取失败，放弃抓取" % url)
            else:
                self.w.log_thread.run_("url: %s， 连续5次抓取失败，放弃抓取" % url)
            return

        try:
            resp = requests.get(
                url,
                headers=self.get_headers(t=t,
                                         referer=referer,
                                         host=host,
                                         cookies=cookies),timeout=2*60)
        except BaseException:
            if self.w is None:
                print(
                "url: %s, [%s] -- [%s]" %
                (url, sys.exc_info()[0], sys.exc_info()[1]))
            else:
                self.w.log_thread.run_(
                    "url: %s, [%s] -- [%s]" %
                    (url, sys.exc_info()[0], sys.exc_info()[1]))
                return self.get_json(url, referer=referer, host=host,
                                 cookies=cookies, count=count + 1, encoding=encoding, t=t)

        if resp.status_code == 200:
            resp.encoding = encoding
            try:
                return resp.json()
            except:
                return self.get_json(url, referer=referer, host=host,
                                     cookies=cookies, count=count + 1, encoding=encoding, t=t)

        else:
            if self.w is None:
                print("url: %s, status: %s" %
                (url, resp.status_code))
            else:
                self.w.log_thread.run_(
                    "url: %s, status: %s" %
                    (url, resp.status_code))
            return self.get_json(url, referer=referer, host=host,
                                 cookies=cookies, count=count + 1, encoding=encoding, t=t)


    @staticmethod
    def cook_str_dict(cookies=""):
        cookies_dict = {}
        cooks = cookies.split(";")
        for cook in cooks:
            c = cook.split("=")
            key = c[0]
            value = "".join(c[1:])
            cookies_dict[key] = value
        return cookies_dict

    @staticmethod
    def cook_dict_str(cookies_dict={}):
        cookies = ""
        for key in cookies_dict:
            cookies += key
            cookies += "="
            cookies += cookies_dict[key]
            cookies += ";"
        return cookies.strip(";")

class SpuSku():
    def __init__(self, cookies=None):
        self.cookies = cookies
    def spu_get_sku(self, spu_lst=[]):

        url = "https://sz.jd.com/productDetail/getImageUrlBySpuIds.ajax?spuIds=%s" % ','.join(spu_lst)
        try:
            resp = Html().get_html(url, cookies=self.cookies, host="sz.jd.com")
        except:
            print()
            return False
        resp_json = json.loads(resp)
        lst = []
        if resp_json["message"] or resp_json["message"] == "success":
            data = resp_json["content"]["data"]
            for d in data:
                spu_id = d["spuId"]
                sku_id = int(d["proUrl"].split("/")[-1].split(".")[0])
                lst.append((spu_id, sku_id))
            return lst
        else:
            print(resp_json["message"])
            return False

if __name__ == '__main__':
    Html().get_html("http://t12.baidu.com/it/u=1610401883,3702459308&fm=173&app=25&f=JPEG?w=464&h=270&s=BE0360864E5614D6158677820300808E")
