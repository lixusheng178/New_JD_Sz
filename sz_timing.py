#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
@Project : JD_Sz
@Time : 2019/10/10 0010 11:53
@Author : 孟凡不凡 (*^__^*)
@Email : 1782316637@qq.com
@File : sz_timing.py
@Describe: 京东尚智定时任务
'''
import os
import sys

import numpy
import json,xlwt

import time
from datetime import datetime, timedelta
from threading import Thread

import pymysqlpool
from redis import StrictRedis
from util.log_util import LoggerUtil
from util.csv_util import CompetSku, Item
from util.html_util import Html, SpuSku
from util.sql_util import MySql
flag = True
rs = StrictRedis(host="116.196.91.160", port=15000, db=100, password="Summer001")
config = {
     'pool_name': 'test',
     'host': '116.196.120.31',
     'port': 3306,
     'user': 'tj',
     'password': 'Toolslit2018',
     'database': 'sz_tools'
}
pool = pymysqlpool.ConnectionPool(**config)
class ShopContrastThread(Thread):
    def __init__(self, parent=None):
        super(ShopContrastThread, self).__init__(parent)

    def set_args(self, own_shop_id=0, shop_name=None,cookies=None, cate_id = 0, user_id=0):
        self.own_shop_id = own_shop_id
        self.own_shop_name = shop_name
        self.cookies = cookies
        self.cate_id = cate_id
        self.user_id = user_id

    # 获取竞店某一天的数据
    def getCompetitionProduct(
            self, selfShopId=0, shop_id=0, date="2019-09-23"):
        url = "https://sz.jd.com/competitionContrast/getCompetitionProduct.ajax"
        data = "?channel=99&compete1ShopId=%s&date=%s&endDate=%s&selfShopId=%s&startDate=%s&type=0" % (
            shop_id, date, date, selfShopId, date)

        host = "sz.jd.com"
        referer = "https://sz.jd.com/competitionContrast/shopContrasts.html"

        resp = Html().get_html(
            url +
            data,
            host=host,
            referer=referer,
            cookies=self.cookies)
        if resp is None:
            LoggerUtil.write_file_log(
                "您的店铺获取shop_id: %s, date: %s, 接口获取异常" %
                (shop_id, date))
            return False
        try:
            resp = json.loads(resp)
        except BaseException:
            LoggerUtil.write_file_log(
                "您的店铺获取shop_id: %s, date: %s,结果转换失败 " %
                (shop_id, date))
            return False

        try:
            dealList = resp["content"]["dealList"]["compete1"]
        except BaseException:
            LoggerUtil.write_file_log(
                "您的店铺获取shop_id: %s, date: %s,获取店铺数据解析异常 " %
                (shop_id, date))
            return False
        spu_lst= [str(deal[1]) for deal in dealList]
        spu_sku = SpuSku(self.cookies).spu_get_sku(spu_lst)
        if spu_sku is False:
            LoggerUtil.write_file_log("您的店铺shop_id： %s，date: %s, 获取sku_id异常" % (shop_id,date))
            return False
        spu_sku_dict = {}
        for s in spu_sku:
            spu_sku_dict[s[0]] = s[1]
        conn = MySql().get_conn()
        if not conn:
            LoggerUtil.write_file_log("数据库连接异常")
            return False
        cur = conn.cursor()
        for deal in dealList:
            sku = CompetSku()
            sku.relat_shop_id = shop_id
            sku.spu_id = deal[1]
            sku.amount_index = deal[2]
            sku.date = date
            sku.price = deal[3]
            sku.sku_id = spu_sku_dict[deal[1]]
            sku.user_id = self.user_id
            sku.get_insert_sku_sql(cur)
        conn.commit()
        cur.close()
        conn.close()
        return True

    # 添加店铺
    def addCompetitionShop(self, shopIdOrUrl=0):
        url = "https://sz.jd.com/competitionAnalysis/addCompetitionShop.ajax?CompeteShopId=%s" % shopIdOrUrl
        Host = "sz.jd.com"
        Referer = "https://sz.jd.com/competitionAnalysis/competitionConfigs.html"
        resp = Html().get_html(
            url,
            host=Host,
            referer=Referer,
            cookies=self.cookies)
        if resp is None:
            LoggerUtil.write_file_log("shop_id: %s, 添加店铺接口请求失败" % shopIdOrUrl)
            return False
        try:
            resp = json.loads(resp)
        except BaseException:
            LoggerUtil.write_file_log("shop_id: %s, 添加店铺结果格式转换异常" % shopIdOrUrl)
            return False
        conn = MySql().get_conn()
        cur = conn.cursor()

        if resp["status"] == 0:
            LoggerUtil.write_file_log(
                "shop_id: %s, %s" %
                (shopIdOrUrl, resp["message"]))
            cur.execute(
                "update sz_relat_shop set flag = 3, add_state = 3, update_date = now() where relat_shop_id = %s and user_id=%s;" %
                (shopIdOrUrl,self.user_id))
            conn.commit()
            conn.close()
            return True
        else:
            LoggerUtil.write_file_log(
                "shop_id: %s, %s" %
                (shopIdOrUrl, resp["message"]))
            if "已超过可监控店铺添加数量" in resp["message"]:
                return -1
            cur.execute(
                "update sz_relat_shop set flag = 4, msg = %s, add_state = 4, update_date = now() where  relat_shop_id = %s and user_id=%s;", (resp["message"], shopIdOrUrl, self.user_id))
            conn.commit()
            conn.close()
            return False
    # 删除店铺

    def delCompetitionShop(self, CompeteShopId=0):
        url = "https://sz.jd.com/competitionAnalysis/delCompetitionShop.ajax?CompeteShopId=%s" % CompeteShopId
        host = "sz.jd.com"
        referer = "https://sz.jd.com/competitionAnalysis/competitionConfigs.html"

        resp = Html().get_html(
            url,
            host=host,
            referer=referer,
            cookies=self.cookies)
        if resp is None:
            LoggerUtil.write_file_log("shop_id: %s, 删除店铺，结果获取失败" % CompeteShopId)
            return False
        try:
            resp = json.loads(resp)
        except BaseException:
            LoggerUtil.write_file_log("shop_id: %s, 删除店铺结果格式转换异常" % CompeteShopId)
            return False

        if resp["status"] == 0:
            LoggerUtil.write_file_log(
                "shop_id: %s, 删除店铺 %s" %
                (CompeteShopId, resp["message"]))
            with pool.connection() as conn:
                cur = conn.cursor()
                cur.execute("update sz_relat_shop set add_state = 2 where relat_shop_id = %s;", (CompeteShopId,))
                conn.commit()
            return True
        else:
            LoggerUtil.write_file_log(
                "shop_id: %s, 删除店铺： %s" %
                (CompeteShopId, resp["message"]))
            return False

    def get_shop(self):
        conn = MySql().get_conn()
        if not conn:
            LoggerUtil.write_file_log("数据库连接失败")
            return False
        cur = conn.cursor()
        cur.execute(
            "select relat_shop_id,relat_shop_name,flag from sz_relat_shop where flag in (1, 2) and cate=%s and lock_state = 1 and shop_state=1 and user_id = %s order by flag, update_date limit 20",(self.cate_id, self.user_id))
        data = cur.fetchall()
        self.relat_shop_lst = ",".join([str(x[0]) for x in data])
        cur.execute("update sz_relat_shop set lock_state = 2 where relat_shop_id in (%s) and user_id=%s;" % (self.relat_shop_lst, self.user_id))
        conn.commit()
        flag_lst = [int(x[2]) for x in data]
        if flag_lst == len(data) * [2]:
            cur.execute("update sz_relat_shop set flag = 1,update_date = now() where flag = 2 and cate = %s and user_id=%s;", (self.cate_id,self.user_id))
            conn.commit()
        conn.close()
        return data

    # 获取一添加对比的竞店
    def getAllCompetitionShopData(self):
        url = "https://sz.jd.com/competitionAnalysis/getAllCompetitionShopData.ajax"
        host = "sz.jd.com"
        referer = "https://sz.jd.com/competitionAnalysis/competitionConfigs.html"
        resp = Html().get_html(
            url,
            host=host,
            referer=referer,
            cookies=self.cookies)
        if resp is None:
            LoggerUtil.write_file_log("您的店铺获取已添加的竞店数据失败")
            return False

        try:
            resp = json.loads(resp)
        except BaseException:
            LoggerUtil.write_file_log("获取已添加的竞店数据转换异常")
            return False
        try:
            data = resp["content"]["data"]
        except BaseException:
            LoggerUtil.write_file_log("获取竞店数据列表异常")
            return False
        shop_lst = []
        for d in data:
            shop_name = d["CompeteShopName"]
            shop_id = d["CompeteShopId"]
            shop_lst.append((shop_id, shop_name))
        return shop_lst

    def get_relat_shop_date(self, relat_shop_id=0):
        conn = MySql().get_conn()
        if conn is False:
            return False
        cur = conn.cursor()
        cur.execute(
            "select max(date) from sz_shop_items where relat_shop_id = %s and user_id=%s;", (relat_shop_id,self.user_id))
        date = cur.fetchone()
        cur.close()
        conn.close()
        if date[0]:
            old_y, old_m, old_d = str(date[0]).split("-")
            return (datetime.now() - datetime(int(old_y),
                                              int(old_m), int(old_d))).days - 1
        else:
            return 30

    # 判断这个店在数据库中是否存在
    def is_shop_data(self, relat_shop_id=0,
                     relat_shop_name=None):
        conn = MySql().get_conn()
        cur = conn.cursor()
        cur.execute(
            "select * from sz_relat_shop where relat_shop_id = %s and user_id=%s;", (relat_shop_id,self.user_id))
        data = cur.fetchone()
        if data:
            cur.execute(
                "update sz_relat_shop set flag = 3,update_date = now() where relat_shop_id = %s and user_id=%s;",
                (relat_shop_id, self.user_id))
            conn.commit()
            cur.close()
            conn.close()
            return True

        cur.execute(
            "insert into sz_relat_shop (relat_shop_id, relat_shop_name, flag, create_date, update_date,cate,user_id) values (%s, %s, %s, now(), now(),%s,%s)",
            (relat_shop_id, relat_shop_name, 1,self.cate_id, self.user_id))
        conn.commit()
        conn.close()
        return False

    def run(self):
        own_shop_id = self.own_shop_id
        # 第一步删除店铺中已存在的竞店
        relat_shop_lst = self.getAllCompetitionShopData()
        if relat_shop_lst == False:
            return
        for relat_shop in relat_shop_lst:
            relat_shop_id, relat_shop_name = relat_shop

            self.is_shop_data(relat_shop_id, relat_shop_name)
            days = self.get_relat_shop_date(relat_shop_id)
            for day_num in range(days, 0, -1):
                date = str((datetime.now() - timedelta(days=day_num)).date())
                if self.getCompetitionProduct(
                        own_shop_id, relat_shop_id, date):
                    LoggerUtil.write_file_log(
                        "店铺: %s, 获取竞店, date: %s, 数据成功" %
                        (relat_shop_name, date))
                else:
                    LoggerUtil.write_file_log(
                        "店铺: %s, 获取竞店, date: %s, 数据失败" %
                        (relat_shop_name, date))

            conn = MySql().get_conn()
            cur = conn.cursor()
            cur.execute(
                "update sz_relat_shop set flag = 2, update_date = now() where relat_shop_id = %s and user_id=0;", (relat_shop_id,self.user_id))
            conn.commit()
            conn.close()
            if self.delCompetitionShop(relat_shop_id):
                LoggerUtil.write_file_log("您的店铺删除店铺： %s 成功" % relat_shop_name)

        shop_lst = self.get_shop()
        try:
            for relat_shop_id, relat_shop_name, flag in shop_lst:
                add_flag = self.addCompetitionShop(relat_shop_id)
                if add_flag == -1:
                    return
                if add_flag:
                    days = self.get_relat_shop_date(relat_shop_id)
                    for day_num in range(days, 0, -1):
                        date = str(
                            (datetime.now() -
                             timedelta(
                                days=day_num)).date())
                        if self.getCompetitionProduct(
                                own_shop_id, relat_shop_id, date):
                            LoggerUtil.write_file_log(
                                "店铺: %s, 获取竞店, date: %s, 数据成功" %
                                (relat_shop_name, date))
                        else:
                            LoggerUtil.write_file_log(
                                "店铺: %s, 获取竞店, date: %s, 数据失败" %
                                (relat_shop_name, date))

                    with pool.connection() as conn:
                        cur = conn.cursor()
                        cur.execute(
                            "update sz_relat_shop set flag = 2 where relat_shop_id = %s and user_id=%s;", (relat_shop_id,self.user_id))
                        conn.commit()
                        conn.close()
        except:
            LoggerUtil.write_file_log("[%s] -- [%s]" % (sys.exc_info()[0], sys.exc_info()[1]))
        finally:
            with pool.connection() as conn:

                cur = conn.cursor()
                cur.execute(
                    "update sz_relat_shop set lock_state = 1 where relat_shop_id in (%s) and user_id=%s;" % (self.relat_shop_lst,self.user_id))
                conn.commit()
                conn.close()

class LossAnalysisThread(Thread):
    def __init__(self, parent=None):
        super(LossAnalysisThread, self).__init__(parent)
        self.start_date = str((datetime.now() - timedelta(days=30)).date())
        self.end_date = str((datetime.now() - timedelta(days=1)).date())

    def set_args(self, cookies=None,shop_name=None,shop_id=None, user_id=0):
        self.shop_id = shop_id
        self.shop_name = shop_name
        self.cookies = cookies
        self.user_id= user_id
    def getLossProList(self):
        url = "https://sz.jd.com/competitionAnalysis/getLossProList.ajax"
        data = {
            "date": "30" + self.end_date,
            "endDate": self.end_date,
            "indChannel": 99,
            "startDate": self.start_date,
            "unitType": 0
        }
        s = "?"
        for d in data:
            s += "%s=%s&" % (d, data[d])

        host = "sz.jd.com"
        referer = "https://sz.jd.com/competitionAnalysis/lossAnalysiss.html"
        resp = Html().get_html(
            url +
            s.strip("&"),
            host=host,
            referer=referer,
            cookies=self.cookies)
        try:
            resp = json.loads(resp)
        except BaseException:
            LoggerUtil.write_file_log("流失商品数据结果转换异常")
            return False
        if resp["message"] or resp["message"] == "success":
            data = resp["content"]["data"]
            buy_url_lst = []


            for d in data:
                name = d[0]
                buy_url_id = d[1]
                amount = d[2]
                if float(amount) == 0:
                    continue
                LoggerUtil.write_file_log(
                    "name: %s, buy_url_id: %s" %
                    (name, buy_url_id))
                buy_url_lst.append((name, buy_url_id))
            return buy_url_lst
        else:
            LoggerUtil.write_file_log(resp["message"])
            return False

    def getLossProToOtherShopDetail(self, buy_url_id=0):
        url = "https://sz.jd.com/competitionAnalysis/getLossProToOtherShopDetail.ajax?date=30%s&endDate=%s&indChannel=99&proId=%s&startDate=%s&unitType=0" % (
            self.end_date, self.end_date, buy_url_id, self.start_date)

        host = "sz.jd.com"
        referer = "https://sz.jd.com/competitionAnalysis/lossAnalysiss.html"
        resp = Html().get_html(
            url,
            host=host,
            referer=referer,
            cookies=self.cookies)

        try:
            resp = json.loads(resp)
        except BaseException:
            LoggerUtil.write_file_log("buy_url_id: %s, 获取详情结果转换异常" % buy_url_id)
            return False
        if resp["message"] or resp["message"] == "success":

            data = resp["content"]["data"]
            if len(data) > 0:

                spu_lst = [str(x[1]) for x in data]
                spu_sku = SpuSku(self.cookies).spu_get_sku(spu_lst)
                if spu_sku is False:
                    LoggerUtil.write_file_log("buy_url_id： %s，获取sku_id异常" % buy_url_id)
                    return False

                spu_sku_dict = {}
                for s in spu_sku:
                    spu_sku_dict[s[0]] = s[1]
                conn = MySql().get_conn()
                cur = conn.cursor()
                for d in data:
                    item = Item()
                    # 商品名
                    item.name = d[0]
                    if not item.name:
                        continue
                    item.own_shop_id = self.shop_id
                    # 商品id
                    item.buy_url_id = d[1]
                    # 店铺名
                    item.shop_name = d[2]
                    # 店铺链接
                    item.shop_url = d[3]
                    if "http" not in d[3]:
                        item.shop_url = "http:" + d[3]

                    # 流失成交金额
                    item.transact_amount = d[4]
                    # 流失成交单量
                    item.turnover = d[5]
                    # 流失成交商品件数
                    item.lost_shop_num = d[6]
                    # 流失成交客户数
                    item.lost_peo_num = d[7]
                    # 流失成交均价
                    item.transact_price = d[8]
                    item.sku_id = spu_sku_dict[d[1]]
                    item.user_id = self.user_id
                    # item.write()
                    item.insert_item(cur)
                    # LoggerUtil.write_file_log("buy_url_id: %s, 插入成功" % d[0])
                conn.commit()
                cur.close()
                conn.close()
                LoggerUtil.write_file_log(
                    "buy_url_id: %s, 获得%s条流失数据" %
                    (buy_url_id, len(data)))
        else:
            LoggerUtil.write_file_log(resp["message"])
            return

    def run(self):
        buy_url_lst = self.getLossProList()
        if not buy_url_lst:
            LoggerUtil.write_file_log("店铺： %s，获取的有效流失数据为0条" % self.shop_name)
            return
        count = len(buy_url_lst)

        LoggerUtil.write_file_log("店铺： %s，共获得商品数： %s条" % (self.shop_name,count))
        for name, buy_url_id in buy_url_lst:

            LoggerUtil.write_file_log("正在获取商品： %s的流失数据" % name)
            self.getLossProToOtherShopDetail(buy_url_id)
            count -= 1
            LoggerUtil.write_file_log("商品： %s 的流失数据写入成功, 还剩%s条" % (name, count))
        LoggerUtil.write_file_log("店铺： %s， 所有数据均以获取结束" % self.shop_name)

def update_own_shop_state(shop_id=0, work_state=1, user_id=0):
    with pool.connection() as conn:
        cur = conn.cursor()
        cur.execute("update sz_own_shop set work_state = %s,update_date=now() where own_shop_id = %s and user_id=%s;", (work_state, shop_id, user_id))
        conn.commit()

def run_shop(shop_name=None, sz_shop_value={}):

    shop_value = sz_shop_value[shop_name]
    cookies = shop_value["cookies"]
    flag = shop_value["flag"]
    shop_flag = shop_value["shop_flag"]
    shop_id = shop_value["shop_id"]
    cate_id = shop_value["cate"]
    user_id = shop_value["user_id"]
    now_time = datetime.now()

    if flag == 1:
        if shop_flag is None:
            update_own_shop_state(shop_id,2,user_id)
            loss_analysis_thread = LossAnalysisThread()
            loss_analysis_thread.set_args(cookies, shop_name,shop_id, user_id)
            shop_contrast_thread = ShopContrastThread()
            shop_contrast_thread.set_args(shop_id, shop_name,cookies,cate_id, user_id)
            loss_analysis_thread.start()
            shop_contrast_thread.start()
            loss_analysis_thread.join()
            shop_contrast_thread.join()
            update_own_shop_state(shop_id,1, user_id)
            sz_shop_value = json.loads(rs.get("out_sz_shop_redis").decode())
            shop_value["shop_flag"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            sz_shop_value[shop_name] = shop_value
            rs.set("out_sz_shop_redis", json.dumps(sz_shop_value, ensure_ascii=False))
            LoggerUtil.write_file_log("店铺： %s， 数据采集结束" % shop_name)
            return
        start_date = datetime.strptime(shop_flag, "%Y-%m-%d %H:%M:%S")
        if (now_time - start_date).days > 0 or (now_time - start_date).seconds > 60 * 60 * 24:
            update_own_shop_state(shop_id,2)

            loss_analysis_thread = LossAnalysisThread()
            loss_analysis_thread.set_args(cookies, shop_name, shop_id, user_id)
            shop_contrast_thread = ShopContrastThread()
            shop_contrast_thread.set_args(shop_id, shop_name, cookies, cate_id, user_id)
            loss_analysis_thread.start()
            shop_contrast_thread.start()
            loss_analysis_thread.join()
            shop_contrast_thread.join()
            update_own_shop_state(shop_id,1)

            sz_shop_value = json.loads(rs.get("out_sz_shop_redis").decode())
            shop_value["shop_flag"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            sz_shop_value[shop_name] = shop_value
            rs.set("out_sz_shop_redis", json.dumps(sz_shop_value, ensure_ascii=False))
            LoggerUtil.write_file_log("店铺： %s， 数据采集结束" % shop_name)
        else:
            LoggerUtil.write_file_log("店铺： %s， 店铺添加的时间不足24小时" % shop_name)
            return
    else:
        LoggerUtil.write_file_log("店铺： %s，的cookies无效，请及时登录" % shop_name)


def run():
    sz_shop_value = json.loads(rs.get("out_sz_shop_redis").decode())
    pool = []
    for sz_shop in sz_shop_value:
        t = Thread(target=run_shop, args=(sz_shop,sz_shop_value))
        pool.append(t)
        t.start()
    for t in pool:
        t.join()

if __name__ == '__main__':
    while True:
        run()
        print("[%s]: 休眠中..." % datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        time.sleep(60)
    #

