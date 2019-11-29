#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
@Project : JD_Sz
@Time : 2019/9/24 0024 17:35
@Author : 孟凡不凡 (*^__^*)
@Email : 1782316637@qq.com
@File : csv_util.py
@Describe:
'''
import os
import time
from datetime import datetime as dt
from util.sku_util import Sku
from pymysql import IntegrityError, OperationalError, InternalError


class Item():
    def __init__(self,w=None):
        self.own_shop_id = None
        self.w = w
        # 商品名
        self.name = None
        # 商品id
        self.buy_url_id = None
        # 店铺名
        self.shop_name = None
        # 店铺链接
        self.shop_url = None

        # 流失成交金额
        self.transact_amount = None
        # 流失成交单量
        self.turnover = None
        # 流失成交商品件数
        self.lost_shop_num = None
        # 流失成交客户数
        self.lost_peo_num = None
        # 流失成交均价
        self.transact_price = None
        self.sku_id = None
        self.user_id = 0

    def write(self):
        file_name = "商品流失分析%s.csv" % (dt.now().strftime('%Y-%m-%d'))
        if not os.path.isfile(file_name):
            with open(file_name,"w", encoding="utf-8") as f:
                f.write("商品id, 商品名, 店铺名, 店铺链接, 流失成交金额, 流失成交单量, 流失成交商品件数, 流失成交客户数, 流失成交均价\n")
                f.write("%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s \n" % (self.name, self.buy_url_id, self.shop_name, self.shop_url, self.transact_amount, self.turnover, self.lost_shop_num, self.lost_peo_num, self.transact_price))

        else:
            with open(file_name,"a", encoding="utf-8") as f:
                f.write("%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s ,%s \n" % (
                self.name, self.buy_url_id, self.shop_name, self.shop_url, self.transact_amount, self.turnover,
                self.lost_shop_num, self.lost_peo_num, self.transact_price))

    def insert_item(self,cur, count=0):
        cur.execute("select * from sz_loss_items where buy_url_id = %s;", (self.buy_url_id,))
        data = cur.fetchone()
        if data:
            try:
                cur.execute("update sz_loss_items set create_date = now(),own_shop_id=%s  where buy_url_id = %s and user_id=%s;" , (self.own_shop_id, self.buy_url_id, self.user_id))
            except IntegrityError:
                return
            except OperationalError:
                print("buy_url_id: %s, lock" % self.buy_url_id)
                time.sleep(0.2)
                return self.insert_item(cur,count+1)
            except InternalError:
                print("buy_url_id: %s, lock" % self.buy_url_id)
                time.sleep(0.2)
                return self.insert_item(cur,count+1)
            if self.w is None:
                print("buy_url_id: %s, 已存在" % self.buy_url_id)
            else:
                self.w.log_thread.run_("buy_url_id: %s, 已存在" % self.buy_url_id)

        else:
            try:
                cur.execute("insert into sz_loss_items (buy_url_id, name, shop_name, shop_url, transact_amount, turnover, lost_shop_num, lost_peo_num, transact_price, create_date, sku_id, own_shop_id, user_id) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, now(), %s, %s, %s);",
                            (self.buy_url_id, self.name, self.shop_name, self.shop_url,self.transact_amount, self.turnover, self.lost_shop_num, self.lost_peo_num, self.transact_price,self.sku_id,self.own_shop_id, self.user_id))
            except IntegrityError:
                return
            except OperationalError:
                print("buy_url_id: %s, lock" % self.buy_url_id)
                time.sleep(0.2)
                return self.insert_item(cur,count+1)
            except InternalError:
                print("buy_url_id: %s, lock" % self.buy_url_id)
                time.sleep(0.2)
                return self.insert_item(cur,count+1)

            if self.w is None:
                print("buy_url_id: %s, 插入成功" % self.buy_url_id)
            else:

                self.w.log_thread.run_("buy_url_id: %s, 插入成功" % self.buy_url_id)


class CompetSku():
    def __init__(self):
        self.relat_shop_id = None
        self.spu_id = None
        self.amount_index = None
        self.date = None
        self.price = None
        self.create_date = dt.now().strftime('%Y-%m-%d %H:%M:%S')
        self.sku_id = None
        self.user_id=0
    def get_insert_sku_sql(self, cur, count=0):
        if count > 5:
            return
        cate = Sku(self.sku_id).get_sku_args()
        if cate is False:
            return
        cate1_id, cate2_id,cate3_id,cate1_name, cate2_name, cate3_name = cate
        cur.execute("select * from sz_shop_items where spu_id = %s and relat_shop_id = %s and date = %s and user_id=%s;" % (self.spu_id, self.relat_shop_id, self.date, self.user_id))
        data = cur.fetchone()
        if data:
            print("spu_id: %s, 数据库中已存在" % self.spu_id)
        else:
            try:
                cur.execute("insert into sz_shop_items (relat_shop_id, spu_id, amount_index, date, create_date,price,sku_id,cate1_id,cate2_id,cate3_id,cate1_name, cate2_name, cate3_name, user_id) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s, %s)", (self.relat_shop_id, self.spu_id, self.amount_index,self.date, self.create_date, self.price,self.sku_id, cate1_id,cate2_id,cate3_id,cate1_name, cate2_name, cate3_name,self.user_id))
            except IntegrityError:
                return
            except OperationalError:
                print("spu_id: %s, lock" % self.spu_id)
                time.sleep(0.2)
                return self.get_insert_sku_sql(cur,count+1)
            except InternalError:
                print("spu_id: %s, lock" % self.spu_id)
                time.sleep(0.2)
                return self.get_insert_sku_sql(cur,count+1)

