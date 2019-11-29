#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
@Project : JD_Sz
@Time : 2019/10/16 0016 14:01
@Author : 孟凡不凡 (*^__^*)
@Email : 1782316637@qq.com
@File : down_file.py
@Describe:
'''
import os
import smtplib
import sys
import time
from datetime import datetime, timedelta
from threading import Thread
from email import encoders
from email.header import Header
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import numpy
import xlwt

from util.log_util import LoggerUtil
from util.sql_util import MySql


class DownLossThread(Thread):
    def __init__(self, parent=None):
        super(DownLossThread, self).__init__(parent)

    def set_args(self, start_date, end_date, file_name=None):
        self.start_date = start_date
        self.end_date = end_date
        self.file_name = file_name

    def get_datas(self):
        conn = MySql().get_conn()
        cur = conn.cursor()
        cur.execute(
            "select buy_url_id, name, shop_name, shop_url, transact_amount, turnover, lost_shop_num, lost_peo_num, transact_price, sku_id,shop_num,user_id from sz_loss_items where create_date > %s and create_date < %s and shop_flag = 1 ORDER BY user_id",
            (self.start_date,
             self.end_date))
        datas = cur.fetchall()
        cur.close()
        conn.close()
        new_datas = {}

        if len(datas) == 0:
            return new_datas
        user_index = datas[0][11]
        new_datas[user_index] = []
        for buy_url_id, name, shop_name, shop_url, transact_amount, turnover, lost_shop_num, lost_peo_num, transact_price, sku_id,shop_num,user_id in datas:
            if user_index != user_id:
                user_index = user_id

                new_datas[user_index] = []
            new_datas[user_index].append((buy_url_id, name, shop_name, shop_url, transact_amount, turnover, lost_shop_num, lost_peo_num, transact_price, sku_id,shop_num))
        return new_datas

    def down_shop_datas(self, user_id, datas=[]):
        if os.path.isfile(self.file_name):
            os.remove(self.file_name)
        workbook = xlwt.Workbook()
        worksheet = workbook.add_sheet('My Sheet')
        worksheet.write(0, 0, "商品spu_id")
        worksheet.write(0, 1, "商品名")
        worksheet.write(0, 2, "店铺名")
        worksheet.write(0, 3, "店铺链接")
        worksheet.write(0, 4, "流失成交金额")
        worksheet.write(0, 5, "流失成交单量")
        worksheet.write(0, 6, "流失成交商品件数")
        worksheet.write(0, 7, "流失成交客户数")
        worksheet.write(0, 8, "流失成交均价")
        worksheet.write(0, 9, "sku_id")
        worksheet.write(0, 10, "店铺商品数")
        index = 1
        for buy_url_id, name, shop_name, shop_url, transact_amount, turnover, lost_shop_num, lost_peo_num, transact_price, sku_id, shop_num in datas:
            worksheet.write(index, 0, buy_url_id)
            worksheet.write(index, 1, name)
            worksheet.write(index, 2, shop_name)
            worksheet.write(index, 3, shop_url)
            worksheet.write(index, 4, transact_amount)
            worksheet.write(index, 5, turnover)
            worksheet.write(index, 6, lost_shop_num)
            worksheet.write(index, 7, lost_peo_num)
            worksheet.write(index, 8, transact_price)
            worksheet.write(index, 9, "https://item.jd.com/%s.html" % sku_id)
            worksheet.write(index, 10, shop_num)
            index += 1
        workbook.save('%s' % self.file_name)

        conn = MySql().get_conn()
        cur = conn.cursor()
        cur.execute("SELECT name, email from user where id = %s", (user_id,))
        shop_data = cur.fetchone()
        cur.close()
        conn.close()
        user_name, email =  shop_data
        email = [x.strip() for x in email.replace("；",";").split(";")]
        MyEmail(email).send_main("【流失数据】%s--%s--%s条 " % (user_name, datetime.now().strftime("%Y-%m-%d"),len(datas)), [self.file_name])

    def run(self):
        new_datas = self.get_datas()
        for user_id in new_datas:
            datas = new_datas[user_id]
            self.down_shop_datas(user_id, datas)
        LoggerUtil.write_down_file_log(
            "%s -- %s , 流失数据导出成功" %
            (self.start_date, self.end_date))

class DownShopThread(Thread):
    def __init__(self, parent=None):
        super(DownShopThread, self).__init__(parent)

    def set_args(self, start_date, end_date,
                 min_price, min_num, file_name=None):
        self.start_date = start_date
        self.end_date = end_date
        self.min_price = min_price
        self.min_num = min_num
        self.file_name = file_name

    def get_data(self):
        conn = MySql().get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT
                sz_shop_items.relat_shop_id,
                sz_relat_shop.relat_shop_name,
                sz_shop_items.spu_id,
                sz_shop_items.amount_index,
                sz_shop_items.price,
                sz_shop_items.date,
                sz_shop_items.create_date,
                sz_shop_items.sku_id,
                sz_shop_items.cate1_name,
                sz_shop_items.cate2_name,
                sz_shop_items.cate3_name,
                sz_shop_items.user_id
            FROM
                sz_shop_items,
                sz_relat_shop
            WHERE
                sz_shop_items.create_date >= %s
            AND sz_shop_items.create_date < %s
            AND sz_shop_items.relat_shop_id = sz_relat_shop.relat_shop_id
            AND sz_relat_shop.relat_shop_id = sz_shop_items.relat_shop_id
            ORDER BY
                sz_relat_shop.cate,
                sz_shop_items.relat_shop_id,
                sz_shop_items.date;
        """, (self.start_date, self.end_date))
        datas = cur.fetchall()
        cur.close()
        conn.close()
        new_datas = {}
        if len(datas) == 0:
            return new_datas
        date_index = datas[0][5]
        relat_shop_index = datas[0][0]
        cate_id = datas[0][8]
        amount_index_dict = {}
        ret_datas = []
        m_datas = []
        sku_lst = []
        flag = False
        for relat_shop_id, relat_shop_name, spu_id, amount_index, price, date, create_date,sku_id,cate,cate1_name,cate2_name,cate3_name in datas:
            if cate not in new_datas:
                cate_id = cate
                new_datas[cate] = []
            if date_index != date or relat_shop_index != relat_shop_id:
                for amount_index in amount_index_dict:
                    if amount_index_dict[amount_index] < self.min_num:
                        for m in m_datas:
                            if m[3] == amount_index and float(
                                    m[4]) > self.min_price and m[2] not in sku_lst:
                                sku_lst.append(m[2])
                                flag = True
                                new_datas[cate_id].append((m[0], m[1], m[2], m[3], m[4], m[5], m[6],m[7],m[8],m[9],m[10]))
                    else:
                        print(
                            "amount_index:%s, date: %s" %
                            (amount_index, date_index))
                date_index = date
                relat_shop_index = relat_shop_id
                m_datas = []
                amount_index_dict = {}
            if amount_index in amount_index_dict:
                amount_index_dict[amount_index] += 1
            else:
                amount_index_dict[amount_index] = 1
            m_datas.append(
                (relat_shop_id,
                 relat_shop_name,
                 spu_id,
                 amount_index,
                 price,
                 date,create_date,sku_id,cate1_name,cate2_name,cate3_name))

        for amount_index in amount_index_dict:
            if amount_index_dict[amount_index] < self.min_num:
                for m in m_datas:
                    if m[3] == amount_index:
                        ret_datas.append(m)
        if not flag:
            LoggerUtil.write_down_file_log(
                "起始时间： %s, 截止时间： %s, 价格小于： %s，重复数小于：%s条件的筛选下，没有数据导出" %
                (self.start_date, self.end_date, self.min_price, self.min_num))
        return new_datas

    def down_cate(self, cate_id, datas):
        conn = MySql().get_conn()
        cur = conn.cursor()
        cur.execute("select email,cate_name from sz_cate where cate_id = %s and flag = 1", (cate_id,))
        email_data = cur.fetchall()
        if len(email_data) > 0:
            cate_name = email_data[0][1]
            email_data = [x[0] for x in email_data]
            email_len = len(email_data)
            datas = numpy.array_split(datas, email_len)
            index = 0
            for data in datas:
                with open(self.file_name, "w",encoding="utf-8") as f:
                    f.write("竞店id, 竞店名, spu_id, 成交金额指数, 京东价, 日期, 创建时间, sku_id,cate1_name,cate2_name,cate3_name\n")
                email = [x.strip() for x in email_data[index].replace("；",";").split(";")]

                with open(self.file_name, "a",encoding="utf-8") as f:
                    for relat_shop_id, relat_shop_name, spu_id, amount_index, price, date, create_date,sku_id,cate1_name,cate2_name,cate3_name in data:
                        f.write("%s, %s, %s, %s, %s, %s, %s, https://item.jd.com/%s.html, %s, %s, %s\n" % (relat_shop_id, relat_shop_name, spu_id, amount_index, price, date, create_date,sku_id,cate1_name,cate2_name,cate3_name))
                MyEmail(email).send_main("【竞店数据】%s--%s--%s/%s--%s条 " %  (cate_name, datetime.now().strftime('%Y-%m-%d'), index, email_len, len(data)), [self.file_name])
                time.sleep(5)
                index += 1
            LoggerUtil.write_down_file_log("cate_id: %s, 日期： %s，发送成功" %  (cate_id, datetime.now().strftime('%Y-%m-%d')))
        else:
            LoggerUtil.write_down_file_log("类目id： %s, 没有可以发送的邮箱" % cate_id)
            return

    def run(self):
        new_datas = self.get_data()
        for cate_id in new_datas:
            print("cate: %s, %s" % (cate_id, len(new_datas[cate_id])))
        for cate_id in new_datas:
            datas = new_datas[cate_id]
            self.down_cate(cate_id, datas)
        LoggerUtil.write_down_file_log(
            "%s, 竞店数据导出成功" %
            datetime.now().strftime("%Y-%m-%d"))


class MyEmail():
    def __init__(self,emial=["1782316637@qq.com"]):
        emial.append("526422057@qq.com")
        self.to_mail = emial
        self.subject = "尚智竞店数据以及流失数据每日更新"
        self.from_user = "何明耀<526422057@qq.com>"
        self.smtpserver = "smtp.qq.com"
        self.smtpport = 465
        self.from_mail = "1782316637@qq.com"
        self.password = "rittkttrmxqebffi"
    def send_main(self, content="", file_name=None):
        self.subject = content
        msg = MIMEMultipart()
        msg['From'] = Header(self.from_user, 'utf-8')
        msg['Subject'] = Header(self.subject, 'utf-8')
        part = MIMEText(content,'plain','utf-8')
        msg.attach(part)

        for file in file_name:
            with open(file,"rb") as f:
                attach = MIMEBase('application', 'octet-stream')
                attach.set_payload(f.read())
                attach.add_header('Content-Disposition', 'attachment', filename=file)
                encoders.encode_base64(attach)
                f.close()
            msg.attach(attach)
        emial_str = ""
        for m in self.to_mail:
            emial_str += m
            emial_str += ";"
        smtp = smtplib.SMTP_SSL(self.smtpserver, self.smtpport)
        try:
            smtp.login(self.from_mail, self.password)
            smtp.sendmail(self.from_mail, self.to_mail, msg.as_string())
            LoggerUtil.write_down_file_log("%s: 发送成功, 邮箱：%s" % (content,emial_str))
        except:
            LoggerUtil.write_down_file_log("[%s] -- [%s]" % (sys.exc_info()[0], sys.exc_info()[1]))
            LoggerUtil.write_down_file_log("%s: 发送失败, 邮箱：%s" % (content, emial_str))
        finally:
            smtp.close()

def downFile():
    loss_file_name = "商品流失分析%s.xls" % (datetime.now().strftime('%Y-%m-%d'))
    shop_file_name = "竞店数据导出%s.csv" % datetime.now().strftime("%Y-%m-%d")
    min_price = 0
    min_num = 5
    down_loss_thread = DownLossThread()
    down_shop_thread = DownShopThread()
    start_date = datetime.now()
    offset = timedelta(days=-1)
    start_date = "%s %s" % ((start_date + offset).strftime('%Y-%m-%d'), "14:00:00")
    end_date = "%s %s" % (datetime.now().strftime('%Y-%m-%d'), "14:00:00")
    down_loss_thread.set_args(start_date,end_date,file_name=loss_file_name)
    down_shop_thread.set_args(start_date, end_date,min_price=min_price,min_num=min_num, file_name=shop_file_name)
    down_loss_thread.start()
    down_shop_thread.start()
    down_loss_thread.join()
    down_shop_thread.join()
    LoggerUtil.write_down_file_log("所有邮件发送结束")


if __name__ == '__main__':
    downFile()