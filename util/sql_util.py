#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
@Project : jd
@Time : 2019/9/20 0020 14:45
@Author : 孟凡不凡 (*^__^*)
@Email : 1782316637@qq.com
@File : sql_util.py
@Describe:
'''

import sqlite3
import sys

import pymysql


class MySql():
    @staticmethod
    def get_conn():
        try:
            conn = pymysql.connect(host='116.196.120.31', user='tj', password='Toolslit2018', db='sz_tools',
                                   charset='utf8')
            return conn
        except:
            return False




class JDMySql():
    def __init__(self, w=None):
        self.w = w
        try:
            self.conn = sqlite3.connect('../user.db')

            create_jd_cmd = """
                CREATE TABLE IF NOT EXISTS jd_brand
            (ID INTEGER PRIMARY KEY AUTOINCREMENT,
            Brand NVARCHAR(100) NOT NULL,
            brand_id INT NOT NULL
            );
            """
            self.conn.execute(create_jd_cmd)
        except BaseException:
            self.w.log_thread.run_("京东品牌表创建失败")

    def insert_brand(self, brand=None, brand_id=0):
        self.cur = self.conn.cursor()
        self.cur.execute("select * from jd_brand where Brand = ?", (brand,))
        data = self.cur.fetchone()
        if data:
            self.w.log_thread.run_("品牌： %s， 库中已存在" % brand)
            self.cur.close()

        else:
            self.cur.execute(
                "insert into jd_brand (Brand, brand_id) values (?, ?);", (brand, brand_id))
            self.conn.commit()
            self.w.log_thread.run_("品牌词：%s, 插入成功" % brand)
            self.cur.close()

    def get_all_brand(self):
        self.cur = self.conn.cursor()
        self.cur.execute("select Brand, brand_id from jd_brand")
        data = self.cur.fetchall()
        self.cur.close()
        return data

    def close(self):
        self.conn.close()


class TbMysql():
    def __init__(self, w=None):
        self.w = w
        try:
            self.conn = sqlite3.connect('user.db')

            create_tb_cmd = """
                CREATE TABLE IF NOT EXISTS tb_brand
            (ID INTEGER PRIMARY KEY AUTOINCREMENT,
            buy_url_id INT NOT NULL,
            SHOP_NAME NVARCHAR(100) NOT NULL,
            price NVARCHAR(10),
            brand NVARCHAR(100) default NULL,
            file_id INT NOT NULL
            );
            """
            create_tb_file_cmd = """
                CREATE TABLE IF NOT EXISTS tb_brand_file (
                    ID INTEGER PRIMARY KEY AUTOINCREMENT,
                    file_name NVARCHAR(100) NOT NULL
                )
            """
            self.conn.execute(create_tb_cmd)
            self.conn.execute(create_tb_file_cmd)
        except BaseException:
            self.w.log_thread.run_(
                "[%s] -- [%s]" %
                (sys.exc_info()[0], sys.exc_info()[1]))
            self.w.log_thread.run_("淘宝品牌表创建失败")
            self.conn.rollback()
            return False

    def insert_tb(self, buy_url_id=0, shop_name=None,
                  price=0, brand=None, file_id=0):
        self.cur = self.conn.cursor()
        self.cur.execute(
            "select * from tb_brand where buy_url_id = ?;", (buy_url_id,))
        data = self.cur.fetchone()
        if data:
            self.w.log_thread.run_("buy_url_id: %s 库中已存在" % buy_url_id)
            self.cur.close()
        else:
            self.cur.execute(
                "insert into tb_brand (buy_url_id, SHOP_NAME, price, brand, file_id) values (?,?,?,?,?);",
                (buy_url_id,
                 shop_name,
                 price,
                 brand,
                 file_id))
            self.conn.commit()
            self.w.log_thread.run_("buy_url_id: %s, 插入成功" % buy_url_id)
            self.cur.close()

    def insert_file(self, file_name=None):
        self.cur = self.conn.cursor()
        self.cur.execute(
            "select * from tb_brand_file where  file_name = ?;", (file_name,))
        data = self.cur.fetchone()
        if data:
            self.w.log_thread.run_("文件： %s，已存入数据库中")
            self.cur.close()
        else:
            self.cur.execute(
                "insert into tb_brand_file (file_name) values (?);", (file_name,))
            self.conn.commit()
            self.w.log_thread.run_("文件： %s，已成功插入数据库")
            self.cur.close()

    def get_file_id(self, file_name=None):
        self.cur = self.conn.cursor()
        self.cur.execute(
            "select * from tb_brand_file where  file_name = ?;", (file_name,))
        data = self.cur.fetchone()
        if data:
            self.cur.close()
            return data[0]
        else:
            self.w.log_thread.run_("数据库中不存在这个文件")
            self.cur.close()
            return False

    def get_all_file(self):
        self.cur = self.conn.cursor()
        self.cur.execute("select * from tb_brand_file")
        data = self.cur.fetchall()
        self.cur.close()
        return data

    def update_brand(self, buy_url_id=0, brand=None):
        self.cur = self.conn.cursor()
        try:
            self.cur.execute(
                "update tb_brand set brand = ? where buy_url_id =?;", (brand, buy_url_id))
            self.conn.commit()
            self.w.log_thread.run_(
                "buy_url_id: %s, 品牌成功更新为： %s" %
                (buy_url_id, brand))
        except BaseException:
            self.w.log_thread.run_("buy_url_id:%s, 品牌更新失败" % buy_url_id)
        finally:
            self.cur.close()

    def get_not_data(self):
        self.cur = self.conn.cursor()
        self.cur.execute(
            "select buy_url_id, SHOP_NAME, price from tb_brand where brand is NULL ")
        data = self.cur.fetchall()
        self.cur.close()
        return data

    def get_all_brand(self, file_id=0):
        self.cur = self.conn.cursor()
        if file_id != 0:
            self.cur.execute(
                "select buy_url_id, SHOP_NAME, price, brand from tb_brand where brand is not NULL AND file_id = ?;",
                (file_id,
                 ))
            data = self.cur.fetchall()
        else:
            self.cur.execute(
                "select buy_url_id, SHOP_NAME, price, brand from tb_brand where brand is not NULL;")
            data = self.cur.fetchall()
        self.cur.close()
        return data

    def close(self):
        self.conn.close()


if __name__ == '__main__':
    # pass
    # jd_sql = JDMySql()
    # jd_sql.insert_brand("男装", "1")
    # print(jd_sql.get_all_brand())
    # jd_sql.close()

    print(MySql().get_conn())