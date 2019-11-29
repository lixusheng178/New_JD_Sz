#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
@File  : log_util.py
@Author: 孟凡不凡
@Date  : 2019/7/9 11.json:58
@Desc  :  日志输出
'''
import os
import sys
from datetime import datetime as dt

# 日志显示框
from PyQt5.QtCore import QThread, pyqtSignal


class logThread(QThread):
    result = pyqtSignal(str)

    def __init__(self, parent=None):
        super(logThread, self).__init__(parent)

    def run_(self, message):
        # time.sleep(random.random() * 5)
        self.result.emit(message)


class BoxThread(QThread):
    result = pyqtSignal(tuple)

    def __init__(self, parent=None):
        super(BoxThread, self).__init__(parent)

    def run_(self, tup):
        self.result.emit(tup)


class LoggerUtil():
    @staticmethod
    def get_log_file():
        filename = "日志记录%s.txt" % (dt.now().strftime('%Y-%m-%d'))
        if not os.path.exists(filename):
            with open(filename, "w", encoding="gbk") as w:
                w.write("日志记录:\n")
        return filename

    """
       ocr每天扫描会生成新的日志文件
       """

    @staticmethod
    def write_file_log(info=None):
        if info is None:
            info = "[%s][%s]" % (sys.exc_info()[0], sys.exc_info()[1])
        date_str = dt.now().strftime('%Y-%m-%d %H:%M:%S')
        with open(LoggerUtil.get_log_file(), "a+", encoding="gbk") as f:
            try:
                f.write("[%s]:%s\n" % (date_str, info))

                print("[%s]:%s\n" % (date_str, info))
            except:
                pass

    @staticmethod
    def get_down_log_file():
        filename = "每日到文件日志记录%s.txt" % (dt.now().strftime('%Y-%m-%d'))
        if not os.path.exists(filename):
            with open(filename, "w", encoding="gbk") as w:
                w.write("日志记录:\n")
        return filename

    """
       ocr每天扫描会生成新的日志文件
       """

    @staticmethod
    def write_down_file_log(info=None):
        if info is None:
            info = "[%s][%s]" % (sys.exc_info()[0], sys.exc_info()[1])
        date_str = dt.now().strftime('%Y-%m-%d %H:%M:%S')
        with open(LoggerUtil.get_down_log_file(), "a+", encoding="gbk") as f:
            try:
                f.write("[%s]:%s\n" % (date_str, info))

                print("[%s]:%s\n" % (date_str, info))
            except:

                pass

if __name__ == '__main__':
    LoggerUtil.write_file_log("按时搜房")
