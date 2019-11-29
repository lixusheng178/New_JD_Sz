#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
@Project : New_JD_Sz
@Time : 2019/11/26 0026 18:36
@Author : 孟凡不凡 (*^__^*)
@Email : 1782316637@qq.com
@File : login_class.py
@Describe:
'''
import json

from PyQt5 import QtWidgets, QtCore, QtGui
from PyQt5.QtCore import QUrl, QTimer, Qt
from PyQt5.QtWebEngineWidgets import QWebEngineView, QWebEngineProfile
from PyQt5.QtWidgets import QHBoxLayout, QMessageBox, QLineEdit
from bs4 import BeautifulSoup
from redis import StrictRedis


from util.html_util import Html
from util.sql_util import MySql
from datetime import datetime as dt
sz_login_url = "https://sz.jd.com/login.html?ReturnUrl=http://sz.jd.com/index.html"
rs = StrictRedis(host="116.196.91.160", port=15000, db=100, password="Summer001")


class Ui_Sz_LoginForm(object):
    def setupUi(self, Sz_LoginForm):
        Sz_LoginForm.setObjectName("Sz_LoginForm")
        Sz_LoginForm.resize(1400, 800)
        self.form = Sz_LoginForm
        self.web_widget = QtWidgets.QWidget(Sz_LoginForm)
        self.web_widget.setGeometry(QtCore.QRect(60, 70, 1200, 600))
        self.web_widget.setObjectName("web_widget")
        self.label = QtWidgets.QLabel(Sz_LoginForm)
        self.label.setGeometry(QtCore.QRect(70, 30, 401, 21))
        self.label.setObjectName("label")
        self.ShopNameLabel = QtWidgets.QLabel(Sz_LoginForm)
        self.ShopNameLabel.setGeometry(QtCore.QRect(830, 30, 251, 21))
        self.ShopNameLabel.setText("")
        self.ShopNameLabel.setObjectName("ShopNameLabel")
        self.login_out_btn = QtWidgets.QPushButton(Sz_LoginForm)
        self.login_out_btn.setGeometry(QtCore.QRect(1120, 30, 131, 31))
        self.login_out_btn.setObjectName("login_out_btn")

        self.shop_box = QtWidgets.QComboBox(Sz_LoginForm)
        self.shop_box.setGeometry(QtCore.QRect(60, 720, 251, 31))
        self.shop_box.setObjectName("shop_box")
        self.shop_box.addItem("")
        self.user_edit = QtWidgets.QLineEdit(Sz_LoginForm)
        self.user_edit.setGeometry(QtCore.QRect(460, 720, 171, 31))
        self.user_edit.setObjectName("user_edit")
        self.label_2 = QtWidgets.QLabel(Sz_LoginForm)
        self.label_2.setGeometry(QtCore.QRect(380, 730, 71, 16))
        font = QtGui.QFont()
        font.setPointSize(12)
        self.label_2.setFont(font)
        self.label_2.setObjectName("label_2")
        self.password_edit = QtWidgets.QLineEdit(Sz_LoginForm)
        self.password_edit.setGeometry(QtCore.QRect(770, 720, 171, 31))
        self.password_edit.setText("")
        self.password_edit.setObjectName("password_edit")
        self.label_3 = QtWidgets.QLabel(Sz_LoginForm)
        self.label_3.setGeometry(QtCore.QRect(690, 730, 71, 16))
        font = QtGui.QFont()
        font.setPointSize(12)
        self.label_3.setFont(font)
        self.label_3.setObjectName("label_3")
        self.retranslateUi(Sz_LoginForm)
        QtCore.QMetaObject.connectSlotsByName(Sz_LoginForm)

    def retranslateUi(self, Sz_LoginForm):
        _translate = QtCore.QCoreApplication.translate
        Sz_LoginForm.setWindowTitle(_translate("Sz_LoginForm", "京东尚智登录获取cookies小工具"))
        self.label.setText(_translate("Sz_LoginForm", "请在下面的浏览器插件中登录您的账号："))
        self.login_out_btn.setText(_translate("Sz_LoginForm", "退出当前账号"))
        self.shop_box.setItemText(0, _translate("Sz_LoginForm", "登录店铺名"))
        self.label_2.setText(_translate("Sz_LoginForm", "账号："))
        self.label_3.setText(_translate("Sz_LoginForm", "密码："))
class Ui_Form(object):
    def setupUi(self, Form):
        Form.setObjectName("Form")
        Form.resize(500, 400)
        self.form = Form
        self.main_widget = QtWidgets.QWidget(Form)
        self.main_widget.setGeometry(QtCore.QRect(50, 30, 721, 511))
        self.main_widget.setObjectName("main_widget")

        self.retranslateUi(Form)
        QtCore.QMetaObject.connectSlotsByName(Form)

    def retranslateUi(self, Form):
        _translate = QtCore.QCoreApplication.translate
        Form.setWindowTitle(_translate("Form", "Form"))


class Ui_login_dialog(object):
    def setupUi(self, login_dialog):
        login_dialog.setObjectName("login_dialog")
        login_dialog.resize(396, 292)
        self.dialog = login_dialog
        self.layoutWidget = QtWidgets.QWidget(login_dialog)
        self.layoutWidget.setGeometry(QtCore.QRect(70, 160, 241, 31))
        self.layoutWidget.setObjectName("layoutWidget")
        self.horizontalLayout_2 = QtWidgets.QHBoxLayout(self.layoutWidget)
        self.horizontalLayout_2.setContentsMargins(0, 0, 0, 0)
        self.horizontalLayout_2.setObjectName("horizontalLayout_2")
        self.password_label = QtWidgets.QLabel(self.layoutWidget)
        self.password_label.setObjectName("password_label")
        self.horizontalLayout_2.addWidget(self.password_label)
        spacerItem = QtWidgets.QSpacerItem(40, 20, QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Minimum)
        self.horizontalLayout_2.addItem(spacerItem)
        self.password_edit = QtWidgets.QLineEdit(self.layoutWidget)
        self.password_edit.setObjectName("password_edit")
        self.horizontalLayout_2.addWidget(self.password_edit)
        self.login_btn = QtWidgets.QPushButton(login_dialog)
        self.login_btn.setGeometry(QtCore.QRect(60, 220, 251, 41))
        self.login_btn.setObjectName("login_btn")
        self.layoutWidget_2 = QtWidgets.QWidget(login_dialog)
        self.layoutWidget_2.setGeometry(QtCore.QRect(70, 100, 241, 31))
        self.layoutWidget_2.setObjectName("layoutWidget_2")
        self.horizontalLayout = QtWidgets.QHBoxLayout(self.layoutWidget_2)
        self.horizontalLayout.setContentsMargins(0, 0, 0, 0)
        self.horizontalLayout.setObjectName("horizontalLayout")
        self.user_label = QtWidgets.QLabel(self.layoutWidget_2)
        self.user_label.setObjectName("user_label")
        self.horizontalLayout.addWidget(self.user_label)
        spacerItem1 = QtWidgets.QSpacerItem(40, 20, QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Minimum)
        self.horizontalLayout.addItem(spacerItem1)
        self.user_edit = QtWidgets.QLineEdit(self.layoutWidget_2)
        self.user_edit.setObjectName("user_edit")
        self.horizontalLayout.addWidget(self.user_edit)
        self.tipslabel = QtWidgets.QLabel(login_dialog)
        self.tipslabel.setGeometry(QtCore.QRect(50, 40, 251, 21))
        self.tipslabel.setObjectName("tipslabel")

        self.retranslateUi(login_dialog)
        QtCore.QMetaObject.connectSlotsByName(login_dialog)

    def retranslateUi(self, login_dialog):
        _translate = QtCore.QCoreApplication.translate
        login_dialog.setWindowTitle(_translate("login_dialog", "Dialog"))
        self.password_label.setText(_translate("login_dialog", "密 码："))
        self.login_btn.setText(_translate("login_dialog", "登 录"))
        self.user_label.setText(_translate("login_dialog", "账 号："))
        self.tipslabel.setText(_translate("login_dialog", "TextLabel"))


class MyWebEngineView(QWebEngineView):
    def __init__(self, *args, **kwargs):
        super(MyWebEngineView, self).__init__(*args, **kwargs)
        # 绑定cookie被添加的信号槽
        QWebEngineProfile.defaultProfile().cookieStore().cookieAdded.connect(self.onCookieAdd)
        self.cookies = {}  # 存放cookie字典

    def onCookieAdd(self, cookie):  # 处理cookie添加的事件
        name = cookie.name().data().decode('utf-8')  # 先获取cookie的名字，再把编码处理一下
        value = cookie.value().data().decode('utf-8')  # 先获取cookie值，再把编码处理一下
        self.cookies[name] = value  # 将cookie保存到字典里

    # 获取cookie
    def get_cookie(self):
        cookie_str = ''
        for key, value in self.cookies.items():  # 遍历字典
            cookie_str += (key + '=' + value + ';')  # 将键值对拿出来拼接一下
        return cookie_str  # 返回拼接好的字符串

    def clear_cookies(self):
        self.page().profile().clearHttpCache()
        self.page().profile().cookieStore().deleteAllCookies()
        self.cookies = {}


class SzLoginClass(QtWidgets.QMainWindow, Ui_Form):
    def __init__(self,parent=None):
        super(SzLoginClass, self).__init__(parent)
        self.setupUi(self)
        self.hbox = QHBoxLayout()
        self.desktop = QtWidgets.QApplication.desktop()

        self.main_widget.setLayout(self.hbox)
        self.initial_dialog()


    def initial_dialog(self):
        self.login_form = QtWidgets.QDialog()
        self.login_dialog = Ui_login_dialog()
        self.login_dialog.setupUi(self.login_form)
        self.login_dialog.password_edit.setEchoMode(QLineEdit.Password)
        self.login_dialog.tipslabel.setText("")
        self.login_dialog.login_btn.clicked.connect(self.user_login)
        self.login_form.show()
        self.hbox.addWidget(self.login_form)
        self.main_widget.resize(396, 292)
        self.resize(500, 400)

    def user_login(self):
        user_name = self.login_dialog.user_edit.text()
        if user_name is None or user_name.strip()=="":
            self.login_dialog.tipslabel.setText("你的账号为空")
            return
        password =self.login_dialog.password_edit.text()
        if password is None or password.strip() =="":
            self.login_dialog.tipslabel.setText("你的密码未输入")
            return

        conn = MySql().get_conn()
        cur = conn.cursor()
        cur.execute("select id, password from user where id = %s", (user_name,) )
        data = cur.fetchone()
        cur.close()
        conn.close()
        if data:
            d_password = data[1]
            if password == d_password:
                self.user_id = user_name
                self.login_to_sz()
            else:
                self.login_dialog.tipslabel.setText("你输入的账号或者密码不正确，请重新输入")
        else:
            self.login_dialog.tipslabel.setText("你输入的账号不存在，请核实后登录")



    def login_to_sz(self):
        if self.hbox.indexOf(self.login_form) != -1:
            self.login_form.close()
            self.hbox.removeWidget(self.login_form)
            self.sz_form = QtWidgets.QDialog()
            self.sz_dialog = Ui_Sz_LoginForm()
            self.sz_dialog.setupUi(self.sz_form)
            self.sz_hbox = QHBoxLayout()
            self.sz_dialog.web_widget.setLayout(self.sz_hbox)
            self.sz_dialog.web = MyWebEngineView()
            self.sz_dialog.web.resize(1200, 600)
            self.sz_dialog.web.load(QUrl(sz_login_url))
            self.sz_dialog.web.show()
            self.sz_hbox.addWidget(self.sz_dialog.web)
            self.sz_dialog.xxx = ""
            self.sz_dialog.login_out_btn.setEnabled(False)
            self.sz_dialog.is_login_timer = QTimer(self)
            self.sz_dialog.is_login_timer.timeout.connect(self.is_login)
            self.sz_dialog.is_login_timer.start(1000)
            self.add_shop_box()
            self.sz_dialog.login_out_btn.clicked.connect(self.login_out)
            self.sz_form.show()
            self.hbox.addWidget(self.sz_form)
            self.main_widget.resize(1400, 800)
            self.resize(1400, 900)
            x = (self.desktop.width() - self.width()) // 2
            y = (self.desktop.height() - self.height()) // 2
            self.move(x, y)
        else:
            self.sz_form.close()
            self.hbox.removeWidget(self.sz_form)
            self.login_form = QtWidgets.QDialog()
            self.login_dialog = Ui_login_dialog()
            self.login_dialog.setupUi(self.login_form)
            self.login_dialog.tipslabel.setText("")
            self.login_dialog.login_btn.clicked.connect(self.user_login)
            self.login_form.show()
            self.hbox.addWidget(self.login_form)
            self.main_widget.resize(396, 292)
            self.resize(500, 400)
            x = (self.desktop.width() - self.width()) // 2
            y = (self.desktop.height() - self.height()) // 2
            self.move(x, y)

    def add_shop_box(self):
        conn = MySql().get_conn()
        cur = conn.cursor()
        cur.execute("select own_shop_name,user, password from sz_own_shop where shop_state = 1 and user_id=%s", (self.user_id))
        self.shop_data = cur.fetchall()
        cur.close()
        conn.close()
        for own_shop_name, user, password in self.shop_data:
            self.sz_dialog.shop_box.addItem(user)

        self.sz_dialog.shop_box.currentIndexChanged.connect(self.change_shop)

    def change_shop(self):
        shop_index = self.sz_dialog.shop_box.currentIndex()
        if shop_index != 0:
            own_shop_name, user, passwords = self.shop_data[shop_index - 1]
            self.sz_dialog.user_edit.setText(user)
            self.sz_dialog.password_edit.setText(passwords)

    def get_info(self):
        self.sz_dialog.web.page().toHtml(self.x)
        content = self.sz_dialog.xxx
    def x(self,html):
        self.sz_dialog.xxx =html

    def is_login(self):
        self.get_info()
        soup = BeautifulSoup(self.sz_dialog.xxx, "lxml")
        shop_name_span = soup.find("span", {"ng-model":"userName"})
        if shop_name_span:
            self.cookies = self.sz_dialog.web.get_cookie()
            self.shop_name = shop_name_span.find("a").attrs["title"]
            self.shop_id = int(shop_name_span.find("a").attrs["href"].split("/")[-1].split(".")[0].split("-")[-1])
            self.sz_dialog.ShopNameLabel.setText(self.shop_name)
            if self.save_cookies() is False:
                self.sz_dialog.label.setText("你的账号下未录入该店，请录入后重新登录" )
            else:
                self.sz_dialog.label.setText("您的店铺：%s， cookies已经保存成功" % self.shop_name)
            self.sz_dialog.login_out_btn.setEnabled(True)
        else:
            pass

    def get_user(self):
        url = "https://sz.jd.com/sz/api/personalCenter/getBasicInforData.ajax"
        host = "sz.jd.com"
        referer = "https://sz.jd.com/sz/view/personalCenter/basicInfor.html"
        resp = Html().get_html(url, host=host,referer=referer,cookies=self.cookies)
        if resp is None:
            return False
        try:
            self.user = json.loads(resp)["content"]["basicInfor"]["userPin"]
        except:
            return False


    def save_cookies(self):
        if self.get_user() is False:
            QMessageBox.information(self, "提示", "店铺账号抓取异常")
            return

        conn = MySql().get_conn()
        cur = conn.cursor()
        cur.execute("select cate  from sz_own_shop where user = %s and user_id=%s;", (self.user,self.user_id))
        data = cur.fetchone()
        cur.execute("update sz_own_shop set own_shop_id = %s, own_shop_name = %s where user = %s and user_id=%s;",
                    (self.shop_id, self.shop_name, self.user, self.user_id))
        conn.commit()
        if data:
            if data[0]:
                cate = data[0]
                old_value = json.loads(rs.get("out_sz_shop_redis").decode())

                if self.shop_name in old_value:
                    shop_flag = old_value[self.shop_name]["shop_flag"]
                else:
                    shop_flag = None
                value = {"cookies": self.cookies, "flag": 1, "shop_flag": shop_flag,
                         "update_date": dt.now().strftime('%Y-%m-%d %H:%M:%S'), "shop_id": self.shop_id, "cate": cate,"user_id": self.user_id}
                old_value[self.shop_name] = value
                rs.set("sz_shop_redis", json.dumps(old_value, ensure_ascii=False))
            else:
                form = QtWidgets.QDialog()
                flags = Qt.WindowMinMaxButtonsHint
                form.setWindowFlag(flags)
                ui = Shop_Parmas_Class()
                ui.setupUi(form)
                ui.set_value(self.cookies, self.shop_id, self.shop_name, self.user,self.user_id)
                form.show()
                form.exec_()
                self.form.show()
            cur.close()
            conn.close()
            self.sz_dialog.is_login_timer.stop()
        else:
            cur.close()
            conn.close()
            self.sz_dialog.is_login_timer.stop()
            return False


    def login_out(self):
        self.sz_dialog.xxx = ""
        self.sz_dialog.ShopNameLabel.setText("")
        self.sz_dialog.web.clear_cookies()
        self.sz_dialog.label.setText("请在下面的浏览器插件中登录您的账号：")
        self.sz_dialog.web.load(QUrl(sz_login_url))
        self.sz_dialog.login_out_btn.setEnabled(False)
        self.sz_dialog.is_login_timer.start(1000)


class Shop_Params_Dialog(object):
    def setupUi(self, Dialog):
        Dialog.setObjectName("Dialog")
        Dialog.resize(293, 180)
        self.dialog = Shop_Params_Dialog
        self.cate_box = QtWidgets.QComboBox(Dialog)
        self.cate_box.setGeometry(QtCore.QRect(110, 70, 161, 31))
        self.cate_box.setObjectName("cate_box")
        self.push_btn = QtWidgets.QPushButton(Dialog)
        self.push_btn.setGeometry(QtCore.QRect(30, 120, 241, 31))
        self.push_btn.setObjectName("push_btn")
        self.label = QtWidgets.QLabel(Dialog)
        self.label.setGeometry(QtCore.QRect(30, 80, 71, 16))
        self.label.setObjectName("label")
        self.label_3 = QtWidgets.QLabel(Dialog)
        self.label_3.setGeometry(QtCore.QRect(20, 30, 221, 16))
        self.label_3.setObjectName("label_3")

        self.retranslateUi(Dialog)
        QtCore.QMetaObject.connectSlotsByName(Dialog)

    def retranslateUi(self, Dialog):
        _translate = QtCore.QCoreApplication.translate
        Dialog.setWindowTitle(_translate("Dialog", "参数绑定"))
        self.push_btn.setText(_translate("Dialog", "提 交"))
        self.label.setText(_translate("Dialog", "选择类目："))
        self.label_3.setText(_translate("Dialog", "请在下面选择该店的类目以及绑定的邮箱"))


class Shop_Parmas_Class(QtWidgets.QWidget, Shop_Params_Dialog):
    def __init__(self,parent=None):
        super(Shop_Parmas_Class, self).__init__(parent)

    def set_value(self, cookies=None, shop_id=0, shop_name=None, user=None,user_id=0):
        self.cookies = cookies
        self.shop_id = shop_id
        self.shop_name = shop_name
        self.user = user
        self.user_id = user_id
        self.close_btn = QtWidgets.QPushButton(self)
        self.close_btn.clicked.connect(self.close)
        self.close_btn.setHidden(False)
        self.add_cate_item()
        self.push_btn.clicked.connect(self.push)

    def add_cate_item(self):
        conn = MySql().get_conn()
        cur = conn.cursor()
        cur.execute("select id, name from jd_category where lev = 1")
        self.cate_data = cur.fetchall()
        cur.close()
        conn.close()
        for cate_id,cate_name in self.cate_data:
            self.cate_box.addItem(cate_name)


    def push(self):
        cate_index = self.cate_box.currentIndex()
        cate_id = self.cate_data[cate_index][0]

        old_value = json.loads(rs.get("out_sz_shop_redis").decode())

        if self.shop_name in old_value:
            shop_flag = old_value[self.shop_name]["shop_flag"]
        else:
            shop_flag = None
        value = {"cookies": self.cookies, "flag": 1, "shop_flag": shop_flag,
                 "update_date": dt.now().strftime('%Y-%m-%d %H:%M:%S'), "shop_id": self.shop_id, "cate": cate_id,"user_id": self.user_id}
        old_value[self.shop_name] = value
        rs.set("out_sz_shop_redis", json.dumps(old_value, ensure_ascii=False))
        conn = MySql().get_conn()
        cur = conn.cursor()
        cur.execute("update sz_own_shop set own_shop_id = %s, own_shop_name = %s, cate = %s where user = %s and user_id=%s", (self.shop_id,self.shop_name, cate_id,self.user, self.user_id))
        conn.commit()
        cur.close()
        conn.close()
        self.label_3.setText("该店铺已经保存成功，请关闭该窗口")


if __name__ == "__main__":
    import sys
    app = QtWidgets.QApplication(sys.argv)
    mainWindow = SzLoginClass()
    mainWindow.show()
    sys.exit(app.exec_())