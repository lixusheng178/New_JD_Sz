#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
@Project : JD_Sz
@Time : 2019/11/12 0012 17:15
@Author : 孟凡不凡 (*^__^*)
@Email : 1782316637@qq.com
@File : sku_util.py
@Describe:
'''

from util.html_util import Html

class Sku():
    def __init__(self,sku_id=0):
        self.sku_id = sku_id

    def get_sku_args(self):
        url = "http://116.196.87.179/sku/searchBySkuNum?skuNum=%s" % self.sku_id
        resp = Html().get_json(url)
        if resp is None:
            return False
        if resp["code"] == 0:
            try:
                cate1_name = resp["data"]["cate1"]
                cate2_name = resp["data"]["cate2"]
                cate3_name = resp["data"]["cate3"]
                cateUrl = resp["data"]["cateUrl"]
                ""
                # http://list.jd.com/list.html?cat=9855,9858,13821
                cate = cateUrl.split("cat=")[-1]
                cate1_id, cate2_id,cate3_id = cate.split(",")
                return (cate1_id, cate2_id, cate3_id, cate1_name, cate2_name, cate3_name)
            except:
                return False
        else:
            print(resp["msg"])
            return False

if __name__ == '__main__':
    sku_id = "11066593741"
    Sku(sku_id).get_sku_args()