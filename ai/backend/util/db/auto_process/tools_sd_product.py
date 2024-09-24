import os

import pymysql
from ad_api.api import sponsored_display
from ad_api.base import Marketplaces
import json
import datetime
from decimal import Decimal
from ai.backend.util.db.configuration.path import get_config_path
from ai.backend.util.db.util.common import get_ad_my_credentials,get_proxies
from ai.backend.util.db.auto_process.base_api import BaseApi


class ProductTools(BaseApi):
    def __init__(self, db, brand, market):
        super().__init__(db, brand, market)

    def create_product_api(self,product_info):
        try:
            result = sponsored_display.ProductAds(credentials=self.credentials,
                                                  marketplace=Marketplaces[self.market.upper()],
                                                  access_token=self.access_token,
                                                  proxies=get_proxies(self.market),
                                                  debug=True).create_product_ads(
                    body=json.dumps(product_info))
        except Exception as e:
            print("add product failed: ", e)
            result = None
        adId = ""
        if result and result.payload[0]:
            adId = result.payload[0]["adId"]
            print("add product success,adId is :", adId)
            res = ["success",adId]
        else:
            print("add product failed:")
            res = ["failed",adId]
        return res



    def update_product_api(self,product_info):

        try:
            result = sponsored_display.ProductAds(credentials=self.credentials,
                                                  marketplace=Marketplaces[self.market.upper()],
                                                  access_token=self.access_token,
                                                  proxies=get_proxies(self.market),
                                                  debug=True).edit_product_ads(
                    body=json.dumps(product_info))
            print(result)
        except Exception as e:
            print("update product failed: ", e)
            result = None
        compaignID = ""
        if result and result.payload:
            campaignID = result.payload
            print("update product success", compaignID)
            res = ["success", campaignID]
        else:
            print(" update product failed:")
            res = ["failed", ""]
        # 返回创建的 compaignID
        return res

    def get_product_api(self, adGroupID):
        try:
            result = sponsored_display.ProductAds(credentials=self.credentials,
                                                  marketplace=Marketplaces[self.market.upper()],
                                                  access_token=self.access_token,
                                                  proxies=get_proxies(self.market),
                                                  debug=True).list_product_ads(
                adGroupIdFilter=adGroupID)
        except Exception as e:
            print("查找商品失败: ", e)
            result = None

        if result and result.payload:
            defaultBid_old = result.payload
            print(" 查找商品成功")
        else:
            print("查找商品失败:")
            defaultBid_old = None
        return defaultBid_old

    def get_creatives_api(self, adGroupID):
        try:
            result = sponsored_display.Creatives(credentials=self.credentials,
                                                 marketplace=Marketplaces[self.market.upper()],
                                                 access_token=self.access_token,
                                                 proxies=get_proxies(self.market),
                                                 debug=True).list_creatives(
                adGroupIdFilter=adGroupID)
        except Exception as e:
            print("查找创意素材失败: ", e)
            result = None

        if result and result.payload:
            defaultBid_old = result.payload
            print(" 查找创意素材成功")
        else:
            print("查找创意素材失败:")
            defaultBid_old = None
        return defaultBid_old

    def create_creatives_api(self, creatives_info):
        try:
            result = sponsored_display.Creatives(credentials=self.credentials,
                                                 marketplace=Marketplaces[self.market.upper()],
                                                 access_token=self.access_token,
                                                 proxies=get_proxies(self.market),
                                                 debug=True).create_creatives(
                body=json.dumps(creatives_info))
        except Exception as e:
            print("创建创意素材失败: ", e)
            result = None

        if result and result.payload[0]["creativeId"]:
            defaultBid_old = result.payload[0]["creativeId"]
            print(" 创建创意素材成功")
        else:
            print("创建创意素材失败:")
            defaultBid_old = None
        return defaultBid_old

#修改品测试

# pt=ProductTools('KAPEYDESI')
# # # res = pt.update_product_api(product_info)
# # # print(type(res))
# # # print(res)
# res = pt.get_creatives_api('SA',436764506244567)
# print(res)
#new_results = [{"asin": result["asin"]} for result in res]

# 打印新的列表
# print(new_results)
# pt=ProductTools('Veement')
# res = pt.get_product_api('UK','435116840242085')
# print(type(res))
# print(res)
