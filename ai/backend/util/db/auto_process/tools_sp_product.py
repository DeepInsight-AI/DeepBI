import os

import pymysql
from ad_api.api import sponsored_products
from ad_api.base import Marketplaces
import json
import datetime
from decimal import Decimal
from ai.backend.util.db.util.common import get_ad_my_credentials,get_proxies
from ai.backend.util.db.auto_process.base_api import BaseApi


class ProductTools(BaseApi):
    def __init__(self, db, brand, market):
        super().__init__(db, brand, market)

    def create_product_api(self,product_info):
        try:
            result = sponsored_products.ProductAdsV3(credentials=self.credentials,
                                                     marketplace=Marketplaces[self.market.upper()],
                                                     access_token=self.access_token,
                                                     proxies=get_proxies(self.market),
                                                     debug=True).create_product_ads(
                    body=json.dumps(product_info))
        except Exception as e:
            print("add product failed: ", e)
            result = None
        adId = ""
        if result and result.payload["productAds"]["success"]:
            adId = result.payload["productAds"]["success"][0]["adId"]
            print("add product success,adId is :", adId)
            res = ["success",adId]
        else:
            print("add product failed:")
            res = ["failed",adId]
        return res

    def update_product_api(self,product_info):

        try:
            result = sponsored_products.ProductAdsV3(credentials=self.credentials,
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
        if result and result.payload["productAds"]["success"]:
            campaignID = result.payload["productAds"]["success"][0]["adId"]
            print("update product success", compaignID)
            res = ["success", campaignID]
        else:
            print(" update product failed:")
            res = ["failed", ""]
        # 返回创建的 compaignID
        return res

    def get_product_api(self, adGroupID):
        adGroup_info = {
            "adGroupIdFilter": {
                "include": [
                    str(adGroupID)
                ]
            }
        }
        try:
            result = sponsored_products.ProductAdsV3(credentials=self.credentials,
                                                     marketplace=Marketplaces[self.market.upper()],
                                                     access_token=self.access_token,
                                                     proxies=get_proxies(self.market),
                                                     debug=True).list_product_ads(
                body=json.dumps(adGroup_info))
        except Exception as e:
            print("查找商品失败: ", e)
            result = None

        if result and result.payload["productAds"]:
            defaultBid_old = result.payload["productAds"]
            print(" 查找商品成功")
        else:
            print("查找商品失败:")
            defaultBid_old = 1
        return defaultBid_old

#修改品测试

# pt=ProductTools('LAPASA')
# # # res = pt.update_product_api(product_info)
# # # print(type(res))
# # # print(res)
# res = pt.get_product_api('FR',392187134232726)
# print(res)

# pt=ProductTools('LAPASA')
# res = pt.get_product_api('FR','386959248314006')
# print(type(res))
# print(res)
