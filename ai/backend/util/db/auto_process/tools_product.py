import pymysql
from ad_api.api import sponsored_products
from ad_api.base import Marketplaces
import json
import datetime
from decimal import Decimal

my_credentials = dict(
        refresh_token='****',
        client_id='****',
        client_secret='****',
        profile_id='****',
    )

class ProductTools:
    def __init__(self):
        self.my_credentials = dict(
        refresh_token='****',
        client_id='****',
        client_secret='****',
        profile_id='****',
    )
    def create_product_api(self,product_info):
        try:
            result = sponsored_products.ProductAdsV3(credentials=my_credentials,
                                                                 marketplace=Marketplaces.NA,
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
            result = sponsored_products.ProductAdsV3(credentials=my_credentials,
                                                                 marketplace=Marketplaces.NA,
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

#修改品测试

# pt=ProductTools()
# res = pt.update_product_api(product_info)
# print(type(res))
# print(res)


# pt=ProductTools()
# res = pt.create_product_api(product_info)
# print(type(res))
# print(res)
