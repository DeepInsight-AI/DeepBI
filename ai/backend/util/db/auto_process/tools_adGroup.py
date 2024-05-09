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

class AdGroupTools:
    def __init__(self):
        self.my_credentials = dict(
        refresh_token='****',
        client_id='****',
        client_secret='****',
        profile_id='****',
    )

    # 新建广告组
    def create_adGroup_api(self,adGroup_info):
        try:
            result = sponsored_products.AdGroupsV3(credentials=my_credentials,
                                                            marketplace=Marketplaces.NA,
                                                            debug=True).create_ad_groups(
                body=json.dumps(adGroup_info))
        except Exception as e:
            print("create adGroup failed: ", e)
            result = None
        adGroupID = ""
        if result and result.payload["adGroups"]["success"]:
            adGroupID = result.payload["adGroups"]["success"][0]["adGroup"]["adGroupId"]
            print("create adGroup success,adGroupId is:", adGroupID)
            return ["success",adGroupID]
        else:
            print("create adGroup failed:", e)
            return ["failed", adGroupID]


    # 更新广告组
    def update_adGroup_api(self,adGroup_info):

        try:
            result = sponsored_products.AdGroupsV3(credentials=my_credentials,
                                                            marketplace=Marketplaces.NA,
                                                            debug=True).edit_ad_groups(
                body=json.dumps(adGroup_info))
        except Exception as e:
            print("update adGroup failed: ", e)
            result = None
        adGroupID = ""
        if result and result.payload["adGroups"]["success"]:
            adGroupID = result.payload["adGroups"]["success"][0]["adGroup"]["adGroupId"]
            print("update adGroup success,adGroupId is:", adGroupID)
            return ["success",adGroupID]
        else:
            print("update adGroup failed:", e)
            return ["failed", adGroupID]


    def get_adGroup_api(self,market,adGroupID):
        adGroup_info = {
  "maxResults": 10,
  "adGroupIdFilter": {
    "include": [
      adGroupID
    ]
  },
  "includeExtendedDataFields": True,
}
        try:
            result = sponsored_products.AdGroupsV3(credentials=my_credentials,
                                                            marketplace=Marketplaces.NA,
                                                            debug=True).list_ad_groups(
                body=json.dumps(adGroup_info))
        except Exception as e:
            print("查找广告组失败: ", e)
            result = None
        adGroupID = ""
        if result and result.payload["adGroups"][0]["defaultBid"]:
            defaultBid_old = result.payload["adGroups"][0]["defaultBid"]
            print("addgroup 创建查找广告组失败成功,adGroupId is:", adGroupID)
        else:
            print("查找广告组失败:", e)
            defaultBid_old=1
        return defaultBid_old



    # 广告组增加否定定向关键词
    def add_adGroup_negativekw(self,adGroup_negativekw_info):
        try:
            result = sponsored_products.NegativeKeywordsV3(credentials=my_credentials,
                                                            marketplace=Marketplaces.NA,
                                                            debug=True).create_negative_keyword(
                body=json.dumps(adGroup_negativekw_info))
        except Exception as e:
            print("add adGroup negative keyword failed: ", e)
            result = None
        if result and result.payload["negativeKeywords"]["success"]:
            negativeKeywordId = result.payload["negativeKeywords"]["success"][0]["negativeKeywordId"]
            print("add adGroup negative keyword success,negativeKeywordId is:", negativeKeywordId)
            return ["success",negativeKeywordId]
        else:
            print("add adGroup negative keyword failed:", e)
            return ["failed", ""]

    # 广告组修改否定定向关键词
    def update_adGroup_negativekw(self,adGroup_negativekw_info):
        try:
            result = sponsored_products.NegativeKeywordsV3(credentials=my_credentials,
                                                            marketplace=Marketplaces.NA,
                                                            debug=True).edit_negative_keyword(
                body=json.dumps(adGroup_negativekw_info))
        except Exception as e:
            print("update adGroup negative keyword failed: ", e)
            result = None
        if result and result.payload["negativeKeywords"]["success"]:
            negativeKeywordId = result.payload["negativeKeywords"]["success"][0]["negativeKeywordId"]
            print("update adGroup negative keyword success,negativeKeywordId is:", negativeKeywordId)
            return ["success",negativeKeywordId]
        else:
            print("update adGroup negative keyword failed:", e)
            return ["failed", ""]
# 广告组更新否定关键词测试
# adGroup_negativekw_info = {
# "negativeKeywords": [
# {
# "keywordId": "426879824500654",
# "state": "ENABLED"
# }
# ]
# }
# agt=AdGroupTools()
# res = agt.update_adGroup_negativekw(adGroup_negativekw_info)
# print(type(res))
# print(res)

# 新增广告组否定定向关键词测试 --426879824500654
# adGroup_negativekw_info = {
#   "negativeKeywords": [
#     {
#       "campaignId": "531571979684792",
#       "matchType": "NEGATIVE_EXACT",
#       "state": "ENABLED",
#       "adGroupId": "311043566627712",
#       "keywordText": "0508negativekw01"
#     }
#   ]
# }
# agt=AdGroupTools()
# res = agt.add_adGroup_negativekw(adGroup_negativekw_info)
# print(type(res))
# print(res)

# 测试
# adgroup_info={
#   "adGroups": [
#     {
#       "name": "adgroupB09ZQLY99J",
#       "state": "PAUSED",
#       "adGroupId": "311043566627712",
#       "defaultBid": 1.0
#     }
#   ]
# }
#
# agt=AdGroupTools()
# # 测试更新广告系列信息
# res = agt.get_adGroup_api('US','311043566627712')
# print(type(res))
# print(res)
