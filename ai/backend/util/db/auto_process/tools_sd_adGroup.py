import os

import pymysql
from ad_api.api import sponsored_display
from ad_api.base import Marketplaces
import json
import datetime
from decimal import Decimal
from ai.backend.util.db.configuration.path import get_config_path
from ai.backend.util.db.util.common import get_ad_my_credentials,get_proxies


class AdGroupTools_SD:
    def __init__(self,brand):
        self.brand = brand

    def load_credentials(self,market):
        my_credentials,access_token = get_ad_my_credentials(market,self.brand)
        return my_credentials,access_token


    # 新建广告组
    def create_adGroup_api(self,adGroup_info,market):
        try:
            credentials, access_token = self.load_credentials(market)
            result = sponsored_display.AdGroups(credentials=credentials,
                                                marketplace=Marketplaces[market.upper()],
                                                access_token=access_token,
                                                proxies=get_proxies(market),
                                                debug=True).create_ad_groups(
                body=json.dumps(adGroup_info))
        except Exception as e:
            print("create adGroup failed: ", e)
            result = None
        adGroupID = ""
        if result and result.payload[0]["adGroupId"]:
            adGroupID = result.payload[0]["adGroupId"]
            print("create adGroup success,adGroupId is:", adGroupID)
            return ["success", adGroupID]
        else:
            print("create adGroup failed")
            return ["failed", adGroupID]


    # 更新广告组
    def update_adGroup_api(self,adGroup_info):

        try:
            result = sponsored_display.AdGroups(credentials=my_credentials,
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

        try:
            credentials, access_token = self.load_credentials(market)
            result = sponsored_display.AdGroups(credentials=credentials,
                                                marketplace=Marketplaces[market.upper()],
                                                access_token=access_token,
                                                proxies=get_proxies(market),
                                                debug=True).get_ad_group(
                str(adGroupID))
        except Exception as e:
            print("查找广告组失败: ", e)
            result = None
        adGroupID = ""
        if result and result.payload["adGroupId"]:
            defaultBid_old = result.payload
            print("addgroup 查找广告组成功,adGroupId is:", adGroupID)
        else:
            print("查找广告组失败")
            defaultBid_old = None
        return defaultBid_old

    def get_adGroup_bycampaignid_api(self,market,campaignid):

        try:
            credentials, access_token = self.load_credentials(market)
            result = sponsored_display.AdGroups(credentials=credentials,
                                                marketplace=Marketplaces[market.upper()],
                                                access_token=access_token,
                                                proxies=get_proxies(market),
                                                debug=True).list_ad_groups(
                campaignIdFilter=str(campaignid))
        except Exception as e:
            print("查找广告组失败: ", e)
            result = None
        adGroupID = ""
        if result and result.payload:
            defaultBid_old = result.payload
            print("addgroup 查找广告组成功,adGroupId is:", adGroupID)
        else:
            print("查找广告组失败")
            defaultBid_old = None
        return defaultBid_old

    def get_adGroup_negativekw(self, market, adGroupID):
        adGroup_info = {
            "maxResults": 500,
            "adGroupIdFilter": {
                "include": [
                    str(adGroupID)
                ]
            },
            "includeExtendedDataFields": True,
        }
        try:
            credentials, access_token = self.load_credentials(market)
            result = sponsored_products.NegativeKeywordsV3(credentials=credentials,
                                                           marketplace=Marketplaces[market.upper()],
                                                           access_token=access_token,
                                                           proxies=get_proxies(market),
                                                           debug=True).list_negative_keywords(
                body=json.dumps(adGroup_info))
        except Exception as e:
            print("查找广告组否定关键词失败: ", e)
            result = None
        if result and result.payload["negativeKeywords"]:
            negativeKeywords = result.payload["negativeKeywords"]
            print("addgroup 查找广告组否定关键词成功")
        else:
            print("查找广告组失败:")
            negativeKeywords = None
        return negativeKeywords

    # 广告组增加否定定向关键词
    def add_adGroup_negativekw(self,adGroup_negativekw_info,market):
        try:
            credentials, access_token = self.load_credentials(market)
            result = sponsored_products.NegativeKeywordsV3(credentials=credentials,
                                                           marketplace=Marketplaces[market.upper()],
                                                           access_token=access_token,
                                                           proxies=get_proxies(market),
                                                           debug=True).create_negative_keyword(
                body=json.dumps(adGroup_negativekw_info))
        except Exception as e:
            print("add adGroup negative keyword failed: ", e)
            result = None
        if result and result.payload["negativeKeywords"]["success"][0]["negativeKeywordId"]:
            negativeKeywordId = result.payload["negativeKeywords"]["success"][0]["negativeKeywordId"]
            print("add adGroup negative keyword success,negativeKeywordId is:", negativeKeywordId)
            return ["success",negativeKeywordId]
        else:
            print("add adGroup negative keyword failed:")
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

    #
    def list_adGroup_negative_pd(self, adGroup_negativekw_info, market):
        try:
            credentials, access_token = self.load_credentials(market)
            result = sponsored_products.NegativeTargetsV3(credentials=credentials,
                                                          marketplace=Marketplaces[market.upper()],
                                                          access_token=access_token,
                                                          proxies=get_proxies(market),
                                                          debug=True).list_negative_product_targets(
                body=json.dumps(adGroup_negativekw_info))
        except Exception as e:
            print("list adGroup negative product failed: ", e)
            result = None
        if result:
            print("list adGroup negative product success")
            return ["success", result]
        # if result and result.payload["negativeKeywords"]["success"]:
        #     negativeKeywordId = result.payload["negativeKeywords"]["success"][0]["negativeKeywordId"]
        #     print("add adGroup negative keyword success,negativeKeywordId is:", negativeKeywordId)
        #     return ["success", negativeKeywordId]
        else:
            print("list adGroup negative product failed:", e)
            return ["failed", ""]

    def list_adGroup_Targeting(self, market, adGroupID):
        try:
            credentials, access_token = self.load_credentials(market)

            result = sponsored_display.Targets(credentials=credentials,
                                               marketplace=Marketplaces[market.upper()],
                                               access_token=access_token,
                                               proxies=get_proxies(market),
                                               debug=True).list_products_targets(
                adGroupIdFilter=adGroupID)
        except Exception as e:
            print("list adGroup Targeting failed: ", e)
            result = None
        if result and result.payload:
            print("list adGroup Targeting success")
            return result.payload
        # if result and result.payload["negativeKeywords"]["success"]:
        #     negativeKeywordId = result.payload["negativeKeywords"]["success"][0]["negativeKeywordId"]
        #     print("add adGroup negative keyword success,negativeKeywordId is:", negativeKeywordId)
        #     return ["success", negativeKeywordId]
        else:
            print("list adGroup TargetingClause failed")
            return ["failed", ""]

    def list_adGroup_Targeting_by_targetId(self, market, targetId):
        try:
            credentials, access_token = self.load_credentials(market)

            result = sponsored_display.Targets(credentials=credentials,
                                               marketplace=Marketplaces[market.upper()],
                                               access_token=access_token,
                                               proxies=get_proxies(market),
                                               debug=True).get_products_target(
                targetId=targetId)
            if result and result.payload:
                print("list adGroup Targeting success")
                return result.payload
            # if result and result.payload["negativeKeywords"]["success"]:
            #     negativeKeywordId = result.payload["negativeKeywords"]["success"][0]["negativeKeywordId"]
            #     print("add adGroup negative keyword success,negativeKeywordId is:", negativeKeywordId)
            #     return ["success", negativeKeywordId]
            else:
                print("list adGroup TargetingClause failed")
                return ["failed", ""]
        except Exception as e:
            print("list adGroup Targeting failed: ", e)
            return ["failed", ""]

    def list_adGroup_Targeting_by_campaignId(self, market, campaignId):
        try:
            credentials, access_token = self.load_credentials(market)

            result = sponsored_display.Targets(credentials=credentials,
                                               marketplace=Marketplaces[market.upper()],
                                               access_token=access_token,
                                               proxies=get_proxies(market),
                                               debug=True).list_products_targets(
                campaignIdFilter=campaignId)
            if result and result.payload:
                print("list adGroup Targeting success")
                return result.payload
            # if result and result.payload["negativeKeywords"]["success"]:
            #     negativeKeywordId = result.payload["negativeKeywords"]["success"][0]["negativeKeywordId"]
            #     print("add adGroup negative keyword success,negativeKeywordId is:", negativeKeywordId)
            #     return ["success", negativeKeywordId]
            else:
                print("list adGroup TargetingClause failed")
                return ["failed", ""]
        except Exception as e:
            print("list adGroup Targeting failed: ", e)
            return ["failed", ""]

    def create_adGroup_Targeting(self, adGroup_info, market):
        try:
            credentials, access_token = self.load_credentials(market)
            result = sponsored_display.Targets(credentials=credentials,
                                               marketplace=Marketplaces[market.upper()],
                                               access_token=access_token,
                                               proxies=get_proxies(market),
                                               debug=True).create_products_targets(
                body=json.dumps(adGroup_info))
        except Exception as e:
            print("create adGroup TargetingClause failed: ", e)
            result = None
        if result and result.payload[0]["targetId"]:
            print("create adGroup TargetingClause success")
            return ["success", result.payload[0]["targetId"] ]
        # if result and result.payload["negativeKeywords"]["success"]:
        #     negativeKeywordId = result.payload["negativeKeywords"]["success"][0]["negativeKeywordId"]
        #     print("add adGroup negative keyword success,negativeKeywordId is:", negativeKeywordId)
        #     return ["success", negativeKeywordId]
        else:
            print("create adGroup TargetingClause failed")
            return ["failed", ""]

    def update_adGroup_Targeting(self, adGroup_info, market):
        try:
            credentials, access_token = self.load_credentials(market)
            result = sponsored_display.Targets(credentials=credentials,
                                               marketplace=Marketplaces[market.upper()],
                                               access_token=access_token,
                                               proxies=get_proxies(market),
                                               debug=True).edit_products_targets(
                body=json.dumps(adGroup_info))

            if result and result.payload[0]["targetId"]:
                print("update adGroup TargetingClause success")
                return ["success", result.payload[0]["targetId"] ]
            # if result and result.payload["negativeKeywords"]["success"]:
            #     negativeKeywordId = result.payload["negativeKeywords"]["success"][0]["negativeKeywordId"]
            #     print("add adGroup negative keyword success,negativeKeywordId is:", negativeKeywordId)
            #     return ["success", negativeKeywordId]
            else:
                print("update adGroup TargetingClause failed")
                return ["failed", ""]
        except Exception as e:
            print("update adGroup TargetingClause failed: ", e)
            return ["failed", ""]

    def list_adGroup_Targetingrecommendations(self, market, adGroupID,products):
        adGroup_info={
  "tactic": "T00020",
  "products": products,
  "typeFilter": [
    "CATEGORY"
  ]
}
        try:
            credentials, access_token = self.load_credentials(market)

            result = sponsored_display.TargetsRecommendations(credentials=credentials,
                                                              marketplace=Marketplaces[market.upper()],
                                                              access_token=access_token,
                                                              proxies=get_proxies(market),
                                                              debug=True).list_targets_recommendations(
                body=json.dumps(adGroup_info))
        except Exception as e:
            print("list adGroup TargetingClause failed: ", e)
            result = None
        if result and result.payload:
            print("list adGroup Targeting success")
            return result.payload
        # if result and result.payload["negativeKeywords"]["success"]:
        #     negativeKeywordId = result.payload["negativeKeywords"]["success"][0]["negativeKeywordId"]
        #     print("add adGroup negative keyword success,negativeKeywordId is:", negativeKeywordId)
        #     return ["success", negativeKeywordId]
        else:
            print("list adGroup TargetingClause failed")
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
# agt=AdGroupTools_SD('LAPASA')
# res = agt.list_adGroup_Targeting('DE',516174005648745)
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
# agt=AdGroupTools_SD()
# res = agt.list_adGroup_TargetingClause(397527887041271,'FR')
# # print(type(res))
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
# agt=AdGroupTools_SD('LAPASA')
# # 测试更新广告系列信息
# res = agt.list_adGroup_Targeting_by_campaignId('FR','366836223007357')
# print(type(res))
# print(res)
