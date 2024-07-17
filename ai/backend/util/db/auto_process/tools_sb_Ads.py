import pymysql
from ad_api.api import sponsored_brands
from ad_api.base import Marketplaces
import json
import datetime
from decimal import Decimal


class AdsTools:
    def __init__(self):
        self.credentials = self.load_credentials()

    def load_credentials(self):
        credentials_path = './credentials.json'
        with open(credentials_path) as f:
            config = json.load(f)
        return config['credentials']

    def select_market(self, market):
        credentials = self.credentials.get(market)
        if not credentials:
            raise ValueError(f"Market '{market}' not found in credentials")
        # 返回相应的凭据和市场信息
        return credentials, Marketplaces[market.upper()]


    # 新建广告组
    def create_video_ads_api(self,adGroup_info,market):
        try:
            credentials, marketplace = self.select_market(market)
            result = sponsored_brands.AdsV4(credentials=credentials,
                                                            marketplace=marketplace,
                                                            debug=True).create_video_ads(
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
    def update_ads_api(self,adGroup_info):

        try:
            result = sponsored_brands.AdsV4(credentials=my_credentials,
                                                            marketplace=Marketplaces.NA,
                                                            debug=True).update_ads(
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


    def get_ads_api(self,market,adGroupID):
        credentials, marketplace = self.select_market(market)
        adGroup_info = {
  "maxResults": 10,
  "adGroupIdFilter": {
    "include": [
      str(adGroupID)
    ]
  },
  "includeExtendedDataFields": False,
}
        try:
            result = sponsored_brands.AdsV4(credentials=credentials,
                                                            marketplace=marketplace,
                                                            debug=True).list_ads(
                body=json.dumps(adGroup_info))
        except Exception as e:
            print("查找广告组失败: ", e)
            result = None
        adGroupID = ""
        if result and result.payload["ads"][0]:
            defaultBid_old = result.payload["ads"][0]
            print("ads 查找广告成功", adGroupID)
        else:
            print("查找广告组失败")
            defaultBid_old=1
        return defaultBid_old


# 广告组更新否定关键词测试
# adGroup_negativekw_info = {
# "negativeKeywords": [
# {
# "keywordId": "426879824500654",
# "state": "ENABLED"
# }
# ]
# }
# agt=AdGroupTools_SD()
# res = agt.get_adGroup_negativekw('FR',306913253726907)
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
agt=AdsTools()
# 测试更新广告系列信息
res = agt.get_ads_api('FR','370686358313691')
# print(type(res))
# print(res)
