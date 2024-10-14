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
from ai.backend.util.db.util.db_tool.proxies import ProxyManager


class CampaignTools(BaseApi):
    def __init__(self, db, brand, market):
        super().__init__(db, brand, market)
        self.proxy_manager = ProxyManager()

    def make_request(self, method_name, *args, **kwargs):
        attempts = 0
        while attempts < self.attempts_time:
            try:
                campaigns = sponsored_display.Campaigns(
                    credentials=self.credentials,
                    marketplace=Marketplaces[self.market.upper()],
                    access_token=self.access_token,
                    proxies=self.proxy_manager.get_proxies(self.market),
                    debug=True
                )

                # 动态调用方法
                result = getattr(campaigns, method_name)(*args, **kwargs)

                if result and result.payload:
                    print(f"{method_name} success")
                    return result.payload
                else:
                    print(f"{method_name} failed:")
                    self.wait_time()
                    res = result.payload
                    attempts += 1
            except Exception as e:
                print(f"{method_name} failed: ", e)
                self.wait_time()
                res = e
                attempts += 1

        return res

    def list_campaigns_api(self, campaignId):
        return self.make_request("get_campaign", str(campaignId))

    def list_all_campaigns_api(self):
        return self.make_request("list_campaigns")

    def create_campaigns_api(self, campaign_info):
        return self.make_request("create_campaigns", json.dumps(campaign_info))
    # def list_campaigns_api(self,campaignId):
    #     attempts = 0
    #     while attempts < 3:
    #         try:
    #             result = sponsored_display.Campaigns(credentials=self.credentials,
    #                                                  marketplace=Marketplaces[self.market.upper()],
    #                                                  access_token=self.access_token,
    #                                                  proxies=self.proxy_manager.get_proxies(self.market),
    #                                                  debug=True).get_campaign(
    #               str(campaignId))
    #             if result and result.payload:
    #
    #                 print("list campaign success")
    #                 res = result.payload
    #                 return res
    #             else:
    #                 print("list campaign failed:")
    #                 self.wait_time()
    #                 res = result.payload
    #                 attempts += 1
    #         except Exception as e:
    #             print("list campaign failed: ", e)
    #             self.wait_time()
    #             res = e
    #             attempts += 1
    #
    #         # 返回创建的 compaignID
    #     return res

    # 新建广告活动/系列
    # def create_campaigns_api(self,campaign_info):
    #     attempts = 0
    #     while attempts < 3:
    #         try:
    #             result = sponsored_display.Campaigns(credentials=self.credentials,
    #                                                  marketplace=Marketplaces[self.market.upper()],
    #                                                  access_token=self.access_token,
    #                                                  proxies=self.proxy_manager.get_proxies(self.market),
    #                                                  debug=True).create_campaigns(
    #                 body=json.dumps(campaign_info))
    #             compaignID = ""
    #             if result and result.payload[0]["campaignId"]:
    #                 campaignID = result.payload[0]["campaignId"]
    #                 print("create campaign success, compaignID is ", compaignID)
    #                 res = ["success",campaignID]
    #                 return res
    #             else:
    #                 print(" create campaign failed:")
    #                 self.wait_time()
    #                 attempts += 1
    #         except Exception as e:
    #             print("create campaign failed: ", e)
    #             self.wait_time()
    #             attempts += 1
    #     return ["failed", None]

    # 更新广告系列（调整Budget、状态、enddate）
    def update_campaigns(self,campaign_info):

        try:
            result = sponsored_display.Campaigns(credentials=self.credentials,
                                                 marketplace=Marketplaces[self.market.upper()],
                                                 access_token=self.access_token,
                                                 proxies=self.proxy_manager.get_proxies(self.market),
                                                 debug=True).edit_campaigns(
                body=json.dumps(campaign_info))
            print(result)
        except Exception as e:
            print("update campaigns failed: ", e)
            result = None
        compaignID = ""
        if result.payload[0]["code"] == 'SUCCESS':
            campaignID = result.payload[0]["campaignId"]
            print("update campaigns success, compaignID is ", compaignID)
            res = ["success", campaignID]
        else:
            print(" update campaigns failed:")
            res = ["failed", ""]
        # 返回创建的 compaignID
        return res

    # 给广告系列增加否定关键词
    def add_campaigns_negative_keywords(self,campaign_negativekv_info):

        try:
            result = sponsored_brands.CampaignNegativeKeywordsV3(credentials=my_credentials,
                                                    marketplace=Marketplaces.NA,
                                                    debug=True).create_campaign_negative_keywords(
                body=json.dumps(campaign_negativekv_info))
            print(result)
        except Exception as e:
            print("add campaign negative keyword failed: ", e)
            result = None
        if result and result.payload["campaignNegativeKeywords"]["success"]:
            campaignNegativeKeywordId = result.payload["campaignNegativeKeywords"]["success"][0]["campaignNegativeKeywordId"]
            print("add campaign negative keyword success, campaignNegativeKeywordId is ", campaignNegativeKeywordId)
            res = ["success", campaignNegativeKeywordId]
        else:
            print(" add campaign negative keyword failed")
            res = ["failed", ""]
        # 返回创建的 campaignNegativeKeywordId
        return res


    # 给广告系列更新否定关键词
    def update_campaigns_negative_keywords(self,campaign_negativekv_info):

        try:
            result = sponsored_brands.CampaignNegativeKeywordsV3(credentials=my_credentials,
                                                    marketplace=Marketplaces.NA,
                                                    debug=True).edit_campaign_negative_keywords(
                body=json.dumps(campaign_negativekv_info))
            print(result)
        except Exception as e:
            print("更新广告系列否定关键词失败: ", e)
            result = None
        if result and result.payload["campaignNegativeKeywords"]["success"]:
            campaignNegativeKeywordId = result.payload["campaignNegativeKeywords"]["success"][0]["campaignNegativeKeywordId"]
            print("更新广告系列否定关键词成功:campaignNegativeKeywordId is ", campaignNegativeKeywordId)
            res = ["success", campaignNegativeKeywordId]
        else:
            print(" xxxx 更新广告系列否定关键词失败:")
            res = ["failed", ""]
        # 返回创建的 campaignNegativeKeywordId
        return res



# 测试 新增广告系列的否定关键词
# campaign_negativekv_info={
#   "campaignNegativeKeywords": [
#     {
#       "campaignId": "531571979684792",
#       "matchType": "NEGATIVE_PHRASE",
#       "state": "ENABLED",
#       "keywordText": "addtest0508"
#     }
#   ]
# }
# ct=CampaignTools()
# res = ct.add_campaigns_negative_keywords(campaign_negativekv_info)
# print(res)

    def get_campaigns_negative_keywords(self,campaign_info):
        try:
            result = sponsored_brands.CampaignNegativeKeywordsV3(credentials=my_credentials,
                                                    marketplace=Marketplaces.NA,
                                                    debug=True).list_campaign_negative_keywords(
                body=json.dumps(campaign_info))
            print(result)
        except Exception as e:
            print("get广告系列否定关键词失败: ", e)
            result = None
        if result and result.payload["campaignNegativeKeywords"]:
            campaignNegativeKeywordId = result.payload["campaignNegativeKeywords"]
            print("get广告系列否定关键词成功:campaignNegativeKeywordId is ", campaignNegativeKeywordId)
            res = ["success", campaignNegativeKeywordId]
        else:
            print(" xxxx get广告系列否定关键词失败:")
            res = ["failed", ""]
        # 返回创建的 campaignNegativeKeywordId
        return res

    def delete_campaigns_api(self,campaign_id):
        try:
            campaign_info={
  "campaignIdFilter": {
    "include": [
        str(campaign_id)
    ]
  }
}
            result = sponsored_display.CampaignsV4(credentials=self.credentials,
                                                   marketplace=Marketplaces[self.market.upper()],
                                                   access_token=self.access_token,
                                                   proxies=self.proxy_manager.get_proxies(self.market),
                                                   debug=True).delete_campaigns(
                body=json.dumps(campaign_info))
        except Exception as e:
            print("delete campaign failed: ", e)
            result = None
        compaignID = ""
        if result and result.payload["campaigns"]:

            print("delete campaign success")
            res = result.payload["campaigns"]

        else:
            print("delete campaign failed:")
            res = ["failed",""]
        # 返回创建的 compaignID
        return print(res)

    # def list_all_campaigns_api(self):
    #     attempts = 0
    #     while attempts < 3:
    #         try:
    #             result = sponsored_display.Campaigns(credentials=self.credentials,
    #                                                  marketplace=Marketplaces[self.market.upper()],
    #                                                  access_token=self.access_token,
    #                                                  proxies=self.proxy_manager.get_proxies(self.market),
    #                                                  debug=True).list_campaigns(
    #               )
    #             if result and result.payload:
    #
    #                 print("list campaign success")
    #                 res = result.payload
    #                 return res
    #
    #             else:
    #                 print("list campaign failed:")
    #                 self.wait_time()
    #                 attempts += 1
    #         except Exception as e:
    #             print("list campaign failed: ", e)
    #             self.wait_time()
    #             attempts += 1
    #     # 返回创建的 compaignID
    #     return None

if __name__ == "__main__":

    ct = CampaignTools('amazon_ads','LAPASA','JP')
    #测试更新广告系列信息
    res = ct.list_all_campaigns_api()
    # res = ct.list_campaigns_api(498971857900272,'FR')
    # #print(type(res))
    #print(res)

# 测试新增广告系列

# #更新广告关键词测试
# campaign_negativekv_info = {
#   "campaignNegativeKeywords": [
#     {
#       "keywordId": "496300953645062",
#       "state": "PAUSED"
#     }
#   ]
# }
# ct=CampaignTools('VC1')
# res = ct.list_all_campaigns_api('US')
# print(res)
