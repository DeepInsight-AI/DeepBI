import os

import pymysql
from ad_api.api import sponsored_products
from ad_api.base import Marketplaces
import json
import datetime
from decimal import Decimal
from ai.backend.util.db.configuration.path import get_config_path
from ai.backend.util.db.util.common import get_ad_my_credentials,get_proxies


class BudgetRulesTools:
    def __init__(self,brand):
        self.brand = brand

    def load_credentials(self,market):
        my_credentials,access_token = get_ad_my_credentials(market,self.brand)
        return my_credentials,access_token

    def list_budget_rules_recommendations_api(self,market,campaignId):
        try:
            campaign_info = {
                "campaignId": str(campaignId),
                "recommendationType": "EVENTS_FOR_EXISTING_CAMPAIGN"
            }
            credentials, access_token = self.load_credentials(market)
            result = sponsored_products.BudgetRulesRecommendations(credentials=credentials,
                                                    marketplace=Marketplaces[market.upper()],
                                                    access_token=access_token,
                                                    proxies=get_proxies(market),
                                                    debug=True).list_campaigns_budget_rules_recommendations(
               body=json.dumps(campaign_info))
        except Exception as e:
            print("list budget_rules_recommendations failed: ", e)
            result = None
        compaignID = ""
        if result and result.payload:

            print("list budget_rules_recommendations success")
            print(result)
            res = result.payload

        else:
            print("list budget_rules_recommendations failed:")
            print(result)
            res = None
        # 返回创建的 compaignID
        return res
    # 新建广告活动/系列
    def create_budget_rules_api(self,campaign_info,market):
        try:

            credentials, access_token = self.load_credentials(market)
            result = sponsored_products.BudgetRules(credentials=credentials,
                                                    marketplace=Marketplaces[market.upper()],
                                                    access_token=access_token,
                                                    proxies=get_proxies(market),
                                                    debug=True).create_budget_rules(
                body=json.dumps(campaign_info))
        except Exception as e:
            print("create campaign failed: ", e)
            result = None
        compaignID = ""
        if result and result.payload["responses"][0]["ruleId"]:
            ruleId = result.payload["responses"][0]["ruleId"]
            print("create_budget_rules success, ruleId is ", ruleId)
            res = ["success",ruleId]
        else:
            print("create_budget_rules failed:")
            res = ["failed",""]
        # 返回创建的 compaignID
        return res

    def Associates_budget_rules_to_campaign(self,budgetRuleIds,campaign_id,market):
        try:
            campaign_info = {
  "budgetRuleIds": [
    str(budgetRuleIds)
  ]
}
            credentials, access_token = self.load_credentials(market)
            result = sponsored_products.BudgetRules(credentials=credentials,
                                                    marketplace=Marketplaces[market.upper()],
                                                    access_token=access_token,
                                                    proxies=get_proxies(market),
                                                    debug=True).create_campaign_budget_rules(
                campaignId=int(campaign_id),
                body=json.dumps(campaign_info))
        except Exception as e:
            print("create campaign failed: ", e)
            result = None
        compaignID = ""
        if result and result.payload["responses"][0]["ruleId"]:
            ruleId = result.payload["responses"][0]["ruleId"]
            print("Associates_budget_rules_to_campaign success, ruleId is ", ruleId)
            res = ["success",ruleId]
        else:
            print("Associates_budget_rules_to_campaign failed:")
            res = ["failed",""]
        # 返回创建的 compaignID
        return res

if __name__ == "__main__":
    BudgetRulesTools('LAPASA').Associates_budget_rules_to_campaign('da16a837-cd35-4baa-988c-84788f5bb726',540943190583757,'IT')



# info = {
#   "campaignIdFilter": {
#     "include": [
#       "165106868294863"
#     ]
#   }
# }
#
# ct = CampaignTools('LAPASA')
# #测试更新广告系列信息
# res = ct.list_campaigns_api(info,'ES')
# #print(type(res))
# print(res)


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
# ct=BudgetRulesTools('LAPASA')
# res = ct.list_budget_rules_recommendations_api('US',440189201268459)
# print(res)
