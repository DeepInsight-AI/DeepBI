import os

import pymysql
from ad_api.api import sponsored_products
from ad_api.base import Marketplaces
import json
import datetime
from decimal import Decimal
from ai.backend.util.db.configuration.path import get_config_path
from ai.backend.util.db.util.common import get_ad_my_credentials,get_proxies


class SPKeywordTools:

    def __init__(self,brand):
        self.brand = brand

    def load_credentials(self,market):
        my_credentials,access_token = get_ad_my_credentials(market,self.brand)
        return my_credentials,access_token

    # 新建关键词
    def create_spkeyword_api(self, keyword_info,market):
        try:
            credentials, access_token = self.load_credentials(market)
            result = sponsored_products.KeywordsV3(credentials=credentials,
                                                   marketplace=Marketplaces[market.upper()],
                                                   access_token=access_token,
                                                   proxies=get_proxies(market),
                                                   debug=True).create_keyword(
                body=json.dumps(keyword_info))
        except Exception as e:
            print("create sp keyword failed: ", e)
            result = None
        keywordId = ""
        if result and result.payload["keywords"]["success"]:
            spkeywordid = result.payload["keywords"]["success"][0]["keywordId"]
            print("create sp keyword success,sp keywordid is:", spkeywordid)
            return ["success", spkeywordid]
        else:
            print("create sp keyword failed")
            return ["failed", keywordId]

    # 修改广告组关键词
    def update_spkeyword_api(self, keyword_info,market):
        try:
            credentials, access_token = self.load_credentials(market)
            result = sponsored_products.KeywordsV3(credentials=credentials,
                                                   marketplace=Marketplaces[market.upper()],
                                                   access_token=access_token,
                                                   proxies=get_proxies(market),
                                                   debug=True).edit_keyword(
                body=json.dumps(keyword_info))
        except Exception as e:
            print("update sp keyword failed: ", e)
            result = None
        keywordId = ""
        if result and result.payload["keywords"]["success"]:
            spkeywordid = result.payload["keywords"]["success"][0]["keywordId"]
            print("update sp keyword success,sp keywordid is:", spkeywordid)
            return ["success", spkeywordid]
        else:
            print("update sp keyword failed:")
            return ["failed", keywordId]

    def delete_spkeyword_api(self, keyword_info,market):
        try:
            credentials, access_token = self.load_credentials(market)
            result = sponsored_products.KeywordsV3(credentials=credentials,
                                                   marketplace=Marketplaces[market.upper()],
                                                   access_token=access_token,
                                                   proxies=get_proxies(market),
                                                   debug=True).delete_keywords(
                body=json.dumps(keyword_info))
        except Exception as e:
            print("delete sp keyword failed: ", e)
            result = None
        keywordId = ""
        if result and result.payload["keywords"]["success"]:
            spkeywordid = result.payload["keywords"]["success"][0]["keywordId"]
            print("delete sp keyword success,sp keywordid is:", spkeywordid)
            return ["success", spkeywordid]
        else:
            print("delete sp keyword failed")
            return ["failed", keywordId]

    def get_spkeyword_api(self, market, adGroupID):
        credentials, access_token = self.load_credentials(market)
        adGroup_info = {
            "maxResults": 2000,
            "adGroupIdFilter": {
                "include": [
                    str(adGroupID)
                ]
            },
            "includeExtendedDataFields": False,
        }
        try:
            result = sponsored_products.KeywordsV3(credentials=credentials,
                                                   marketplace=Marketplaces[market.upper()],
                                                   access_token=access_token,
                                                   proxies=get_proxies(market),
                                                   debug=True).list_keywords(
                body=json.dumps(adGroup_info))
        except Exception as e:
            print("查找关键词失败: ", e)
            result = None

        if result and result.payload["keywords"]:
            defaultBid_old = result.payload["keywords"]
            print(" 查找关键词成功")
        else:
            print("查找关键词失败:")
            defaultBid_old = None
        return defaultBid_old

    def get_spkeyword_api_by_campaignid(self, market, campaignId):
        credentials, access_token = self.load_credentials(market)
        adGroup_info = {
            "maxResults": 2000,
            "campaignIdFilter": {
                "include": [
                    str(campaignId)
                ]
            },
            "includeExtendedDataFields": False,
        }
        try:
            result = sponsored_products.KeywordsV3(credentials=credentials,
                                                   marketplace=Marketplaces[market.upper()],
                                                   access_token=access_token,
                                                   proxies=get_proxies(market),
                                                   debug=True).list_keywords(
                body=json.dumps(adGroup_info))
        except Exception as e:
            print("查找关键词失败: ", e)
            result = None

        if result and result.payload["keywords"]:
            defaultBid_old = result.payload["keywords"]
            print(" 查找关键词成功")
        else:
            print("查找关键词失败:")
            defaultBid_old = None
        return defaultBid_old

    def get_spkeyword_api_by_keywordId(self, market, keywordId):
        credentials, access_token = self.load_credentials(market)
        adGroup_info = {
            "maxResults": 2000,
            "keywordIdFilter": {
                "include": [
                    str(keywordId)
                ]
            },
            "includeExtendedDataFields": False,
        }
        try:
            result = sponsored_products.KeywordsV3(credentials=credentials,
                                                   marketplace=Marketplaces[market.upper()],
                                                   access_token=access_token,
                                                   proxies=get_proxies(market),
                                                   debug=True).list_keywords(
                body=json.dumps(adGroup_info))
        except Exception as e:
            print("查找关键词失败: ", e)
            result = None

        if result and result.payload["keywords"]:
            defaultBid_old = result.payload["keywords"]
            print(" 查找关键词成功")
        else:
            print("查找关键词失败:")
            defaultBid_old = None
        return defaultBid_old

    def get_spkeyword_recommendations_api(self, market, campaignId, adGroupID):
        credentials, access_token = self.load_credentials(market)
        adGroup_info = {
  "campaignId": str(campaignId),
  "recommendationType": "KEYWORDS_FOR_ADGROUP",
  "adGroupId": adGroupID
}
        try:
            result = sponsored_products.RankedKeywordsRecommendations(credentials=credentials,
                                                                      marketplace=Marketplaces[market.upper()],
                                                                      access_token=access_token,
                                                                      proxies=get_proxies(market),
                                                                      debug=True).list_ranked_keywords_recommendations(
                body=json.dumps(adGroup_info))
        except Exception as e:
            print("查找推荐关键词失败: ", e)
            result = None

        if result and result.payload:
            defaultBid_old = result.payload
            print(" 查找推荐关键词成功")
        else:
            print("查找推荐关键词失败:")
            defaultBid_old = None
        return defaultBid_old


# sp = SPKeywordTools('LAPASA')
# res = sp.get_spkeyword_api_by_keywordId('IT',177235977989981)
# #res = sp.get_spkeyword_recommendations_api('SE',548980770113799,421510724316832)
# print(res)
