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

class SPKeywordTools:

    def __init__(self):
        self.my_credentials = dict(
        refresh_token='****',
        client_id='****',
        client_secret='****',
        profile_id='****',
    )
    # 新建关键词
    def create_spkeyword_api(self, keyword_info):
        try:
            result = sponsored_products.KeywordsV3(credentials=my_credentials,
                                                         marketplace=Marketplaces.NA,
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
            print("create sp keyword failed:", e)
            return ["failed", keywordId]

    # 修改广告组关键词
    def update_spkeyword_api(self, keyword_info):
        try:
            result = sponsored_products.KeywordsV3(credentials=my_credentials,
                                                         marketplace=Marketplaces.NA,
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
            print("update sp keyword failed:", e)
            return ["failed", keywordId]
