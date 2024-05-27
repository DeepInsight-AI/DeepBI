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

    # 新建关键词
    def create_spkeyword_api(self, keyword_info,market):
        try:
            credentials, marketplace = self.select_market(market)
            result = sponsored_products.KeywordsV3(credentials=credentials,
                                                         marketplace=marketplace,
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

    def get_spkeyword_api(self, market, adGroupID):
        credentials, marketplace = self.select_market(market)
        adGroup_info = {
            "maxResults": 500,
            "adGroupIdFilter": {
                "include": [
                    str(adGroupID)
                ]
            },
            "includeExtendedDataFields": False,
        }
        try:
            result = sponsored_products.KeywordsV3(credentials=credentials,
                                                   marketplace=marketplace,
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
            defaultBid_old = 1
        return defaultBid_old


# sp = SPKeywordTools()
# sp.get_spkeyword_api('FR',510948590522856)
