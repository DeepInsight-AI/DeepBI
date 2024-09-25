import os
import pymysql
from ad_api.api import Reports
from ad_api.base import Marketplaces
import json
import datetime
from decimal import Decimal
from ai.backend.util.db.configuration.path import get_config_path
from ai.backend.util.db.util.common import get_ad_my_credentials,get_proxies

class CreativeTools:
    def __init__(self,brand):
        self.brand = brand

    def load_credentials(self,market):
        my_credentials,access_token = get_ad_my_credentials(market,self.brand)
        return my_credentials,access_token


    # 新建广告组
    def create_report_api(self,market):
        adGroup_info = {
  "endDate": "2024-08-20",
  "configuration": {
    "adProduct": "SPONSORED_PRODUCTS",
    "columns": [
      "impressions",
      "clicks",
      "cost",
      "purchases1d",
      "purchases7d",
      "purchases14d",
      "purchases30d",
      "purchasesSameSku1d",
      "purchasesSameSku7d",
      "purchasesSameSku14d",
      "purchasesSameSku30d",
      "unitsSoldClicks1d",
      "unitsSoldClicks7d",
      "unitsSoldClicks14d",
      "unitsSoldClicks30d",
      "sales1d",
      "sales7d",
      "sales14d",
      "sales30d",
      "attributedSalesSameSku1d",
      "attributedSalesSameSku7d",
      "attributedSalesSameSku14d",
      "attributedSalesSameSku30d",
      "unitsSoldSameSku1d",
      "unitsSoldSameSku7d",
      "unitsSoldSameSku14d",
      "unitsSoldSameSku30d",
      "kindleEditionNormalizedPagesRead14d",
      "kindleEditionNormalizedPagesRoyalties14d",
      "startDate",
      "endDate",
      "campaignBiddingStrategy",
      "costPerClick",
      "clickThroughRate",
      "spend",
      "campaignName",
      "campaignId",
      "campaignStatus",
      "campaignBudgetType",
      "campaignBudgetAmount",
      "campaignRuleBasedBudgetAmount",
      "campaignApplicableBudgetRuleId",
      "campaignApplicableBudgetRuleName",
      "campaignBudgetCurrencyCode"
    ],
    "reportTypeId": "spCampaigns",
    "format": "GZIP_JSON",
    "groupBy": [
      "campaign"
    ],
    "filters": [
      {
        "field": "campaignStatus",
        "values": [
          "ENABLED",
          "PAUSED",
          "ARCHIVED"
        ]
      }
    ],
    "timeUnit": "SUMMARY"
  },
  "name": "SponsoredProductsCampaignsSummaryReport",
  "startDate": "2023-08-20"
}
        try:
            credentials, access_token = self.load_credentials(market)
            result = Reports(credentials=credentials,
                                                   marketplace=Marketplaces[market.upper()],
                                                   access_token=access_token,
                                                   proxies=get_proxies(market),
                                                   debug=True).post_report(
                body=json.dumps(adGroup_info))
        except Exception as e:
            print("create report failed: ", e)
            result = None
        reportId = ""
        if result and result.payload:
            reportId = result.payload["reportId"]
            print("create_report_api success,reportId is:", reportId)
            return ["success",reportId]
        else:
            print("create_report_api failed:")
            return ["failed", reportId]

    def register_an_uploaded_asset_api(self,url,name,market):
        adGroup_info = {
  "url": url,
  "name": name,
  "assetType": "IMAGE",
  "assetSubTypeList": [
    "LOGO"
  ],
"registrationContext": {
    "associatedPrograms":[
        {"metadata":
            {
                "dspAdvertiserId": None
            },
            "programName": "AMAZON_DSP"
        }
    ]
},
"associatedSubEntityList":
[

    {
        "brandEntityId": None
    }

],
"skipAssetSubTypesDetection": True
}
        try:
            credentials, access_token = self.load_credentials(market)
            result = CreativeAssets(credentials=credentials,
                                                   marketplace=Marketplaces[market.upper()],
                                                   access_token=access_token,
                                                   proxies=get_proxies(market),
                                                   debug=True).register_asset(
                body=json.dumps(adGroup_info))
        except Exception as e:
            print("create adGroup failed: ", e)
            result = None
        url = ""
        if result and result.payload:
            url = result.payload
            print("register_an_uploaded_asset_api success,url is:", url)
            return ["success",url]
        else:
            print("register_an_uploaded_asset_api failed:")
            return ["failed", url]


res = CreativeTools('LAPASA').create_report_api('US')
print(res)
#CreativeTools('LAPASA').register_an_uploaded_asset_api('https://al-na-9d5791cf-3faf.s3.amazonaws.com/1a8af5f5-ac5c-4837-80a3-757626bba97a.png?x-amz-meta-filename=C%3A%2FUsers%2Fadmin%2FDesktop%2F%E6%B5%8B%E8%AF%95%2FVeement.png&X-Amz-Security-Token=FwoGZXIvYXdzEF0aDK8bWXbYuNV1i26%2FoyLIAW%2FcjImPKrZy6sYStFFCadm0Dx6CGuuxxHSR2CM9UbkXjsb8l55X7OsmTJWUtqOjm5l1CWUM9EUFbovkOwVcAwGDC3HHdkddUfu1hxxTdTPMcNiBWmtb4Q3cKalqDDXRneU6uQ2Z64TEXNk96bKsQPHP83Q977CFKtDQNfmqWbZT6%2FEz4R0bhev8U415BaK1kTw859Q3Uapdh6IHRyse6po%2BRQzCUPj9XwBI9GEkQnN2S%2Fodslt%2BqGXjRYJMboBisk8A1s5Odo1yKNv817UGMi24aEaANYqrCI9V15TaPHOjDxsXM7pnk9pLeiZejLgVBWAGNnAHNMOFUBcfFIg%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20240809T113656Z&X-Amz-SignedHeaders=host&X-Amz-Expires=3600&X-Amz-Credential=ASIA3NCY4QOMWGF26NX4%2F20240809%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=320eb4a5709ae7dbcf43bff697150280a14bcef8e0d0c8cb17ec6fbd2e7d4553','Veement','US')
