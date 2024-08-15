import os
import pymysql
from ad_api.api import CreativeAssets
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
    def create_an_upload_location_api(self,fileName,market):
        adGroup_info = {
  "fileName": fileName
}
        try:
            credentials, access_token = self.load_credentials(market)
            result = CreativeAssets(credentials=credentials,
                                                   marketplace=Marketplaces[market.upper()],
                                                   access_token=access_token,
                                                   proxies=get_proxies(market),
                                                   debug=True).upload_asset(
                body=json.dumps(adGroup_info))
        except Exception as e:
            print("create adGroup failed: ", e)
            result = None
        url = ""
        if result and result.payload["url"]:
            url = result.payload["url"]
            print("create_an_upload_location_api success,url is:", url)
            return ["success",url]
        else:
            print("create_an_upload_location_api failed:")
            return ["failed", url]

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


#CreativeTools('LAPASA').create_an_upload_location_api('C:/Users/admin/Desktop/测试/Veement.png','US')

#CreativeTools('LAPASA').register_an_uploaded_asset_api('https://al-na-9d5791cf-3faf.s3.amazonaws.com/1a8af5f5-ac5c-4837-80a3-757626bba97a.png?x-amz-meta-filename=C%3A%2FUsers%2Fadmin%2FDesktop%2F%E6%B5%8B%E8%AF%95%2FVeement.png&X-Amz-Security-Token=FwoGZXIvYXdzEF0aDK8bWXbYuNV1i26%2FoyLIAW%2FcjImPKrZy6sYStFFCadm0Dx6CGuuxxHSR2CM9UbkXjsb8l55X7OsmTJWUtqOjm5l1CWUM9EUFbovkOwVcAwGDC3HHdkddUfu1hxxTdTPMcNiBWmtb4Q3cKalqDDXRneU6uQ2Z64TEXNk96bKsQPHP83Q977CFKtDQNfmqWbZT6%2FEz4R0bhev8U415BaK1kTw859Q3Uapdh6IHRyse6po%2BRQzCUPj9XwBI9GEkQnN2S%2Fodslt%2BqGXjRYJMboBisk8A1s5Odo1yKNv817UGMi24aEaANYqrCI9V15TaPHOjDxsXM7pnk9pLeiZejLgVBWAGNnAHNMOFUBcfFIg%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20240809T113656Z&X-Amz-SignedHeaders=host&X-Amz-Expires=3600&X-Amz-Credential=ASIA3NCY4QOMWGF26NX4%2F20240809%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=320eb4a5709ae7dbcf43bff697150280a14bcef8e0d0c8cb17ec6fbd2e7d4553','Veement','US')
