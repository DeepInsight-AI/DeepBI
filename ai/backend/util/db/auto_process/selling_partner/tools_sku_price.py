from sp_api.api import ListingsItems
from sp_api.base import Marketplaces
from ai.backend.util.db.auto_process.selling_partner.util.client_config import sp_config
from ai.backend.util.db.auto_process.selling_partner.util.access_token import get_access_token
import json

class SkuTools:
    def __init__(self):
        self.sellerId = 'A9S56BTL94S55'

    def get_region_from_market(self, market):
        # 定义市场到地区的映射关系
        market_to_region = {
            'ES': 'EU',
            'DE': 'EU',
            'FR': 'EU',
            'IT': 'EU',
            'NL': 'EU',
            'BE': 'EU',
            'SE': 'EU',
            'UK': 'EU',
            'US': 'NA',
            'JP': 'FE'
            # 添加更多的市场到地区映射
        }
        # 返回对应市场的地区，如果没有匹配到默认返回None
        return market_to_region.get(market)

    def get_credentials_and_access_token(self, region):
        # 获取 credentials
        my_credentials = {
            'refresh_token': sp_config['client'][region]['refresh-token'],
            'lwa_app_id': sp_config['client'][region]['client-id'],
            'lwa_client_secret': sp_config['client'][region]['client-secret'],
        }

        # 获取 access token
        access_token = get_access_token(region, "SP")

        return my_credentials, access_token

    def list_item_api(self, market, sku):
        region = self.get_region_from_market(market)
        if region is None:
            print(f"Market '{market}' is not mapped to any region.")
            return None
        my_credentials, access_token = self.get_credentials_and_access_token(region)
        request_body = {
            "marketplaceIds": [Marketplaces[market].marketplace_id],
            "includedData": ['summaries', 'offers', 'attributes']
        }
        try:
            result = ListingsItems(
                                credentials=my_credentials,
                                marketplace=Marketplaces[market],
                                refresh_token=my_credentials['refresh_token'],
                                restricted_data_token=access_token
                            ).get_listings_item(
                                sellerId=self.sellerId,
                                sku=sku,
                                **request_body,
                            )
        except Exception as e:
            print("list sku failed: ", e)
            result = None

        return result.payload

    def update_item_api(self, market, sku, sku_info):
        region = self.get_region_from_market(market)
        if region is None:
            print(f"Market '{market}' is not mapped to any region.")
            return None
        my_credentials, access_token = self.get_credentials_and_access_token(region)
        request_body = {
            "marketplaceIds": [Marketplaces[market].marketplace_id],
        }
        try:
            result = ListingsItems(
                                credentials=my_credentials,
                                marketplace=Marketplaces[market],
                                refresh_token=my_credentials['refresh_token'],
                                restricted_data_token=access_token
                            ).patch_listings_item(
                                sellerId=self.sellerId,
                                sku=sku,
                                **request_body,
                                body=json.dumps(sku_info)
                            )
        except Exception as e:
            print("list sku failed: ", e)
            result = None
        if result and result.payload["status"] == 'ACCEPTED':
            sku = result.payload["sku"]
            print("update sku price success,sku is :", sku)
            res = ["success", sku]
        else:
            print("update sku price failed:")
            res = ["failed", ]
        return res

# api1 = SkuTools()
# res = api1.list_item_api('FR', 'LPK17SS0001MT020XSR4')
# print(type(res))
# print(res)

