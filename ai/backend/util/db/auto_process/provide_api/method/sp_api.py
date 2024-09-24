from datetime import datetime
import os
import pandas as pd
import json
from ai.backend.util.db.auto_process.gen_sp_campaign import Gen_campaign
from ai.backend.util.db.auto_process.gen_sp_keyword import Gen_keyword
from ai.backend.util.db.auto_process.gen_sp_adgroup import Gen_adgroup
from ai.backend.util.db.auto_process.gen_sp_product import Gen_product
from ai.backend.util.db.auto_process.tools_sp_adGroup import AdGroupTools
from ai.backend.util.db.auto_process.tools_sp_keyword import SPKeywordTools
from ai.backend.util.db.auto_process.create_new_sp_ad_auto import load_config
from ai.backend.util.db.auto_process.advertising.db_tool.check_records_within_24_hours import CheckRecordsWithin24Hours

class auto_api_sp:
    def __init__(self, brand, market, db, user):
        self.brand = brand
        self.market = market
        self.db = db
        self.user = user
        self.exchange_rate = load_config('exchange_rate.json').get('exchange_rate', {}).get("DE", {}).get(self.market)

    def update_sp_ad_budget(self, campaignId, bid):
        try:
            api1 = Gen_campaign(self.db, self.brand, self.market)
            campaign_info = api1.list_camapign(campaignId)
            if campaign_info is not None:
                for item in campaign_info:
                    campaignId = item['campaignId']
                    name = item['name']
                    state = item['state']
                    bid1 = item['budget']['budget']
                    api1.update_camapign_v0(str(campaignId), name, float(bid1), float(bid), state, self.user)
                return 200
            else:
                return 404  # Campaign not found
        except Exception as e:
            print(e)
            return 500  # Internal Server Error

    def update_sp_ad_placement(self, campaignId, bid, placementClassification):
        try:
            api1 = Gen_campaign(self.db, self.brand, self.market)
            campaign_info = api1.list_camapign(campaignId)
            if campaign_info is not None:
                for item in campaign_info:
                    placement_bidding = item['dynamicBidding']['placementBidding']
                    possible_placements = ['PLACEMENT_REST_OF_SEARCH', 'PLACEMENT_PRODUCT_PAGE', 'PLACEMENT_TOP']
                    placement_percentages = {placement: 0 for placement in possible_placements}
                    for item1 in placement_bidding:
                        placement = item1['placement']
                        percentage = item1['percentage']
                        if placement in possible_placements:
                            placement_percentages[placement] = percentage
                    campaignId = item['campaignId']
                    for placement, percentage in placement_percentages.items():
                        if placement == placementClassification:
                            print(f'Placement: {placement}, Percentage: {percentage}')
                            bid1 = percentage
                            if bid1 is not None:
                                api1.update_campaign_placement(str(campaignId), bid1, float(bid), placement, self.user)
                return 200
            else:
                return 404  # Campaign not found
        except Exception as e:
            print(e)
            return 500  # Internal Server Error

    def update_sp_ad_keyword(self, keywordId, bid):
        try:
            api = Gen_keyword(self.db, self.brand, self.market)
            api1 = SPKeywordTools(self.db, self.brand, self.market)
            spkeyword_info = api1.get_spkeyword_api_by_keywordId(keywordId)
            if spkeyword_info is not None:
                for spkeyword in spkeyword_info:
                    bid1 = spkeyword.get('bid')
                    state = spkeyword['state']
                    api.update_keyword_toadGroup(str(keywordId), bid1, float(bid), state, self.user)
                return 200
            else:
                return 404  # Keyword not found
        except Exception as e:
            print(e)
            return 500  # Internal Server Error

    def update_sp_ad_keyword_batch(self, keywordId, bid):
        try:
            api = Gen_keyword(self.db, self.brand, self.market)
            api1 = SPKeywordTools(self.db, self.brand, self.market)
            spkeyword_info = api1.get_spkeyword_api_by_keywordId_batch(keywordId)
            keyword_bid_mapping = {k: v for k, v in zip(keywordId, bid)}
            if spkeyword_info is not None:
                merged_info = []
                for info in spkeyword_info:
                    keyword_id = info['keywordId']
                    if keyword_id in keyword_bid_mapping:
                        merged_info.append({
                            "keywordId": keyword_id,
                            "state": info["state"],
                            "bid": info.get('bid', None),
                            "bid_new": keyword_bid_mapping[keyword_id]  # 从 mapping 中获取 bid_old
                        })
                api.update_keyword_toadGroup_batch(merged_info, self.user)
                return 200
            else:
                return 404  # Keyword not found
        except Exception as e:
            print(e)
            return 500  # Internal Server Error

    def update_sp_ad_automatic_targeting(self, keywordId, bid):
        try:
            api1 = Gen_adgroup(self.db, self.brand, self.market)
            api2 = AdGroupTools(self.db, self.brand, self.market)
            automatic_targeting_info = api2.list_adGroup_TargetingClause_by_targetId(keywordId)
            if automatic_targeting_info is not None:
                for item in automatic_targeting_info:
                    targetId = item['targetId']
                    state = item['state']
                    bid1 = item.get('bid')
                    api1.update_adGroup_TargetingClause(str(targetId), float(bid), state, self.user)
                return 200
            else:
                return 404  # Targeting not found
        except Exception as e:
            print(e)
            return 500  # Internal Server Error

    def update_sp_ad_product_targets(self, keywordId, bid):
        try:
            api1 = Gen_adgroup(self.db, self.brand, self.market)
            api2 = AdGroupTools(self.db, self.brand, self.market)
            automatic_targeting_info = api2.list_adGroup_TargetingClause_by_targetId(keywordId)
            if automatic_targeting_info is not None:
                for automatic_targeting in automatic_targeting_info:
                    targetId = automatic_targeting['targetId']
                    state = automatic_targeting['state']
                    bid1 = automatic_targeting.get('bid')
                    api1.update_adGroup_TargetingClause(str(targetId), float(bid), state, self.user)
                return 200
            else:
                return 404  # Targeting not found
        except Exception as e:
            print(e)
            return 500  # Internal Server Error

    def auto_campaign_status(self, campaignId, status):
        try:
            api1 = Gen_campaign(self.db, self.brand, self.market)
            campaign_info = api1.list_camapign(campaignId)
            if campaign_info is not None:
                for item in campaign_info:
                    campaignId = item['campaignId']
                    name = item['name']
                    state = item['state']
                    res = api1.update_camapign_status(str(campaignId), name, state, status, self.user)
                    if res:
                        return 200
                    else:
                        return 500
            else:
                return 404  # Campaign not found
        except Exception as e:
            print(e)
            return 500  # Internal Server Error

    def auto_sku_status(self, adId, status):
        try:
            api = Gen_product(self.db, self.brand, self.market)
            api.update_product(str(adId), status, self.user)
            return 200
        except Exception as e:
            print(e)
            return 500  # Internal Server Error

    def auto_keyword_status(self, keywordId, status):
        try:
            api = Gen_keyword(self.db, self.brand, self.market)
            api.update_keyword_toadGroup(str(keywordId), None, bid_new=None, state=status, user=self.user)
            return 200
        except Exception as e:
            print(e)
            return 500  # Internal Server Error

    def auto_targeting_status(self, keywordId, status):
        try:
            api1 = Gen_adgroup(self.db, self.brand, self.market)
            api1.update_adGroup_TargetingClause(str(keywordId), bid=None, state=status, user=self.user)
            return 200
        except Exception as e:
            print(e)
            return 500  # Internal Server Error

    def create_product_target(self, keywordId, bid, campaignId, adGroupId):
        try:
            apitool1 = AdGroupTools(self.db, self.brand, self.market)
            api2 = Gen_adgroup(self.db, self.brand, self.market)
            brand_info = apitool1.list_category_refinements(keywordId)
            # 检查是否存在名为"LAPASA"的品牌
            target_brand_name = self.brand
            target_brand_id = None

            for brand in brand_info['brands']:
                if brand['name'] == target_brand_name:
                    target_brand_id = brand['id']
                    targetId = api2.create_adGroup_Targeting2(campaignId, adGroupId,
                                                              float(bid),
                                                              keywordId, target_brand_id, self.user)
                    return 200
            return 404
        except Exception as e:
            print(e)
            return 500  # Internal Server Error

    def create_product_target_asin(self, asin, bid, campaignId, adGroupId):
        try:
            api2 = Gen_adgroup(self.db, self.brand, self.market)
            api2.create_adGroup_Targeting1(campaignId, adGroupId, asin, bid,
                                           state='ENABLED', type='ASIN_SAME_AS', user=self.user)
            return 200
        except Exception as e:
            print(e)
            return 500  # Internal Server Error

    def create_keyword(self, keywordtext, bid, campaignId, adGroupId,matchType):
        try:
            api2 = Gen_keyword(self.db, self.brand, self.market)
            api2.add_keyword_toadGroup_v0(campaignId, adGroupId, keywordtext, matchType,
                                           'ENABLED', float(bid), self.user)
            return 200
        except Exception as e:
            print(e)
            return 500  # Internal Server Error

    def auto_campaign_name(self, campaignId, new_name):
        try:
            api1 = Gen_campaign(self.db, self.brand, self.market)
            campaign_info = api1.list_camapign(campaignId)
            if campaign_info is not None:
                for item in campaign_info:
                    campaignId = item['campaignId']
                    name = item['name']
                    api1.update_camapign_name(str(campaignId), name, new_name, self.user)
                return 200
            else:
                return 404  # Campaign not found
        except Exception as e:
            print(e)
            return 500  # Internal Server Error
