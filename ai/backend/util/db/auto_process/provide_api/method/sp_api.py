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
    def __init__(self, brand, market):
        self.brand = brand
        self.market = market
        self.exchange_rate = load_config('exchange_rate.json').get('exchange_rate', {}).get("DE", {}).get(self.market)

    def update_sp_ad_budget(self, campaignId, bid):
        try:
            api1 = Gen_campaign(self.brand)
            campaign_info = api1.list_camapign(campaignId, self.market)
            if campaign_info is not None:
                for item in campaign_info:
                    campaignId = item['campaignId']
                    name = item['name']
                    state = item['state']
                    bid1 = item['budget']['budget']
                    api1.update_camapign_v0(self.market, str(campaignId), name, float(bid1), float(bid), state=state)
                return 200
            else:
                return 404  # Campaign not found
        except Exception as e:
            print(e)
            return 500  # Internal Server Error

    def update_sp_ad_placement(self, campaignId, bid, placementClassification):
        try:
            api1 = Gen_campaign(self.brand)
            campaign_info = api1.list_camapign(campaignId, self.market)
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
                                api1.update_campaign_placement(self.market, str(campaignId), bid1, float(bid), placement)
                return 200
            else:
                return 404  # Campaign not found
        except Exception as e:
            print(e)
            return 500  # Internal Server Error

    def update_sp_ad_keyword(self, keywordId, bid):
        try:
            api = Gen_keyword(self.brand)
            api1 = SPKeywordTools(self.brand)
            spkeyword_info = api1.get_spkeyword_api_by_keywordId(self.market, keywordId)
            if spkeyword_info is not None:
                for spkeyword in spkeyword_info:
                    bid1 = spkeyword.get('bid')
                    state = spkeyword['state']
                    api.update_keyword_toadGroup(self.market, str(keywordId), bid1, float(bid), state=state)
                return 200
            else:
                return 404  # Keyword not found
        except Exception as e:
            print(e)
            return 500  # Internal Server Error

    def update_sp_ad_automatic_targeting(self, keywordId, bid):
        try:
            api1 = Gen_adgroup(self.brand)
            api2 = AdGroupTools(self.brand)
            automatic_targeting_info = api2.list_adGroup_TargetingClause_by_targetId(keywordId, self.market)
            if automatic_targeting_info is not None:
                for item in automatic_targeting_info:
                    targetId = item['targetId']
                    state = item['state']
                    bid1 = item.get('bid')
                    api1.update_adGroup_TargetingClause(self.market, str(targetId), float(bid), state=state)
                return 200
            else:
                return 404  # Targeting not found
        except Exception as e:
            print(e)
            return 500  # Internal Server Error

    def update_sp_ad_product_targets(self, keywordId, bid):
        try:
            api1 = Gen_adgroup(self.brand)
            api2 = AdGroupTools(self.brand)
            automatic_targeting_info = api2.list_adGroup_TargetingClause_by_targetId(keywordId, self.market)
            if automatic_targeting_info is not None:
                for automatic_targeting in automatic_targeting_info:
                    targetId = automatic_targeting['targetId']
                    state = automatic_targeting['state']
                    bid1 = automatic_targeting.get('bid')
                    api1.update_adGroup_TargetingClause(self.market, str(targetId), float(bid), state=state)
                return 200
            else:
                return 404  # Targeting not found
        except Exception as e:
            print(e)
            return 500  # Internal Server Error

    def auto_campaign_status(self, campaignId, status):
        try:
            api1 = Gen_campaign(self.brand)
            campaign_info = api1.list_camapign(campaignId, self.market)
            if campaign_info is not None:
                for item in campaign_info:
                    campaignId = item['campaignId']
                    name = item['name']
                    state = item['state']
                    api1.update_camapign_status(self.market, str(campaignId), name, state, status)
                return 200
            else:
                return 404  # Campaign not found
        except Exception as e:
            print(e)
            return 500  # Internal Server Error

    def auto_sku_status(self, adId, status):
        try:
            api = Gen_product(self.brand)
            api.update_product(self.market, str(adId), state=status)
            return 200
        except Exception as e:
            print(e)
            return 500  # Internal Server Error

    def auto_keyword_status(self, keywordId, status):
        try:
            api = Gen_keyword(self.brand)
            api.update_keyword_toadGroup(self.market, str(keywordId), None, bid_new=None, state=status)
            return 200
        except Exception as e:
            print(e)
            return 500  # Internal Server Error

    def auto_targeting_status(self, keywordId, status):
        try:
            api1 = Gen_adgroup(self.brand)
            api1.update_adGroup_TargetingClause(self.market, str(keywordId), bid=None, state=status)
            return 200
        except Exception as e:
            print(e)
            return 500  # Internal Server Error

    def create_product_target(self, keywordId, bid, campaignId, adGroupId):
        try:
            apitool1 = AdGroupTools(self.brand)
            api2 = Gen_adgroup(self.brand)
            brand_info = apitool1.list_category_refinements(self.market, keywordId)
            # 检查是否存在名为"LAPASA"的品牌
            target_brand_name = self.brand
            target_brand_id = None

            for brand in brand_info['brands']:
                if brand['name'] == target_brand_name:
                    target_brand_id = brand['id']
                    targetId = api2.create_adGroup_Targeting2(self.market, campaignId, adGroupId,
                                                              float(bid),
                                                              keywordId, target_brand_id)
                    return 200
            return 404
        except Exception as e:
            print(e)
            return 500  # Internal Server Error