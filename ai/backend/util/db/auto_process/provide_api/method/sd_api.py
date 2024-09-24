from datetime import datetime
import os
import pandas as pd
import json
import ast
from ai.backend.util.db.auto_process.gen_sd_campaign import Gen_campaign
from ai.backend.util.db.auto_process.tools_sd_campaign import CampaignTools
from ai.backend.util.db.auto_process.gen_sp_keyword import Gen_keyword
from ai.backend.util.db.auto_process.gen_sd_adgroup import Gen_adgroup
from ai.backend.util.db.auto_process.gen_sd_product import Gen_product
from ai.backend.util.db.auto_process.tools_sd_adGroup import AdGroupTools_SD
from ai.backend.util.db.auto_process.tools_sp_adGroup import AdGroupTools
from ai.backend.util.db.auto_process.tools_sp_keyword import SPKeywordTools
from ai.backend.util.db.auto_process.create_new_sp_ad_auto import load_config

class auto_api_sd:
    def __init__(self, brand, market, db, user):
        self.brand = brand
        self.market = market
        self.db = db
        self.user = user
        self.exchange_rate = load_config('exchange_rate.json').get('exchange_rate', {}).get("DE", {}).get(self.market)

    def update_sd_ad_budget(self, campaign_id, bid):
        try:
            api1 = CampaignTools(self.db, self.brand, self.market)
            api2 = Gen_campaign(self.db, self.brand, self.market)
            campaign_info = api1.list_campaigns_api(campaign_id)
            if campaign_info:
                campaignId = campaign_info['campaignId']
                name = campaign_info['name']
                state = campaign_info['state']
                bid1 = campaign_info['budget']
                api2.update_camapign_v0(str(campaignId), name, state, "daily", float(bid), float(bid1), self.user)
                return 200
            else:
                return 404  # Campaign not found
        except Exception as e:
            print(e)
            return 500  # Internal Server Error

    def update_sd_ad_product_targets(self, keywordId, bid):
        try:
            api1 = Gen_adgroup(self.db, self.brand, self.market)
            api2 = AdGroupTools_SD(self.db, self.brand, self.market)
            automatic_targeting_info = api2.list_adGroup_Targeting_by_targetId(keywordId)
            if automatic_targeting_info:
                targetId = automatic_targeting_info['targetId']
                state = automatic_targeting_info['state']
                res = api1.update_adGroup_Targeting(str(targetId), float(bid), state, self.user)
                if res:
                    return 200
                else:
                    return 500
            else:
                return 404  # Targeting not found
        except Exception as e:
            print(e)
            return 500  # Internal Server Error

    def auto_campaign_status(self, campaignId, status):
        try:
            api1 = Gen_campaign(self.db, self.brand, self.market)
            api2 = CampaignTools(self.db, self.brand, self.market)
            campaign_info = api2.list_campaigns_api(campaignId)
            if campaign_info:
                campaignId = campaign_info['campaignId']
                name = campaign_info['name']
                state = campaign_info['state']
                res = api1.update_camapign_status(str(campaignId), name, state, status.lower(), self.user)
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
            api.update_product(str(adId), status.lower(), self.user)
            return 200
        except Exception as e:
            print(e)
            return 500  # Internal Server Error

    def auto_targeting_status(self, keywordId, status):
        try:
            api1 = Gen_adgroup(self.db, self.brand, self.market)
            api1.update_adGroup_Targeting(str(keywordId), bid=None, state=status.lower(), user=self.user)
            return 200
        except Exception as e:
            print(e)
            return 500  # Internal Server Error

    def create_product_target(self, keywordId, bid, campaignId, adGroupId):
        try:
            apitool1 = AdGroupTools(self.db, self.brand, self.market)
            api3 = Gen_adgroup(self.db, self.brand, self.market)
            brand_info = apitool1.list_category_refinements(keywordId)
            # 检查是否存在名为"LAPASA"的品牌
            target_brand_name = self.brand
            target_brand_id = None
            print('1')
            for brand in brand_info['brands']:
                if brand['name'] == target_brand_name:
                    target_brand_id = brand['id']
                    new_targetId = api3.create_adGroup_Targeting2(adGroupId, keywordId, target_brand_id,
                                                                  expression_type='manual', state='enabled',
                                                                  bid=float(bid), user=self.user)
                    return 200
            return 404
        except Exception as e:
            print(e)
            return 500  # Internal Server Error

    def create_product_target_new(self, keywordId, bid, campaignId, adGroupId):
        try:
            api3 = Gen_adgroup(self.db, self.brand, self.market)
            # 检查是否存在名为"LAPASA"的品牌
            variable = ast.literal_eval(keywordId)
            new_targetId = api3.create_adGroup_Targeting4(adGroupId, variable,
                                                                  expression_type='manual', state='enabled',
                                                                  bid=float(bid), user=self.user)
            return 200
        except Exception as e:
            print(e)
            return 500  # Internal Server Error

    def create_product_target_asin(self, asin, bid, adGroupId):
        try:
            api2 = Gen_adgroup(self.db, self.brand, self.market)
            api2.create_adGroup_Targeting3(adGroupId, asin, 'manual', 'enabled',float(bid), self.user)
            return 200
        except Exception as e:
            print(e)
            return 500  # Internal Server Error

    def auto_campaign_name(self, campaignId, new_name):
        try:
            api1 = Gen_campaign(self.db, self.brand, self.market)
            api2 = CampaignTools(self.db, self.brand, self.market)
            campaign_info = api2.list_campaigns_api(campaignId)
            if campaign_info:
                campaignId = campaign_info['campaignId']
                name = campaign_info['name']
                api1.update_camapign_name(str(campaignId), name, new_name, self.user)
                return 200
            else:
                return 404  # Campaign not found
        except Exception as e:
            print(e)
            return 500  # Internal Server Error
