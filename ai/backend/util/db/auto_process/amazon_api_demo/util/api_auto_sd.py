import json
import os
import pandas as pd
from ai.backend.util.db.auto_process.tools_sd_adGroup import AdGroupTools_SD
from ai.backend.util.db.auto_process.gen_sd_adgroup import Gen_adgroup
from ai.backend.util.db.auto_process.gen_sd_campaign import Gen_campaign
from ai.backend.util.db.auto_process.gen_sd_product import Gen_product
from ai.backend.util.db.auto_process.tools_sd_campaign import CampaignTools
from ai.backend.util.db.auto_process.tools_sp_adGroup import AdGroupTools


class auto_api_sd:
    def __init__(self,brand):
        self.brand = brand

    def auto_campaign_product_targets(self,market,path):
        uploaded_file = path
        if os.stat(uploaded_file).st_size > 2:
            df = pd.read_csv(uploaded_file)
            # 打印 DataFrame 的前几行，以确保成功读取
            print(df.head())
            # 将DataFrame转换为一个列表，每个元素是一个字典表示一行数据
            df_data = json.loads(df.to_json(orient='records'))
            print(df_data)
            api1 = AdGroupTools_SD(self.brand)
            api2 = Gen_adgroup(self.brand)
            for item in df_data:
                campaignId = item["campaignId"]
                bid_adjust = item["bid_adjust"]
                product_targets_info = api1.list_adGroup_Targeting_by_campaignId(market,campaignId)
                print(product_targets_info)
                if product_targets_info is not None:
                    for item in product_targets_info:
                        targetId = item['targetId']
                        state = item['state']
                        bid1 = item.get('bid')
                        if bid1 is not None:  # 检查是否存在有效的bid值
                            bid = bid1 + bid_adjust
                            if state != "ENABLED":
                                continue  # 跳过当前迭代，进入下一次迭代
                            try:
                                api2.update_adGroup_Targeting(market, str(targetId), float(bid), state="ENABLED")
                            except Exception as e:
                                print(e)

    def auto_campaign_budget(self,market,path):
        uploaded_file = path
        if os.stat(uploaded_file).st_size > 2:
            df = pd.read_csv(uploaded_file)
            # 打印 DataFrame 的前几行，以确保成功读取
            print(df.head())
            # 将DataFrame转换为一个列表，每个元素是一个字典表示一行数据
            df_data = json.loads(df.to_json(orient='records'))
            print(df_data)
            api1 = Gen_campaign(self.brand)
            api2 = CampaignTools(self.brand)
            for item in df_data:
                campaignId = item["campaignId"]
                bid_adjust = item["bid_adjust"]
                campaign_info = api2.list_campaigns_api(campaignId,market)
                print(campaign_info)
                if campaign_info is not None:
                    campaignId = campaign_info['campaignId']
                    name = campaign_info['name']
                    state = campaign_info['state']
                    bid1 = campaign_info['budget']
                    if bid1 is not None:  # 检查是否存在有效的bid值
                        bid = bid1 + bid_adjust
                        if state != "ENABLED":
                            continue  # 跳过当前迭代，进入下一次迭代
                        try:
                            api1.update_camapign_v0(market, str(campaignId), name, state, 'daily', float(bid), float(bid1))
                        except Exception as e:
                            print(e)

    def auto_campaign_status(self,market,path,status):
        uploaded_file = path
        if os.stat(uploaded_file).st_size > 2:
            df = pd.read_csv(uploaded_file)
            # 打印 DataFrame 的前几行，以确保成功读取
            print(df.head())
            # 将DataFrame转换为一个列表，每个元素是一个字典表示一行数据
            df_data = json.loads(df.to_json(orient='records'))
            print(df_data)
            api1 = Gen_campaign(self.brand)
            api2 = CampaignTools(self.brand)
            for item in df_data:
                campaignId = item["campaignId"]
                campaign_info = api2.list_campaigns_api(campaignId,market)
                print(campaign_info)
                if campaign_info is not None:
                    campaignId = campaign_info['campaignId']
                    name = campaign_info['name']
                    state = campaign_info['state']
                    try:
                        api1.update_camapign_status(market, str(campaignId), name, state, status.lower())
                    except Exception as e:
                        print(e)

    def auto_sku_status(self,market,path,status):
        uploaded_file = path
        if os.stat(uploaded_file).st_size > 2:
            df = pd.read_csv(uploaded_file)
            # 打印 DataFrame 的前几行，以确保成功读取
            print(df.head())
            # 将DataFrame转换为一个列表，每个元素是一个字典表示一行数据
            df_data = json.loads(df.to_json(orient='records'))
            print(df_data)
            api = Gen_product(self.brand)
            for item in df_data:
                adId = item["adId"]
                api.update_product(market, str(adId), state=status.lower())

    def auto_targeting_status(self,market,path,status):
        uploaded_file = path
        if os.stat(uploaded_file).st_size > 2:
            df = pd.read_csv(uploaded_file)
            # 打印 DataFrame 的前几行，以确保成功读取
            print(df.head())
            # 将DataFrame转换为一个列表，每个元素是一个字典表示一行数据
            df_data = json.loads(df.to_json(orient='records'))
            print(df_data)
            api1 = Gen_adgroup(self.brand)
            for item in df_data:
                keywordId = item["keywordId"]
                api1.update_adGroup_Targeting(market, str(keywordId), bid=None, state=status.lower())

    def auto_create_targeting_category(self,market,path):
        uploaded_file = path
        if os.stat(uploaded_file).st_size > 2:
            df = pd.read_csv(uploaded_file)
            # 打印 DataFrame 的前几行，以确保成功读取
            print(df.head())
            # 将DataFrame转换为一个列表，每个元素是一个字典表示一行数据
            df_data = json.loads(df.to_json(orient='records'))
            print(df_data)
            api1 = Gen_adgroup(self.brand)
            api3 = AdGroupTools(self.brand)
            for item in df_data:
                category_id = item['category_id']
                adGroupId = item['adGroupId']
                bid = item['bid']

                brand_info = api3.list_category_refinements(market, category_id)
                # 检查是否存在名为"LAPASA"的品牌
                target_brand_name = self.brand
                target_brand_id = None
                for brand in brand_info['brands']:
                    if brand['name'] == target_brand_name:
                        target_brand_id = brand['id']
                        try:
                            new_targetId = api1.create_adGroup_Targeting2(market, adGroupId, category_id,
                                                                          target_brand_id,
                                                                          expression_type='manual', state='enabled',
                                                                          bid=bid)
                        except Exception as e:
                            # 处理异常，可以打印异常信息或者进行其他操作
                            print("An error occurred:", e)

    def auto_create_targeting_product(self,market,path):
        uploaded_file = path
        if os.stat(uploaded_file).st_size > 2:
            df = pd.read_csv(uploaded_file)
            # 打印 DataFrame 的前几行，以确保成功读取
            print(df.head())
            # 将DataFrame转换为一个列表，每个元素是一个字典表示一行数据
            df_data = json.loads(df.to_json(orient='records'))
            print(df_data)
            api1 = Gen_adgroup(self.brand)
            for item in df_data:
                asin = item['asin']
                adGroupId = item['adGroupId']
                bid = item['bid']
                try:
                    new_targetId = api1.create_adGroup_Targeting3(market, adGroupId, asin, 'manual', 'enabled', bid)
                except Exception as e:
                    # 处理异常，可以打印异常信息或者进行其他操作
                    print("An error occurred:", e)

    def auto_create_negative_targeting_product(self,market,path):
        uploaded_file = path
        if os.stat(uploaded_file).st_size > 2:
            df = pd.read_csv(uploaded_file)
            # 打印 DataFrame 的前几行，以确保成功读取
            print(df.head())
            # 将DataFrame转换为一个列表，每个元素是一个字典表示一行数据
            df_data = json.loads(df.to_json(orient='records'))
            print(df_data)
            api1 = Gen_adgroup(self.brand)
            for item in df_data:
                asin = item['asin']
                adGroupId = item['adGroupId']
                try:
                    new_targetId = api1.create_adGroup_negative_targeting(market, adGroupId, asin, 'manual', 'enabled')
                except Exception as e:
                    # 处理异常，可以打印异常信息或者进行其他操作
                    print("An error occurred:", e)

# api = auto_api('LAPASA')
# #auto_campaign_keyword('FR',293153114778136)
# #api.auto_campaign_product_targets('FR',493452433045396)
# #api.auto_campaign_automatic_targeting('DE',383490782268653)
# #api.auto_campaign_budget('DE',383490782268653)
# api.auto_campaign_targeting_group('DE',289390472887270)
