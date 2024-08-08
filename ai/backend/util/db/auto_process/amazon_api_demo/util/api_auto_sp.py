import json
import os
import pandas as pd
from ai.backend.util.db.auto_process.tools_sp_keyword import SPKeywordTools
from ai.backend.util.db.auto_process.gen_sp_keyword import Gen_keyword
from ai.backend.util.db.auto_process.tools_sp_adGroup import AdGroupTools
from ai.backend.util.db.auto_process.gen_sp_adgroup import Gen_adgroup
from ai.backend.util.db.auto_process.gen_sp_campaign import Gen_campaign
from backend.util.db.auto_process.gen_sp_product import Gen_product


class auto_api_sp:
    def __init__(self,brand):
        self.brand = brand

    def auto_campaign_keyword(self,market,path):
        uploaded_file = path
        if os.stat(uploaded_file).st_size > 2:
            df = pd.read_csv(uploaded_file)
            # 打印 DataFrame 的前几行，以确保成功读取
            print(df.head())
            # 将DataFrame转换为一个列表，每个元素是一个字典表示一行数据
            df_data = json.loads(df.to_json(orient='records'))
            print(df_data)
            api1 = SPKeywordTools(self.brand)
            api2 = Gen_keyword(self.brand)
            for item in df_data:
                campaignId = item["campaignId"]
                bid_adjust = item["bid_adjust"]
                spkeyword_info = api1.get_spkeyword_api_by_campaignid(market,campaignId)
                if spkeyword_info is not None:
                    for item in spkeyword_info:
                        keywordId = item['keywordId']
                        state = item['state']
                        bid1 = item.get('bid')
                        if bid1 is not None:  # 检查是否存在有效的bid值
                            bid = bid1 + bid_adjust
                            if state != "ENABLED":
                                continue  # 跳过当前迭代，进入下一次迭代
                            try:
                                api2.update_keyword_toadGroup(market, str(keywordId), bid1, bid, state="ENABLED")
                            except Exception as e:
                                print(e)

    def auto_campaign_product_targets(self,market,path):
        uploaded_file = path
        if os.stat(uploaded_file).st_size > 2:
            df = pd.read_csv(uploaded_file)
            # 打印 DataFrame 的前几行，以确保成功读取
            print(df.head())
            # 将DataFrame转换为一个列表，每个元素是一个字典表示一行数据
            df_data = json.loads(df.to_json(orient='records'))
            print(df_data)
            api1 = AdGroupTools(self.brand)
            api2 = Gen_adgroup(self.brand)
            for item in df_data:
                campaignId = item["campaignId"]
                bid_adjust = item["bid_adjust"]
                product_targets_info = api1.list_adGroup_TargetingClause_by_campaignId(campaignId,market)
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
                                api2.update_adGroup_TargetingClause(market, str(targetId), float(bid), state="ENABLED")
                            except Exception as e:
                                print(e)

    def auto_campaign_automatic_targeting(self,market,path):
        uploaded_file = path
        if os.stat(uploaded_file).st_size > 2:
            df = pd.read_csv(uploaded_file)
            # 打印 DataFrame 的前几行，以确保成功读取
            print(df.head())
            # 将DataFrame转换为一个列表，每个元素是一个字典表示一行数据
            df_data = json.loads(df.to_json(orient='records'))
            print(df_data)
            api1 = AdGroupTools(self.brand)
            api2 = Gen_adgroup(self.brand)
            for item in df_data:
                campaignId = item["campaignId"]
                bid_adjust = item["bid_adjust"]
                automatic_targeting_info = api1.list_adGroup_TargetingClause_by_campaignId(campaignId,market)
                print(automatic_targeting_info)
                if automatic_targeting_info is not None:
                    for item in automatic_targeting_info:
                        targetId = item['targetId']
                        state = item['state']
                        bid1 = item.get('bid')
                        if bid1 is not None:  # 检查是否存在有效的bid值
                            bid = bid1 + bid_adjust
                            if state != "ENABLED":
                                continue  # 跳过当前迭代，进入下一次迭代
                            try:
                                api2.update_adGroup_TargetingClause(market, str(targetId), float(bid), state="ENABLED")
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
            for item in df_data:
                campaignId = item["campaignId"]
                bid_adjust = item["bid_adjust"]
                campaign_info = api1.list_camapign(campaignId,market)
                print(campaign_info)
                if campaign_info is not None:
                    for item in campaign_info:
                        campaignId = item['campaignId']
                        name = item['name']
                        state = item['state']
                        bid1 = item['budget']['budget']
                        if bid1 is not None:  # 检查是否存在有效的bid值
                            bid = bid1 + bid_adjust
                            if state != "ENABLED":
                                continue  # 跳过当前迭代，进入下一次迭代
                            try:
                                api1.update_camapign_v0(market, str(campaignId), name, float(bid1), float(bid), state="ENABLED")
                            except Exception as e:
                                print(e)

    def auto_campaign_targeting_group(self,market,path):
        uploaded_file = path
        if os.stat(uploaded_file).st_size > 2:
            df = pd.read_csv(uploaded_file)
            # 打印 DataFrame 的前几行，以确保成功读取
            print(df.head())
            # 将DataFrame转换为一个列表，每个元素是一个字典表示一行数据
            df_data = json.loads(df.to_json(orient='records'))
            print(df_data)
            api1 = Gen_campaign(self.brand)
            for item in df_data:
                campaignId = item["campaignId"]
                bid_adjust = item["bid_adjust"]
                campaign_info = api1.list_camapign(campaignId,market)
                print(campaign_info)
                if campaign_info is not None:
                    for item in campaign_info:
                        # 获取 dynamicBidding 中的 placementBidding 列表
                        placement_bidding = item['dynamicBidding']['placementBidding']

                        # 定义可能的 placement 类型列表
                        possible_placements = ['PLACEMENT_REST_OF_SEARCH', 'PLACEMENT_PRODUCT_PAGE', 'PLACEMENT_TOP']

                        # 定义一个字典来存储各个 placement 的 percentage，默认设置为 0
                        placement_percentages = {placement: 0 for placement in possible_placements}

                        # 遍历 placementBidding 列表，更新 placement_percentages 中存在的项
                        for item1 in placement_bidding:
                            placement = item1['placement']
                            percentage = item1['percentage']

                            # 只更新存在于 possible_placements 中的 placement
                            if placement in possible_placements:
                                placement_percentages[placement] = percentage

                        campaignId = item['campaignId']
                        # 遍历字典，打印每个 placement 和对应的 percentage
                        for placement, percentage in placement_percentages.items():
                            print(f'Placement: {placement}, Percentage: {percentage}')
                            bid1 = percentage
                            if bid1 is not None:  # 检查是否存在有效的bid值
                                bid = bid1 + bid_adjust
                                try:
                                    api1.update_campaign_placement(market, str(campaignId), bid1, bid, placement)
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
            for item in df_data:
                campaignId = item["campaignId"]
                campaign_info = api1.list_camapign(campaignId,market)
                print(campaign_info)
                if campaign_info is not None:
                    for item in campaign_info:
                        campaignId = item['campaignId']
                        name = item['name']
                        state = item['state']
                        try:
                            api1.update_camapign_status(market, str(campaignId), name, state, status)
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
                api.update_product(market, str(adId), state=status)

    def auto_keyword_status(self,market,path,status):
        uploaded_file = path
        if os.stat(uploaded_file).st_size > 2:
            df = pd.read_csv(uploaded_file)
            # 打印 DataFrame 的前几行，以确保成功读取
            print(df.head())
            # 将DataFrame转换为一个列表，每个元素是一个字典表示一行数据
            df_data = json.loads(df.to_json(orient='records'))
            print(df_data)
            api = Gen_keyword(self.brand)
            for item in df_data:
                keywordId = item["keywordId"]
                api.update_keyword_toadGroup(market, str(keywordId), None, bid_new=None, state=status)

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
                api1.update_adGroup_TargetingClause(market, str(keywordId), bid=None, state=status)


# api = auto_api('LAPASA')
# #auto_campaign_keyword('FR',293153114778136)
# #api.auto_campaign_product_targets('FR',493452433045396)
# #api.auto_campaign_automatic_targeting('DE',383490782268653)
# #api.auto_campaign_budget('DE',383490782268653)
# api.auto_campaign_targeting_group('DE',289390472887270)
