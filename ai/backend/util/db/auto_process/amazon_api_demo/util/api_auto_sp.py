import json
import os
import pandas as pd
from ai.backend.util.db.auto_process.tools_sp_keyword import SPKeywordTools
from ai.backend.util.db.auto_process.gen_sp_keyword import Gen_keyword
from ai.backend.util.db.auto_process.tools_sp_adGroup import AdGroupTools
from ai.backend.util.db.auto_process.gen_sp_adgroup import Gen_adgroup
from ai.backend.util.db.auto_process.gen_sp_campaign import Gen_campaign
from ai.backend.util.db.auto_process.gen_sp_product import Gen_product
from ai.backend.util.db.auto_process.tools_sp_budget_rules import BudgetRulesTools
from ai.backend.util.db.auto_process.gen_sp_budget_rules import GenBudgetRule


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

    def auto_create_targeting_category(self,market,path):
        uploaded_file = path
        if os.stat(uploaded_file).st_size > 2:
            df = pd.read_csv(uploaded_file)
            # 打印 DataFrame 的前几行，以确保成功读取
            print(df.head())
            # 将DataFrame转换为一个列表，每个元素是一个字典表示一行数据
            df_data = json.loads(df.to_json(orient='records'))
            print(df_data)
            apitool1 = AdGroupTools(self.brand)
            api2 = Gen_adgroup(self.brand)
            for item in df_data:
                category_id = item['category_id']
                campaignId = item['campaignId']
                adGroupId = item['adGroupId']
                bid = item['bid']
                brand_info = apitool1.list_category_refinements(market, category_id)
                # 检查是否存在名为"LAPASA"的品牌
                target_brand_name = self.brand
                target_brand_id = None
                for brand in brand_info['brands']:
                    if brand['name'] == target_brand_name:
                        target_brand_id = brand['id']
                        try:
                            targetId = api2.create_adGroup_Targeting2(market, campaignId, adGroupId, bid,
                                                                  category_id, target_brand_id)
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
                campaignId = item['campaignId']
                adGroupId = item['adGroupId']
                type = item['type']
                bid = item['bid']
                try:
                    new_targetId = api1.create_adGroup_Targeting1(market, campaignId, adGroupId, asin, bid,'ENABLED', type)
                except Exception as e:
                    # 处理异常，可以打印异常信息或者进行其他操作
                    print("An error occurred:", e)

    def auto_create_keyword(self,market,path):
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
                keyword = item['keyword']
                campaignId = item['campaignId']
                adGroupId = item['adGroupId']
                matchType = item['matchType']
                bid = item['bid']
                try:
                    api.add_keyword_toadGroup_v0(market, str(int(campaignId)), str(int(adGroupId)), keyword,
                                             matchType=matchType, state="ENABLED", bid=float(bid))
                except Exception as e:
                    # 处理异常，可以打印异常信息或者进行其他操作
                    print("An error occurred:", e)

    def auto_create_negative_targeting(self,market,path):
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
                campaignId = item['campaignId']
                adGroupId = item['adGroupId']
                try:
                    api1.create_adGroup_Negative_Targeting_by_asin(market, str(campaignId), str(adGroupId),
                                                                   asin)
                except Exception as e:
                    # 处理异常，可以打印异常信息或者进行其他操作
                    print("An error occurred:", e)

    def auto_create_negative_keyword(self,market,path):
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
                keyword = item['keyword']
                campaignId = item['campaignId']
                adGroupId = item['adGroupId']
                matchType = item['matchType']
                try:
                    api1.add_adGroup_negative_keyword_v0(market, str(campaignId), str(adGroupId), keyword,
                                                         matchType=matchType, state="ENABLED")
                except Exception as e:
                    print("An error occurred:", e)

    def auto_create_BudgetRules(self,market,path,params_modify):
        uploaded_file = path
        if os.stat(uploaded_file).st_size > 2:
            df = pd.read_csv(uploaded_file)
            # 打印 DataFrame 的前几行，以确保成功读取
            print(df.head())
            # 将DataFrame转换为一个列表，每个元素是一个字典表示一行数据
            df_data = json.loads(df.to_json(orient='records'))
            print(df_data)
            apitool1 = GenBudgetRule(self.brand)
            api2 = BudgetRulesTools(self.brand)
            for item in df_data:
                campaignId = item['campaignId']
                ruleId = apitool1.create_budget_rules(market,None if params_modify['enddate'] == '' else params_modify['enddate'], params_modify['startTime'], params_modify['endTime'],
                                                          params_modify['Type'], int(params_modify['budget_increase']), params_modify['Name'],
                                                          params_modify['metricName'], params_modify['comparisonOperator'], int(params_modify['performanceValue'] or '0'),
                                                          params_modify['supportDailySchedule'])
                api2.Associates_budget_rules_to_campaign(ruleId,campaignId,market)
# api = auto_api('LAPASA')
# #auto_campaign_keyword('FR',293153114778136)
# #api.auto_campaign_product_targets('FR',493452433045396)
# #api.auto_campaign_automatic_targeting('DE',383490782268653)
# #api.auto_campaign_budget('DE',383490782268653)
# api.auto_campaign_targeting_group('DE',289390472887270)
