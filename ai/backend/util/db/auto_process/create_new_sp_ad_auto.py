import os
import pandas as pd
import json
import time
from datetime import datetime

import yaml

from ai.backend.util.db.configuration.path import get_config_path
from ai.backend.util.db.auto_process.tools_sp_campaign import CampaignTools
from ai.backend.util.db.auto_process.gen_sp_campaign import Gen_campaign
from ai.backend.util.db.auto_process.tools_sp_adGroup import AdGroupTools
from ai.backend.util.db.auto_process.gen_sp_adgroup import Gen_adgroup  #create_adgroup, add_adGroup_negative_keyword, update_adGroup_TargetingClause,create_adGroup_Targeting1,create_adGroup_Targeting2
from ai.backend.util.db.auto_process.tools_sp_product import ProductTools
from ai.backend.util.db.auto_process.gen_sp_product import Gen_product  #create_productsku
from ai.backend.util.db.auto_process.tools_sp_keyword import SPKeywordTools
from ai.backend.util.db.auto_process.gen_sp_keyword import Gen_keyword  #add_keyword_toadGroup,add_keyword_toadGroup_v0
from ai.backend.util.db.auto_process.tools_db_new_sp import DbNewSpTools
from ai.backend.util.db.auto_process.tools_db_sp import DbSpTools


def load_config(config_file):
    config_path = os.path.join(get_config_path(), config_file)
    with open(config_path) as f:
        return json.load(f) if config_file.endswith('.json') else yaml.safe_load(f)


class Ceate_new_sku:

    def __init__(self, db, brand, market):
        self.brand = brand
        self.market = market
        self.db = db
        self.exchange_rate = load_config('exchange_rate.json')
        self.depository = load_config('Brand.yml')

    def get_exchange_rate(self, market1, market2):
        return self.exchange_rate.get('exchange_rate', {}).get(market2, {}).get(market1)

    def select_depository(self):
        brand_info = self.depository.get(self.db, {})
        if self.brand:
            sub_brand_info = brand_info.get(self.brand, {})
            if self.market:
                country_info = sub_brand_info.get(self.market, {})
                return country_info.get('depository', brand_info.get('default', {}).get('depository'))
            return brand_info.get('depository', brand_info.get('default', {}).get('depository'))

    def create_new_sku(self,market1,market2,brand_name,uploaded_file,budget):
        #uploaded_file = 'C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/db_amazon/法国复刻德国M0708_手动.csv'
        uploaded_file = uploaded_file
        df = pd.read_csv(uploaded_file)
        # 打印 DataFrame 的前几行，以确保成功读取
        print(df.head())
        # 将DataFrame转换为一个列表，每个元素是一个字典表示一行数据
        df_data = json.loads(df.to_json(orient='records'))

        df_processed = pd.DataFrame(df_data)
        result = df_processed.groupby(['campaignId', 'adGroupId', 'new_campaignName', 'new_adGroupName'])['advertisedSku'].agg(list)
        exchange_rate = self.get_exchange_rate(market1, market2)
        processed_ids = {'campaign_ids': set(), 'ad_group_ids': set()}
        # 循环处理每一行数据
        for (campaign_id, ad_group_id, new_campaign_name, new_ad_group_name), advertised_skus_list in result.items():

            if campaign_id not in processed_ids['campaign_ids']:
                # 如果是第一次遇到，执行相应操作
                # 将当前的 campaign_id 添加到已处理的字典中
                processed_ids['campaign_ids'].add(campaign_id)
                processed_ids['ad_group_ids'].add(ad_group_id)
                # 执行创建
                apitool = Gen_campaign(brand_name)
                res = apitool.list_camapign(campaign_id, market2)
                for item in res:
                    if isinstance(item, dict):
                        if item.get('targetingType') == 'AUTO':
                            answer_message = self.auto_targeting(res, market1, market2, new_campaign_name, ad_group_id, new_ad_group_name, exchange_rate,advertised_skus_list,brand_name,budget)
                            print(answer_message)
                            continue
                        else:
                            answer_message = self.manual_targeting(res, market1, market2, new_campaign_name, ad_group_id, new_ad_group_name, exchange_rate,advertised_skus_list,brand_name,budget)
                            print(answer_message)
                            continue

            else:
                # TODO 同一campaign不同ad_group的处理
                # 如果不是第一次遇到 campaign_id，则再检查 ad_group_id 是否是第一次遇到
                if ad_group_id not in processed_ids['ad_group_ids']:
                    # 如果是第一次遇到 ad_group_id，执行相应操作
                    # 将当前的 ad_group_id 添加到已处理的字典中
                    processed_ids['ad_group_ids'].add(ad_group_id)
                    print("Unprocessed ad groups:",ad_group_id)
                    # 因为 ad_group_id 是第一次遇到，所以我们可以直接进入下一次迭代
                    continue

                # 如果 ad_group_id 不是第一次遇到，则直接进行下一次迭代
                continue



    def auto_targeting(self, res, market1,market2, new_campaign_name, ad_group_id, new_ad_group_name, exchange_rate,advertisedSku,brand_name,budget):
        dynamicBidding = res[0]['dynamicBidding']
        # 获取当前日期
        today = datetime.today()
        # 格式化输出
        startDate = today.strftime('%Y-%m-%d')
        # name = 'DeepBI_AUTO_test'
        name = new_campaign_name
        apitool = Gen_campaign(brand_name)
        new_campaign_id = apitool.create_camapign(market1, name, startDate, dynamicBidding, portfolioId=None, endDate=None, targetingType='AUTO', state='PAUSED', budgetType='DAILY', budget=float(budget))
        #new_campaign_id ='333199332378491'
        if new_campaign_id == "":
            return "No new campaign"
        apitool1 = AdGroupTools(brand_name)
        defaultBid_old = exchange_rate * apitool1.get_adGroup_api(market2, ad_group_id)
        #new_adgroup_id = 437018151615304
        apitool2 = Gen_adgroup(brand_name)
        new_adgroup_id = apitool2.create_adgroup(market1, new_campaign_id, new_ad_group_name, defaultBid_old, state='ENABLED')
        market2_TargetingClause = apitool1.list_adGroup_TargetingClause(ad_group_id,market2)
        market1_TargetingClause = apitool1.list_adGroup_TargetingClause(new_adgroup_id, market1)
        if market2_TargetingClause:
            state_bid_info = {}
            for item in market2_TargetingClause:
                type_ = item['expression'][0]['type']
                bid = item.get('bid')  # 获取bid值，如果不存在则为None
                if bid is not None:  # 检查是否存在有效的bid值
                    state_bid_info[type_] = {'state': item['state'], 'bid': bid}

            # 将'state'和'bid'信息应用到第二个查询结果中相同类型的条目中
            for item in market1_TargetingClause:
                type_ = item['expression'][0]['type']
                if type_ in state_bid_info:
                    target_id = item['targetId']
                    state = state_bid_info[type_]['state']
                    bid = exchange_rate * state_bid_info[type_]['bid']
                    targetId = apitool2.update_adGroup_TargetingClause(market1,target_id, bid, state)

        negativekw_info = apitool1.get_adGroup_negativekw(market2,ad_group_id)
        if negativekw_info == None:
            pass
        else:
            # 循环遍历列表中的每个字典
            for item in negativekw_info:
                keyword_text = item['keywordText']
                match_type = item['matchType']
                state = item['state']
                if state != "ENABLED" and state != "PAUSED":
                    continue  # 跳过当前迭代，进入下一次迭代
                try:
                    new_negativeKeywordId= apitool2.add_adGroup_negative_keyword(market1, new_campaign_id,new_adgroup_id,keyword_text,match_type,state)
                except Exception as e:
                # 处理异常，可以打印异常信息或者进行其他操作
                    print("An error occurred:", e)
                    newdbtool = DbNewSpTools(brand_name,market1)
                    newdbtool.add_sp_adGroup_negativeKeyword(market1, None, new_adgroup_id, new_campaign_id, None,
                                                             match_type,
                                                             state, keyword_text, None,
                                                             None, "failed", datetime.now())
                # 继续执行下面的代码

                else:
                # 如果没有发生异常，则执行以下代码
                # 这里可以放一些正常情况下的逻辑
                    print("Command executed successfully.")
        # apitool2 = ProductTools()
        # product_info = apitool2.get_product_api(market2,ad_group_id)
        # if product_info == None:
        #     pass
        # else:
        #     # 循环遍历列表中的每个字典
        #     for item in product_info:
        #         sku = item['sku']
        #         new_sku = create_productsku(market1, new_campaign_id,new_adgroup_id,sku,asin=None,state='ENABLED')

        apitool3 = DbSpTools(brand_name,market1)
        apitool4 = ProductTools(brand_name)
        if market1 == 'US' or market2 == 'US':
            sku_info = apitool3.select_product_sku(market1, market2, advertisedSku)
        else:
            sku_info = apitool3.select_product_sku_by_asin(market1, market2, advertisedSku,self.select_depository(brand_name,market1))
        api3 = Gen_product(brand_name)
        for sku in sku_info:
            try:
                new_sku = api3.create_productsku(market1, new_campaign_id, new_adgroup_id, sku, asin=None, state='ENABLED')
            except Exception as e:
                # 处理异常，可以打印异常信息或者进行其他操作
                print("An error occurred create_productsku:", e)
                newdbtool = DbNewSpTools(brand_name,market1)
                newdbtool.create_sp_product(market1, new_campaign_id, None, sku, new_adgroup_id, None, "failed",
                                            datetime.now(), "SP")
        return f"{new_campaign_name} create successfully"

    def manual_targeting(self, res, market1, market2, new_campaign_name, ad_group_id, new_ad_group_name, exchange_rate, advertisedSku, brand_name,budget):
        dynamicBidding = res[0]['dynamicBidding']
        # 获取当前日期
        today = datetime.today()
        # 格式化输出
        startDate = today.strftime('%Y-%m-%d')
        name = new_campaign_name
        # name = 'DeepBI_MANUAL_test'
        apitool = Gen_campaign(brand_name)
        new_campaign_id = apitool.create_camapign(market1, name, startDate, dynamicBidding, portfolioId=None,
                                                  endDate=None, targetingType='MANUAL', state='PAUSED',
                                                  budgetType='DAILY', budget=float(budget))
        # new_campaign_id ='388952063430466'
        if new_campaign_id == "":
            return "No new campaign"
        apitool1 = AdGroupTools(brand_name)
        defaultBid_old = exchange_rate * apitool1.get_adGroup_api(market2, ad_group_id)
        # new_adgroup_id = '546877730565898'
        apitool5 = Gen_adgroup(brand_name)
        new_adgroup_id = apitool5.create_adgroup(market1, new_campaign_id, new_ad_group_name, defaultBid_old, state='ENABLED')
        apitool3 = DbSpTools(brand_name,market1)
        apitool4 = ProductTools(brand_name)

        # product_info = apitool4.get_product_api(market2, ad_group_id)
        # if product_info == None:
        #     pass
        # else:
        #     # 循环遍历列表中的每个字典
        #     for item in product_info:
        #         sku = item['sku']
        #         new_sku = create_productsku(market1, new_campaign_id, new_adgroup_id, sku, asin=None, state='ENABLED')
        # 通过表
        api3 = Gen_product(brand_name)
        if market1 == 'US' or market2 == 'US':
            sku_info = apitool3.select_product_sku(market1, market2, advertisedSku)
        else:
            sku_info = apitool3.select_product_sku_by_asin(market1, market2, advertisedSku,self.select_depository(brand_name,market1))
        for sku in sku_info:
            try:
                new_sku = api3.create_productsku(market1, new_campaign_id, new_adgroup_id, sku, asin=None, state='ENABLED')
            except Exception as e:
                # 处理异常，可以打印异常信息或者进行其他操作
                print("An error occurred create_productsku:", e)
                newdbtool = DbNewSpTools(brand_name,market1)
                newdbtool.create_sp_product(market1, new_campaign_id, None, sku, new_adgroup_id, None, "failed",
                                            datetime.now(), "SP")

        apitool2 = SPKeywordTools(brand_name)
        spkeyword_info = apitool2.get_spkeyword_api(market2, ad_group_id)
        market2_TargetingClause = apitool1.list_adGroup_TargetingClause(ad_group_id, market2)

        if spkeyword_info == None and market2_TargetingClause[0] != "failed":
            # 添加推荐商品，暂时不用此功能
            # for result in market2_TargetingClause:
            #     expression_type = result['expression'][0]['type']
            #     if expression_type == 'ASIN_SAME_AS':
            #         value = result['expression'][0]['value']
            #         asin = apitool3.select_sp_product_asin(market1,market2,value)
            #         if asin != None:
            #             bid_info = apitool1.list_product_bid_recommendations(market1, asin, new_campaign_id,
            #                                                                   new_adgroup_id)
            #             bid = bid_info['bidRecommendations'][0]['bidRecommendationsForTargetingExpressions'][0]['bidValues'][1]['suggestedBid']
            #             state = result['state']
            #             targetId = create_adGroup_Targeting1(market1,new_campaign_id,new_adgroup_id,asin,bid,state,type='ASIN_SAME_AS')
            #     elif expression_type == 'ASIN_EXPANDED_FROM':
            #         value = result['expression'][0]['value']
            #         asin = apitool3.select_sp_product_asin(market1, market2, value)
            #         if asin != None:
            #             bid_info = apitool1.list_product_bid_recommendations(market1, asin, new_campaign_id,
            #                                                                  new_adgroup_id)
            #             bid = bid_info['bidRecommendations'][0]['bidRecommendationsForTargetingExpressions'][0]['bidValues'][1]['suggestedBid']
            #             state = result['state']
            #             targetId = create_adGroup_Targeting1(market1, new_campaign_id, new_adgroup_id, asin, bid, state, type='ASIN_EXPANDED_FROM')
            new_product_info = apitool4.get_product_api(market1, new_adgroup_id)
            #product = [result['asin'] for result in new_product_info]
            product = [result['asin'] for result in new_product_info if "asin" in result]
            recommendations = apitool1.list_adGroup_Targetingrecommendations(market1, product)
            print(recommendations)
            for category in recommendations["categories"]:
                categories_id = category["id"]
                brand_info = apitool1.list_category_refinements(market1, categories_id)
                # 检查是否存在名为"LAPASA"的品牌
                target_brand_name = brand_name
                target_brand_id = None
                for brand in brand_info['brands']:
                    if brand['name'] == target_brand_name:
                        target_brand_id = brand['id']
                        bid_info = apitool1.list_category_bid_recommendations(market1, categories_id,new_campaign_id,new_adgroup_id)
                        bid = bid_info['bidRecommendations'][0]['bidRecommendationsForTargetingExpressions'][0]['bidValues'][1][
                            'suggestedBid']
                        targetId = apitool5.create_adGroup_Targeting2(market1, new_campaign_id, new_adgroup_id, bid, categories_id,target_brand_id)
                        break
        else:
            # 循环遍历列表中的每个字典
            api4 = Gen_keyword(brand_name)
            for item in spkeyword_info:
                matchType = item['matchType']
                state = item['state']
                bid1 = item.get('bid')
                if bid1 is not None:  # 检查是否存在有效的bid值
                    bid = exchange_rate * bid1
                    keywordText = item['keywordText']
                    if state != "ENABLED" and state != "PAUSED":
                        continue  # 跳过当前迭代，进入下一次迭代
                    try:
                        new_keyword_id = api4.add_keyword_toadGroup(market1, new_campaign_id, matchType, state, bid, new_adgroup_id, keywordText)
                    except Exception as e:
                        # 处理异常，可以打印异常信息或者进行其他操作
                        print("An error occurred:", e)
                        dbNewTools = DbNewSpTools(brand_name,market1)
                        dbNewTools.add_sp_keyword_toadGroup(market1, None, new_campaign_id, matchType, state, bid, new_adgroup_id,
                                                            keywordText, None, "failed", datetime.now())
                    # 继续执行下面的代码
                    else:
                        # 如果没有发生异常，则执行以下代码
                        # 这里可以放一些正常情况下的逻辑
                        print("Command executed successfully.")

            negativekw_info = apitool1.get_adGroup_negativekw(market2, ad_group_id)
            if negativekw_info == None:
                pass
            else:
                # 循环遍历列表中的每个字典
                for item in negativekw_info:
                    keyword_text = item['keywordText']
                    match_type = item['matchType']
                    state = item['state']
                    if state != "ENABLED" and state != "PAUSED":
                        continue  # 跳过当前迭代，进入下一次迭代
                    try:
                        new_negativeKeywordId = apitool5.add_adGroup_negative_keyword(market1, new_campaign_id, new_adgroup_id,
                                                                             keyword_text, match_type, state)
                    except Exception as e:
                        # 处理异常，可以打印异常信息或者进行其他操作
                        print("An error occurred:", e)
                        newdbtool = DbNewSpTools(brand_name,market1)
                        newdbtool.add_sp_adGroup_negativeKeyword(market1, None, new_adgroup_id, new_campaign_id, None,
                                                                 match_type,
                                                                 state, keyword_text, None,
                                                                 None, "failed", datetime.now())
                    # 继续执行下面的代码
                    else:
                        # 如果没有发生异常，则执行以下代码
                        # 这里可以放一些正常情况下的逻辑
                        print("Command executed successfully.")



        return f"{new_campaign_name} create successfully"


    def create_new_sp_asin_no_template(self,info,budget,target_bid, user='test'):
        exchange_rate = self.get_exchange_rate(self.market, 'DE')
        for i in info:
            name1 = f"DeepBI_0514_{i}_ASIN"
            api1 = DbSpTools(self.db, self.brand, self.market)
            res = api1.select_sp_campaign_name(name1)
            if res[0] == "success":
                continue
            else:
                name = f"DeepBI_0514_{i}_ASIN"
                today = datetime.today()
                # 格式化输出
                startDate = today.strftime('%Y-%m-%d')
                apitool = Gen_campaign(self.db, self.brand, self.market)
                new_campaign_id = apitool.create_camapign(name, startDate, dynamicBidding={"placementBidding":[],"strategy":"LEGACY_FOR_SALES"}, portfolioId=None,
                                                   endDate=None, targetingType='MANUAL', state='ENABLED',
                                                   budgetType='DAILY', budget=float(budget),user=user)
                # new_campaign_id = '507943269116693'
                if new_campaign_id == "":
                    print("No new campaign")
                    continue
                api2 = Gen_adgroup(self.db, self.brand, self.market)
                new_adgroup_id = api2.create_adgroup(new_campaign_id, name, defaultBid=0.25 * exchange_rate, state='ENABLED',user=user)
                # new_adgroup_id = '317410479958041'
                api3 = Gen_product(self.db, self.brand, self.market)
                if self.brand == 'LAPASA':
                    sku_info = api1.select_sd_product_sku(i)
                else:
                    sku_info = api1.select_product_sku_by_parent_asin(i, self.select_depository())
                for sku in sku_info:
                    try:
                        new_sku = api3.create_productsku(new_campaign_id, new_adgroup_id, sku,asin=None, state="ENABLED",user=user)
                    except Exception as e:
                        # 处理异常，可以打印异常信息或者进行其他操作
                        print("An error occurred create_productsku:", e)
                        newdbtool = DbNewSpTools(self.db, self.brand, self.market)
                        newdbtool.create_sp_product(self.market,new_campaign_id,None,sku,new_adgroup_id,None,"failed",datetime.now(),"SP", user)
                apitool1 = AdGroupTools(self.db, self.brand, self.market)
                apitool2 = ProductTools(self.db, self.brand, self.market)
                time.sleep(10)
                new_product_info = apitool2.get_product_api(new_adgroup_id)
                try:
                    product = [result['asin'] for result in new_product_info if "asin" in result]
                except Exception as e:
                    print(e)
                    continue
                recommendations = apitool1.list_adGroup_Targetingrecommendations(product)
                print(recommendations)
                for category in recommendations["categories"]:
                    categories_id = category["id"]
                    brand_info = apitool1.list_category_refinements(categories_id)
                    # 检查是否存在名为"LAPASA"的品牌
                    target_brand_name = self.brand
                    target_brand_id = None

                    bid_info = apitool1.list_category_bid_recommendations(categories_id,
                                                                          new_campaign_id, new_adgroup_id)
                    for brand in brand_info['brands']:
                        if brand['name'] == target_brand_name:
                            target_brand_id = brand['id']

                            try:
                                # 尝试获取bid值
                                bid = bid_info['bidRecommendations'][0]['bidRecommendationsForTargetingExpressions'][0][
                                    'bidValues'][0]['suggestedBid']
                            except (IndexError, KeyError, TypeError):
                                # 如果在尝试获取bid值时发生任何异常（比如索引错误、键错误或类型错误），则设置bid为0.25
                                bid = 0.25 * exchange_rate
                            targetId = api2.create_adGroup_Targeting2(new_campaign_id, new_adgroup_id, float(target_bid),
                                                                 categories_id, target_brand_id,user=user)
                    # try:
                    #                     #     # 尝试获取bid值
                    #                     #     bid2 = bid_info['bidRecommendations'][0]['bidRecommendationsForTargetingExpressions'][0][
                    #                     #         'bidValues'][1]['suggestedBid']
                    #                     # except (IndexError, KeyError, TypeError):
                    #                     #     # 如果在尝试获取bid值时发生任何异常（比如索引错误、键错误或类型错误），则设置bid为0.25
                    #                     #     bid2 = 0.25 * exchange_rate
                    #                     # targetId2 = api2.create_adGroup_Targeting1(market, new_campaign_id, new_adgroup_id, categories_id, bid2,
                    #                     #                                       state='ENABLED', type='ASIN_CATEGORY_SAME_AS')
            print(f"{name} create successfully")
        print("all create successfully")

    def create_new_sp_asin_no_template_1(self,market,brand_name):
        uploaded_file = 'C:/Users/admin/Downloads/0615需新建广告 - 新建广告.csv'
        df = pd.read_csv(uploaded_file)
        # 打印 DataFrame 的前几行，以确保成功读取
        print(df.head())
        # 将DataFrame转换为一个列表，每个元素是一个字典表示一行数据
        df_data = json.loads(df.to_json(orient='records'))
        print(df_data)
        # df_data = [
        #         {'nsspu': 'M68R1', 'sku': 'LPM19SS00680BLK00SR1V2'},
        #     {'nsspu': 'M68R1', 'sku': 'LPM19SS00680BLK0XLR1'},
        #     {'nsspu': 'M68R1', 'sku': 'LPM19SS0068DHGY00MR1'},
        #     {'nsspu': 'M68R1', 'sku': 'LPM19SS0068DHGY00SR1'},
        #     {'nsspu': 'M68R1', 'sku': 'LPM19SS0068HGRY0XLR1'},
        # ]
        df_processed = pd.DataFrame(df_data)
        result = df_processed.groupby('nsspu')['sku'].agg(list)
        print("每个 nsspu 对应的 sku 列表：")
        print(result)
        exchange_rate = self.get_exchange_rate(market, 'DE')
        for nsspu, skus in result.items():
            print(f"nsspu: {nsspu}")
            print(f"skus: {skus}")
            name = f"DeepBI_0615_ASIN_{nsspu}"
            today = datetime.today()
            # 格式化输出
            startDate = today.strftime('%Y-%m-%d')
            apitool = Gen_campaign(brand_name)
            new_campaign_id = apitool.create_camapign(market, name, startDate, dynamicBidding={"placementBidding": [],
                                                                                               "strategy": "LEGACY_FOR_SALES"},
                                                      portfolioId=None,
                                                      endDate=None, targetingType='MANUAL', state='PAUSED',
                                                      budgetType='DAILY', budget=3 * exchange_rate)
            # new_campaign_id = '350300198986995'
            if new_campaign_id == "":
                print("No new campaign")
                continue
            api2 = Gen_adgroup(brand_name)
            new_adgroup_id = api2.create_adgroup(market, new_campaign_id, name, defaultBid=0.25 * exchange_rate,
                                            state='ENABLED')
            # new_adgroup_id = '419956427382148'
            # 在这里可以添加你的进一步处理或操作，例如：
            api3 = Gen_product(brand_name)
            for sku in skus:
                try:
                    new_sku = api3.create_productsku(market, new_campaign_id, new_adgroup_id, sku, asin=None,
                                                state="ENABLED")
                except Exception as e:
                    # 处理异常，可以打印异常信息或者进行其他操作
                    print("An error occurred create_productsku:", e)
                    newdbtool = DbNewSpTools(brand_name,market)
                    newdbtool.create_sp_product(market, new_campaign_id, None, sku, new_adgroup_id, None, "failed",
                                                datetime.now(), "SP")
            apitool1 = AdGroupTools(brand_name)
            apitool2 = ProductTools(brand_name)
            time.sleep(10)
            new_product_info = apitool2.get_product_api(market, new_adgroup_id)
            product = [result['asin'] for result in new_product_info if "asin" in result]
            recommendations = apitool1.list_adGroup_Targetingrecommendations(market, product)
            print(recommendations)
            for category in recommendations["categories"]:
                categories_id = category["id"]
                brand_info = apitool1.list_category_refinements(market, categories_id)
                # 检查是否存在名为"LAPASA"的品牌
                target_brand_name = 'LAPASA'
                target_brand_id = None
                bid_info = apitool1.list_category_bid_recommendations(market, categories_id,
                                                                      new_campaign_id, new_adgroup_id)
                for brand in brand_info['brands']:
                    if brand['name'] == target_brand_name:
                        target_brand_id = brand['id']
                        try:
                            # 尝试获取bid值
                            bid1 = bid_info['bidRecommendations'][0]['bidRecommendationsForTargetingExpressions'][0][
                                'bidValues'][0]['suggestedBid']
                        except (IndexError, KeyError, TypeError):
                            # 如果在尝试获取bid值时发生任何异常（比如索引错误、键错误或类型错误），则设置bid为0.25
                            bid1 = 0.25 * exchange_rate

                        targetId1 = api2.create_adGroup_Targeting2(market, new_campaign_id, new_adgroup_id, bid1,
                                                             categories_id, target_brand_id)

                try:
                    # 尝试获取bid值
                    bid2 = bid_info['bidRecommendations'][0]['bidRecommendationsForTargetingExpressions'][0][
                        'bidValues'][1]['suggestedBid']
                except (IndexError, KeyError, TypeError):
                    # 如果在尝试获取bid值时发生任何异常（比如索引错误、键错误或类型错误），则设置bid为0.25
                    bid2 = 0.25 * exchange_rate
                targetId2 = api2.create_adGroup_Targeting1(new_campaign_id, new_adgroup_id, categories_id, bid2,
                                                      state='ENABLED', type='ASIN_CATEGORY_SAME_AS')
            print("------------------------")
            print(f"{name} create successfully")
        print("all create successfully")

    def create_new_sp_auto_no_template(self,info,budget,target_bid,user='test'):
        exchange_rate = self.get_exchange_rate(self.market, 'DE')
        for i in info:
            name1 = f"DeepBI_0502_{i}_AUTO"
            api1 = DbSpTools(self.db, self.brand, self.market)
            res = api1.select_sp_campaign_name(name1)
            if res[0] == "success":
                continue
            else:
                name = f"DeepBI_0502_{i}_AUTO"
                today = datetime.today()
                # 格式化输出
                startDate = today.strftime('%Y-%m-%d')
                apitool = Gen_campaign(self.db, self.brand, self.market)
                new_campaign_id = apitool.create_camapign(name, startDate, dynamicBidding={"placementBidding":[],"strategy":"LEGACY_FOR_SALES"}, portfolioId=None,
                                                   endDate=None, targetingType='AUTO', state='PAUSED',
                                                   budgetType='DAILY', budget=float(budget),user=user)
                if new_campaign_id == "":
                    print("No new campaign")
                    continue
                    # new_campaign_id = '310928261900083'
                api2 = Gen_adgroup(self.db, self.brand, self.market)
                new_adgroup_id = api2.create_adgroup(new_campaign_id, name, defaultBid=0.25 * exchange_rate, state='ENABLED',user=user)
                # new_adgroup_id = '491456703765912'
                api3 = Gen_product(self.db, self.brand, self.market)
                if self.brand == 'LAPASA':
                    sku_info = api1.select_sd_product_sku(i)
                else:
                    sku_info = api1.select_product_sku_by_parent_asin(i, self.select_depository())
                for sku in sku_info:
                    try:
                        new_sku = api3.create_productsku(new_campaign_id, new_adgroup_id, sku,asin=None, state="ENABLED",user=user)
                    except Exception as e:
                        # 处理异常，可以打印异常信息或者进行其他操作
                        print("An error occurred create_productsku:", e)
                        newdbtool = DbNewSpTools(self.db, self.brand, self.market)
                        newdbtool.create_sp_product(new_campaign_id,None,sku,new_adgroup_id,None,"failed",datetime.now(),"SP",user)
                apitool1 = AdGroupTools(self.db, self.brand, self.market)
                market_TargetingClause = apitool1.list_adGroup_TargetingClause(new_adgroup_id)
                # TargetingClause_bid = apitool1.list_automatic_targeting_bid_recommendations(market, new_campaign_id,
                #                                                                             new_adgroup_id)
                expressions = [{'type': expr['type'], 'targetId': item['targetId']} for item in market_TargetingClause
                               for expr in item['expression']]
                # expressions_bid = [
                #     {
                #         'type': rec['targetingExpression']['type'],
                #         'secondSuggestedBid': rec['bidValues'][1]['suggestedBid']
                #     }
                #     for recommendation in TargetingClause_bid['bidRecommendations']
                #     for rec in recommendation['bidRecommendationsForTargetingExpressions']
                # ]
                # bid_map = {bid['type']: bid['secondSuggestedBid'] for bid in expressions_bid}
                #
                # # Update expressions with corresponding bids
                # for expr in expressions:
                #     if expr['type'] == 'QUERY_HIGH_REL_MATCHES':
                #         expr['bid'] = bid_map.get('CLOSE_MATCH')
                #     elif expr['type'] == 'QUERY_BROAD_REL_MATCHES':
                #         expr['bid'] = bid_map.get('LOOSE_MATCH')
                #     elif expr['type'] == 'ASIN_ACCESSORY_RELATED':
                #         expr['bid'] = bid_map.get('COMPLEMENTS')
                #     elif expr['type'] == 'ASIN_SUBSTITUTE_RELATED':
                #         expr['bid'] = bid_map.get('SUBSTITUTES')

                print(expressions)
                for item in expressions:
                    target_id = item['targetId']
                    bid = item.get('bid')
                    print(target_id)
                    print(bid)
                    if bid is not None:
                        targetId = api2.update_adGroup_TargetingClause(target_id, float(target_bid), 'ENABLED',user=user)
                    else:
                        targetId = api2.update_adGroup_TargetingClause(target_id, float(target_bid), 'ENABLED',user=user)

            print(f"{name} create successfully")
        print("all create successfully")

    def create_new_sp_auto_no_template_jiutong(self,info,budget):
        print('jiutong')
        exchange_rate = self.get_exchange_rate(self.market, 'DE')
        for i in info:
            name1 = f"DeepBI_0502_{i}_AUTO"
            api1 = DbSpTools(self.db, self.brand, self.market)
            res = api1.select_sp_campaign_name(name1)
            if res[0] == "success":
                continue
            else:
                name = f"DeepBI_0502_{i}_AUTO"
                today = datetime.today()
                # 格式化输出
                startDate = today.strftime('%Y-%m-%d')
                apitool = Gen_campaign(self.db, self.brand, self.market)
                new_campaign_id = apitool.create_camapign(name, startDate, dynamicBidding={"placementBidding":[],"strategy":"LEGACY_FOR_SALES"}, portfolioId=None,
                                                   endDate=None, targetingType='AUTO', state='ENABLED',
                                                   budgetType='DAILY', budget=float(budget))
                if new_campaign_id == "":
                    print("No new campaign")
                    continue
                # new_campaign_id = '449345435691647'
                api2 = Gen_adgroup(self.db, self.brand, self.market)
                new_adgroup_id = api2.create_adgroup(new_campaign_id, name, defaultBid=0.25 * exchange_rate, state='ENABLED')
                # new_adgroup_id = '361896893484449'
                api3 = Gen_product(self.db, self.brand, self.market)
                if self.brand == 'LAPASA':
                    sku_info = api1.select_sd_product_sku(i)
                else:
                    sku_info = api1.select_product_sku_by_parent_asin(i, self.select_depository())
                for sku in sku_info:
                    try:
                        new_sku = api3.create_productsku(new_campaign_id, new_adgroup_id, sku,asin=None, state="ENABLED")
                    except Exception as e:
                        # 处理异常，可以打印异常信息或者进行其他操作
                        print("An error occurred create_productsku:", e)
                        newdbtool = DbNewSpTools(self.db, self.brand, self.market)
                        newdbtool.create_sp_product(new_campaign_id,None,sku,new_adgroup_id,None,"failed",datetime.now(),"SP")
                time.sleep(10)
                apitool1 = AdGroupTools(self.db, self.brand, self.market)
                market_TargetingClause = apitool1.list_adGroup_TargetingClause(new_adgroup_id)
                TargetingClause_bid = apitool1.list_automatic_targeting_bid_recommendations(new_campaign_id,new_adgroup_id)
                expressions = [{'type': expr['type'], 'targetId': item['targetId']} for item in market_TargetingClause for expr in item['expression']]
                expressions_bid = [
                    {
                        'type': rec['targetingExpression']['type'],
                        'secondSuggestedBid': rec['bidValues'][1]['suggestedBid']
                    }
                    for recommendation in TargetingClause_bid['bidRecommendations']
                    for rec in recommendation['bidRecommendationsForTargetingExpressions']
                ]
                bid_map = {bid['type']: bid['secondSuggestedBid'] for bid in expressions_bid}

                # Update expressions with corresponding bids
                for expr in expressions:
                    if expr['type'] == 'QUERY_HIGH_REL_MATCHES':
                        expr['bid'] = bid_map.get('CLOSE_MATCH')
                    elif expr['type'] == 'QUERY_BROAD_REL_MATCHES':
                        expr['bid'] = bid_map.get('LOOSE_MATCH')
                    elif expr['type'] == 'ASIN_ACCESSORY_RELATED':
                        expr['bid'] = bid_map.get('COMPLEMENTS')
                    elif expr['type'] == 'ASIN_SUBSTITUTE_RELATED':
                        expr['bid'] = bid_map.get('SUBSTITUTES')

                print(expressions)
                for item in expressions:
                    target_id = item['targetId']
                    bid = item['bid']
                    print(target_id)
                    print(bid)
                    if bid is not None:
                        targetId = api2.update_adGroup_TargetingClause(target_id, 0.75*exchange_rate if bid > 0.75*exchange_rate else bid, 'ENABLED')
                    else:
                        targetId = api2.update_adGroup_TargetingClause(target_id, 0.75*exchange_rate, 'ENABLED')


            print(f"{name} create successfully")
        print("all create successfully")

    def create_new_sp_manual_no_template(self,info,budget):
        exchange_rate = self.get_exchange_rate(self.market, 'DE')
        for i in info:
            name1 = f"DeepBI_0502_{i}_MANUAL"
            api1 = DbSpTools(self.db, self.brand, self.market)
            res = api1.select_sp_campaign_name(name1)
            if res[0] == "success":
                continue
            if self.brand == 'LAPASA':
                search_term_info = api1.select_sp_campaign_search_term(i)
            else:
                search_term_info = api1.select_sp_campaign_search_term_by_parent_asin(i,self.select_depository())
            if not search_term_info:
                continue
            name = f"DeepBI_0502_{i}_MANUAL"
            today = datetime.today()
            # 格式化输出
            startDate = today.strftime('%Y-%m-%d')
            apitool = Gen_campaign(self.db, self.brand, self.market)
            new_campaign_id = apitool.create_camapign(name, startDate, dynamicBidding={"placementBidding":[],"strategy":"LEGACY_FOR_SALES"}, portfolioId=None,
                                               endDate=None, targetingType='MANUAL', state='PAUSED',
                                               budgetType='DAILY', budget=float(budget))
            #new_campaign_id = '297477921455980'
            if new_campaign_id == "":
                print("No new campaign")
                continue
            api2 = Gen_adgroup(self.db, self.brand, self.market)
            new_adgroup_id = api2.create_adgroup(new_campaign_id, name, defaultBid=0.25 * exchange_rate, state='ENABLED')
            #new_adgroup_id = '491456703765912'
            api3 = Gen_product(self.db, self.brand, self.market)
            if self.brand == 'LAPASA':
                sku_info = api1.select_sd_product_sku(i)
            else:
                sku_info = api1.select_product_sku_by_parent_asin(i,self.select_depository())
            for sku in sku_info:
                try:
                    new_sku = api3.create_productsku(new_campaign_id, new_adgroup_id, sku,asin=None, state="ENABLED")
                except Exception as e:
                    # 处理异常，可以打印异常信息或者进行其他操作
                    print("An error occurred create_productsku:", e)
                    newdbtool = DbNewSpTools(self.db, self.brand, self.market)
                    newdbtool.create_sp_product(new_campaign_id,None,sku,new_adgroup_id,None,"failed",datetime.now(),"SP")

            apitool2 = SPKeywordTools(self.db, self.brand, self.market)
            api4 = Gen_keyword(self.db, self.brand, self.market)
            # 添加亚马逊推荐的关键词，暂时不用
            # spkeyword_info = apitool2.get_spkeyword_recommendations_api(market, new_campaign_id, new_adgroup_id)
            # for item in spkeyword_info:
            #     matchType = item['matchType']
            #     state = "ENABLED"
            #     bid1 = item.get('bid')
            #     if bid1 is not None:  # 检查是否存在有效的bid值
            #         bid = exchange_rate * bid1
            #         keywordText = item['keyword']
            #         if state != "ENABLED" and state != "PAUSED":
            #             continue  # 跳过当前迭代，进入下一次迭代
            #         try:
            #             new_keyword_id = api4.add_keyword_toadGroup_v0(market, new_campaign_id, new_adgroup_id, keywordText, matchType, state, bid)
            #         except Exception as e:
            #             # 处理异常，可以打印异常信息或者进行其他操作
            #             print("An error occurred:", e)
            #             dbNewTools = DbNewSpTools(brand_name)
            #             dbNewTools.add_sp_keyword_toadGroup(market, None, new_campaign_id, matchType, state, bid,
            #                                                 new_adgroup_id,
            #                                                 keywordText, None, "failed", datetime.now())
            #         # 继续执行下面的代码
            #         else:
            #             # 如果没有发生异常，则执行以下代码
            #             # 这里可以放一些正常情况下的逻辑
            #             print("Command executed successfully.")
            for item in search_term_info:
                try:
                    new_keyword_id = api4.add_keyword_toadGroup_v0(new_campaign_id, new_adgroup_id, item, matchType="EXACT", state="ENABLED", bid=0.5 * exchange_rate)
                    new_keyword_id = api4.add_keyword_toadGroup_v0(new_campaign_id, new_adgroup_id, item, matchType="PHRASE", state="ENABLED", bid=0.5 * exchange_rate)
                    new_keyword_id = api4.add_keyword_toadGroup_v0(new_campaign_id, new_adgroup_id, item, matchType="BROAD", state="ENABLED", bid=0.5 * exchange_rate)
                except Exception as e:
                    # 处理异常，可以打印异常信息或者进行其他操作
                    print("An error occurred:", e)
                    dbNewTools = DbNewSpTools(self.db, self.brand, self.market)
            print(f"{name} create successfully")
        print("all create successfully")

    def create_new_sp_manual_no_template_jiutong(self,uploaded_file,budget):
        exchange_rate = self.get_exchange_rate(self.market, 'DE')
        df = pd.read_csv(uploaded_file)
        # 打印 DataFrame 的前几行，以确保成功读取
        # 按照 parent_asins 列进行分组
        grouped = df.groupby('parent_asins')

        # 创建一个字典来存储每个 parent_asins 对应的列表
        result = {}

        # 遍历每个分组
        for name, group in grouped:
            # 将每个分组的 DataFrame 转换为字典的列表
            result[name] = group[['keyword', 'matchType', 'bid']].to_dict(orient='records')

        # 打印结果
        for parent_asin, info_list in result.items():
            print(f"parent_asins: {parent_asin}")
            name1 = f"DeepBI_0502_{parent_asin}_MANUAL"
            api1 = DbSpTools(self.db, self.brand, self.market)
            res = api1.select_sp_campaign_name(name1)
            if res[0] == "success":
                continue
            name = f"DeepBI_0502_{parent_asin}_MANUAL"
            today = datetime.today()
            # 格式化输出
            startDate = today.strftime('%Y-%m-%d')
            apitool = Gen_campaign(self.db, self.brand, self.market)
            new_campaign_id = apitool.create_camapign(name, startDate, dynamicBidding={"placementBidding":[],"strategy":"LEGACY_FOR_SALES"}, portfolioId=None,
                                               endDate=None, targetingType='MANUAL', state='ENABLED',
                                               budgetType='DAILY', budget=float(budget))
            #new_campaign_id = '297477921455980'
            if new_campaign_id == "":
                print("No new campaign")
                continue
            api2 = Gen_adgroup(self.db, self.brand, self.market)
            new_adgroup_id = api2.create_adgroup(new_campaign_id, name, defaultBid=0.25 * exchange_rate, state='ENABLED')
            #new_adgroup_id = '491456703765912'
            api3 = Gen_product(self.db, self.brand, self.market)
            if self.brand == 'LAPASA':
                sku_info = api1.select_sd_product_sku(parent_asin)
            else:
                sku_info = api1.select_product_sku_by_parent_asin(parent_asin,self.select_depository())
            for sku in sku_info:
                try:
                    new_sku = api3.create_productsku(new_campaign_id, new_adgroup_id, sku,asin=None, state="ENABLED")
                except Exception as e:
                    # 处理异常，可以打印异常信息或者进行其他操作
                    print("An error occurred create_productsku:", e)
                    newdbtool = DbNewSpTools(self.db, self.brand, self.market)
                    newdbtool.create_sp_product(new_campaign_id,None,sku,new_adgroup_id,None,"failed",datetime.now(),"SP","SP")

            apitool2 = SPKeywordTools(self.db, self.brand, self.market)
            api4 = Gen_keyword(self.db, self.brand, self.market)
            # 添加亚马逊推荐的关键词，暂时不用
            # spkeyword_info = apitool2.get_spkeyword_recommendations_api(market, new_campaign_id, new_adgroup_id)
            # for item in spkeyword_info:
            #     matchType = item['matchType']
            #     state = "ENABLED"
            #     bid1 = item.get('bid')
            #     if bid1 is not None:  # 检查是否存在有效的bid值
            #         bid = exchange_rate * bid1
            #         keywordText = item['keyword']
            #         if state != "ENABLED" and state != "PAUSED":
            #             continue  # 跳过当前迭代，进入下一次迭代
            #         try:
            #             new_keyword_id = api4.add_keyword_toadGroup_v0(market, new_campaign_id, new_adgroup_id, keywordText, matchType, state, bid)
            #         except Exception as e:
            #             # 处理异常，可以打印异常信息或者进行其他操作
            #             print("An error occurred:", e)
            #             dbNewTools = DbNewSpTools(brand_name)
            #             dbNewTools.add_sp_keyword_toadGroup(market, None, new_campaign_id, matchType, state, bid,
            #                                                 new_adgroup_id,
            #                                                 keywordText, None, "failed", datetime.now())
            #         # 继续执行下面的代码
            #         else:
            #             # 如果没有发生异常，则执行以下代码
            #             # 这里可以放一些正常情况下的逻辑
            #             print("Command executed successfully.")
            for info in info_list:
                print(f" keyword: {info['keyword']}, matchType: {info['matchType']}, bid: {info['bid']}")
                try:
                    new_keyword_id = api4.add_keyword_toadGroup_v0(new_campaign_id, new_adgroup_id, info['keyword'], matchType=info['matchType'], state="ENABLED", bid=float(info['bid']))
                except Exception as e:
                    # 处理异常，可以打印异常信息或者进行其他操作
                    print("An error occurred:", e)
            print(f"{name} create successfully")
        print("all create successfully")

    def create_new_sp_manual_no_template_jiutong_api(self,keyword_info,budget,user='test'):
        exchange_rate = self.get_exchange_rate(self.market, 'DE')

        # 打印结果
        for parent_asin, info_list in keyword_info.items():
            print(f"parent_asins: {parent_asin}")
            name1 = f"DeepBI_0502_{parent_asin}_MANUAL"
            api1 = DbSpTools(self.db, self.brand, self.market)
            res = api1.select_sp_campaign_name(name1)
            if res[0] == "success":
                continue
            name = f"DeepBI_0502_{parent_asin}_MANUAL"
            today = datetime.today()
            # 格式化输出
            startDate = today.strftime('%Y-%m-%d')
            apitool = Gen_campaign(self.db, self.brand, self.market)
            new_campaign_id = apitool.create_camapign(name, startDate, dynamicBidding={"placementBidding":[],"strategy":"LEGACY_FOR_SALES"}, portfolioId=None,
                                               endDate=None, targetingType='MANUAL', state='ENABLED',
                                               budgetType='DAILY', budget=float(budget),user=user)
            #new_campaign_id = '297477921455980'
            if new_campaign_id == "":
                print("No new campaign")
                continue
            api2 = Gen_adgroup(self.db, self.brand, self.market)
            new_adgroup_id = api2.create_adgroup(new_campaign_id, name, defaultBid=0.25 * exchange_rate, state='ENABLED',user=user)
            #new_adgroup_id = '491456703765912'
            api3 = Gen_product(self.db, self.brand, self.market)
            if self.brand == 'LAPASA':
                sku_info = api1.select_sd_product_sku(parent_asin)
            else:
                sku_info = api1.select_product_sku_by_parent_asin(parent_asin,self.select_depository())
            for sku in sku_info:
                try:
                    new_sku = api3.create_productsku(new_campaign_id, new_adgroup_id, sku,asin=None, state="ENABLED",user=user)
                except Exception as e:
                    # 处理异常，可以打印异常信息或者进行其他操作
                    print("An error occurred create_productsku:", e)
                    newdbtool = DbNewSpTools(self.db, self.brand, self.market)
                    newdbtool.create_sp_product(self.market,new_campaign_id,None,sku,new_adgroup_id,None,"failed",datetime.now(),"SP",user)

            apitool2 = SPKeywordTools(self.db, self.brand, self.market)
            api4 = Gen_keyword(self.db, self.brand, self.market)
            # 添加亚马逊推荐的关键词，暂时不用
            # spkeyword_info = apitool2.get_spkeyword_recommendations_api(market, new_campaign_id, new_adgroup_id)
            # for item in spkeyword_info:
            #     matchType = item['matchType']
            #     state = "ENABLED"
            #     bid1 = item.get('bid')
            #     if bid1 is not None:  # 检查是否存在有效的bid值
            #         bid = exchange_rate * bid1
            #         keywordText = item['keyword']
            #         if state != "ENABLED" and state != "PAUSED":
            #             continue  # 跳过当前迭代，进入下一次迭代
            #         try:
            #             new_keyword_id = api4.add_keyword_toadGroup_v0(market, new_campaign_id, new_adgroup_id, keywordText, matchType, state, bid)
            #         except Exception as e:
            #             # 处理异常，可以打印异常信息或者进行其他操作
            #             print("An error occurred:", e)
            #             dbNewTools = DbNewSpTools(brand_name)
            #             dbNewTools.add_sp_keyword_toadGroup(market, None, new_campaign_id, matchType, state, bid,
            #                                                 new_adgroup_id,
            #                                                 keywordText, None, "failed", datetime.now())
            #         # 继续执行下面的代码
            #         else:
            #             # 如果没有发生异常，则执行以下代码
            #             # 这里可以放一些正常情况下的逻辑
            #             print("Command executed successfully.")
            for info in info_list:
                print(f" keyword: {info['keyword']}, matchType: {info['matchType']}, bid: {info['bid']}")
                try:
                    new_keyword_id = api4.add_keyword_toadGroup_v0(new_campaign_id, new_adgroup_id, info['keyword'], matchType=info['matchType'], state="ENABLED", bid=float(info['bid']),user=user)
                except Exception as e:
                    # 处理异常，可以打印异常信息或者进行其他操作
                    print("An error occurred:", e)
            print(f"{name} create successfully")
        print("all create successfully")

    def create_new_sp_manual_no_template_0503_api(self,keyword_info,budget,user='test'):
        exchange_rate = self.get_exchange_rate(self.market, 'DE')

        # 打印结果
        for parent_asin, info_list in keyword_info.items():
            print(f"parent_asins: {parent_asin}")
            name1 = f"DeepBI_0503_{parent_asin}_MANUAL"
            api1 = DbSpTools(self.db, self.brand, self.market)
            res = api1.select_sp_campaign_name(name1)
            if res[0] == "success":
                continue
            name = f"DeepBI_0503_{parent_asin}_MANUAL"
            today = datetime.today()
            # 格式化输出
            startDate = today.strftime('%Y-%m-%d')
            apitool = Gen_campaign(self.db, self.brand, self.market)
            new_campaign_id = apitool.create_camapign(name, startDate, dynamicBidding={"placementBidding":[],"strategy":"LEGACY_FOR_SALES"}, portfolioId=None,
                                               endDate=None, targetingType='MANUAL', state='ENABLED',
                                               budgetType='DAILY', budget=float(budget),user=user)
            #new_campaign_id = '297477921455980'
            if new_campaign_id == "":
                print("No new campaign")
                continue
            api2 = Gen_adgroup(self.db, self.brand, self.market)
            new_adgroup_id = api2.create_adgroup(new_campaign_id, name, defaultBid=0.25 * exchange_rate, state='ENABLED',user=user)
            #new_adgroup_id = '491456703765912'
            api3 = Gen_product(self.db, self.brand, self.market)
            if self.brand == 'LAPASA':
                sku_info = api1.select_sd_product_sku(parent_asin)
            else:
                sku_info = api1.select_product_sku_by_parent_asin(parent_asin,self.select_depository())
            for sku in sku_info:
                try:
                    new_sku = api3.create_productsku(new_campaign_id, new_adgroup_id, sku,asin=None, state="ENABLED",user=user)
                except Exception as e:
                    # 处理异常，可以打印异常信息或者进行其他操作
                    print("An error occurred create_productsku:", e)
                    newdbtool = DbNewSpTools(self.db, self.brand, self.market)
                    newdbtool.create_sp_product(self.market,new_campaign_id,None,sku,new_adgroup_id,None,"failed",datetime.now(),"SP",user)

            apitool2 = SPKeywordTools(self.db, self.brand, self.market)
            api4 = Gen_keyword(self.db, self.brand, self.market)
            # 添加亚马逊推荐的关键词，暂时不用
            # spkeyword_info = apitool2.get_spkeyword_recommendations_api(market, new_campaign_id, new_adgroup_id)
            # for item in spkeyword_info:
            #     matchType = item['matchType']
            #     state = "ENABLED"
            #     bid1 = item.get('bid')
            #     if bid1 is not None:  # 检查是否存在有效的bid值
            #         bid = exchange_rate * bid1
            #         keywordText = item['keyword']
            #         if state != "ENABLED" and state != "PAUSED":
            #             continue  # 跳过当前迭代，进入下一次迭代
            #         try:
            #             new_keyword_id = api4.add_keyword_toadGroup_v0(market, new_campaign_id, new_adgroup_id, keywordText, matchType, state, bid)
            #         except Exception as e:
            #             # 处理异常，可以打印异常信息或者进行其他操作
            #             print("An error occurred:", e)
            #             dbNewTools = DbNewSpTools(brand_name)
            #             dbNewTools.add_sp_keyword_toadGroup(market, None, new_campaign_id, matchType, state, bid,
            #                                                 new_adgroup_id,
            #                                                 keywordText, None, "failed", datetime.now())
            #         # 继续执行下面的代码
            #         else:
            #             # 如果没有发生异常，则执行以下代码
            #             # 这里可以放一些正常情况下的逻辑
            #             print("Command executed successfully.")
            for info in info_list:
                print(f" keyword: {info['keyword']}, matchType: {info['matchType']}, bid: {info['bid']}")
                try:
                    new_keyword_id = api4.add_keyword_toadGroup_v0(new_campaign_id, new_adgroup_id, info['keyword'], matchType=info['matchType'], state="ENABLED", bid=float(info['bid']),user=user)
                except Exception as e:
                    # 处理异常，可以打印异常信息或者进行其他操作
                    print("An error occurred:", e)
            print(f"{name} create successfully")
        print("all create successfully")

    def create_new_sp_asin_no_template_youniverse(self,info,budget,target_bid,brand_youniverse):
        exchange_rate = self.get_exchange_rate(self.market, 'DE')
        for i in info:
            name1 = f"DeepBI_0514_{i}_ASIN"
            api1 = DbSpTools(self.db, self.brand, self.market)
            res = api1.select_sp_campaign_name(name1)
            if res[0] == "success":
                continue
            else:
                name = f"DeepBI_0514_{i}_ASIN"
                today = datetime.today()
                # 格式化输出
                startDate = today.strftime('%Y-%m-%d')
                apitool = Gen_campaign(self.db, self.brand, self.market)
                new_campaign_id = apitool.create_camapign(name, startDate, dynamicBidding={"placementBidding":[],"strategy":"LEGACY_FOR_SALES"}, portfolioId=None,
                                                   endDate=None, targetingType='MANUAL', state='ENABLED',
                                                   budgetType='DAILY', budget=float(budget))
                # new_campaign_id = '507943269116693'
                if new_campaign_id == "":
                    print("No new campaign")
                    continue
                api2 = Gen_adgroup(self.db, self.brand, self.market)
                new_adgroup_id = api2.create_adgroup(new_campaign_id, name, defaultBid=0.25 * exchange_rate, state='ENABLED')
                # new_adgroup_id = '317410479958041'
                api3 = Gen_product(self.db, self.brand, self.market)
                if self.brand == 'LAPASA':
                    sku_info = api1.select_sd_product_sku(i)
                else:
                    sku_info = api1.select_product_sku_by_parent_asin(i, self.select_depository())
                for sku in sku_info:
                    try:
                        new_sku = api3.create_productsku(new_campaign_id, new_adgroup_id, sku,asin=None, state="ENABLED")
                    except Exception as e:
                        # 处理异常，可以打印异常信息或者进行其他操作
                        print("An error occurred create_productsku:", e)
                        newdbtool = DbNewSpTools(self.db, self.brand, self.market)
                        newdbtool.create_sp_product(self.market,new_campaign_id,None,sku,new_adgroup_id,None,"failed",datetime.now(),"SP")
                apitool1 = AdGroupTools(self.db, self.brand, self.market)
                apitool2 = ProductTools(self.db, self.brand, self.market)
                time.sleep(10)
                new_product_info = apitool2.get_product_api(new_adgroup_id)
                try:
                    product = [result['asin'] for result in new_product_info if "asin" in result]
                except Exception as e:
                    print(e)
                    continue
                recommendations = apitool1.list_adGroup_Targetingrecommendations(product)
                print(recommendations)
                for category in recommendations["categories"]:
                    categories_id = category["id"]
                    brand_info = apitool1.list_category_refinements(categories_id)
                    # 检查是否存在名为"LAPASA"的品牌
                    target_brand_name = brand_youniverse
                    target_brand_id = None

                    bid_info = apitool1.list_category_bid_recommendations(categories_id,
                                                                          new_campaign_id, new_adgroup_id)
                    for brand in brand_info['brands']:
                        if brand['name'] == target_brand_name:
                            target_brand_id = brand['id']

                            try:
                                # 尝试获取bid值
                                bid = bid_info['bidRecommendations'][0]['bidRecommendationsForTargetingExpressions'][0][
                                    'bidValues'][0]['suggestedBid']
                            except (IndexError, KeyError, TypeError):
                                # 如果在尝试获取bid值时发生任何异常（比如索引错误、键错误或类型错误），则设置bid为0.25
                                bid = 0.25 * exchange_rate
                            targetId = api2.create_adGroup_Targeting2(new_campaign_id, new_adgroup_id, float(target_bid),
                                                                 categories_id, target_brand_id)
                    # try:
                    #                     #     # 尝试获取bid值
                    #                     #     bid2 = bid_info['bidRecommendations'][0]['bidRecommendationsForTargetingExpressions'][0][
                    #                     #         'bidValues'][1]['suggestedBid']
                    #                     # except (IndexError, KeyError, TypeError):
                    #                     #     # 如果在尝试获取bid值时发生任何异常（比如索引错误、键错误或类型错误），则设置bid为0.25
                    #                     #     bid2 = 0.25 * exchange_rate
                    #                     # targetId2 = api2.create_adGroup_Targeting1(market, new_campaign_id, new_adgroup_id, categories_id, bid2,
                    #                     #                                       state='ENABLED', type='ASIN_CATEGORY_SAME_AS')
            print(f"{name} create successfully")
        print("all create successfully")

    def create_new_sp_no_template_error(self, market, brand_name,target_bid,new_campaign_id,new_adgroup_id,brand_w):
        exchange_rate = self.get_exchange_rate(market, 'DE')
        apitool1 = AdGroupTools(brand_name)
        apitool2 = ProductTools(brand_name)
        api2 = Gen_adgroup(brand_name)
        time.sleep(10)
        new_product_info = apitool2.get_product_api(market, new_adgroup_id)
        try:
            product = [result['asin'] for result in new_product_info if "asin" in result]
        except Exception as e:
            print(e)
            pass
        recommendations = apitool1.list_adGroup_Targetingrecommendations(market, product)
        print(recommendations)
        for category in recommendations["categories"]:
            categories_id = category["id"]
            brand_info = apitool1.list_category_refinements(market, categories_id)
            # 检查是否存在名为"LAPASA"的品牌
            target_brand_name = brand_w
            target_brand_id = None

            bid_info = apitool1.list_category_bid_recommendations(market, categories_id,
                                                                  new_campaign_id, new_adgroup_id)
            for brand in brand_info['brands']:
                if brand['name'] == target_brand_name:
                    target_brand_id = brand['id']

                    try:
                        # 尝试获取bid值
                        bid = bid_info['bidRecommendations'][0]['bidRecommendationsForTargetingExpressions'][0][
                            'bidValues'][0]['suggestedBid']
                    except (IndexError, KeyError, TypeError):
                        # 如果在尝试获取bid值时发生任何异常（比如索引错误、键错误或类型错误），则设置bid为0.25
                        bid = 0.25 * exchange_rate
                    targetId = api2.create_adGroup_Targeting2(market, new_campaign_id, new_adgroup_id, float(target_bid),
                                                         categories_id, target_brand_id)
# #创建 Ceate_new_sku 类的实例
# ceate_new_sku_instance = Ceate_new_sku()
if __name__ == "__main__":
# 调用 create_new_sku() 方法
    ceate_new_sku_instance = Ceate_new_sku()
    res = ceate_new_sku_instance.create_new_sp_auto_no_template('US',['B08VGL3HV5'],'Rossny',5,10)#OutdoorMaster DELOMO
    print(res)
#ceate_new_sku_instance.create_new_sp_asin_no_template('IT',['B09JHWJZQN','B0D89Y6FPV'],brand_name='DELOMO')
#ceate_new_sku_instance.create_new_sp_asin_no_template_2('IT',['M07','M08'],brand_name='LAPASA')
#ceate_new_sku_instance.create_new_sp_asin_no_template_1('FR')
#ceate_new_sku_instance.create_new_sp_auto_no_template('IT',['B09JHWJZQN', 'B0D89Y6FPV'],brand_name='DELOMO')
#ceate_new_sku_instance.create_new_sp_auto_no_template1('UK',['L17', 'G19', 'G11', 'L44', 'L41', 'L17', 'M11', 'M24', 'M57'],brand_name='LAPASA')
#ceate_new_sku_instance.create_new_sp_manual_no_template('DE',['B14','L100','L103','L59','L98'])
#ES DeepBI_0502_M19 manu 2 复制 1 需要sku
# Ceate_new_sku().create_new_sp_auto_no_template_jiutong('UK',['L01'],'Veement',None)


