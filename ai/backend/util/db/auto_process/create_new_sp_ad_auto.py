import pandas as pd
import json
import time
from tools_sp_campaign import CampaignTools
from gen_sp_campaign import Gen_campaign
from datetime import datetime
from tools_sp_adGroup import AdGroupTools
from gen_sp_adgroup import create_adgroup, add_adGroup_negative_keyword, update_adGroup_TargetingClause,create_adGroup_Targeting1,create_adGroup_Targeting2
from tools_sp_product import ProductTools
from gen_sp_product import create_productsku
from tools_sp_keyword import SPKeywordTools
from gen_sp_keyword import add_keyword_toadGroup
from tools_db_new_sp import DbNewSpTools
from tools_db_sp import DbSpTools

class Ceate_new_sku:

    def __init__(self):
        self.config = self.load_config()

    def load_config(self):
        exchange_rate_path = './exchange_rate.json'
        with open(exchange_rate_path) as f:
            return json.load(f)

    def get_exchange_rate(self, market1, market2):
        return self.config['exchange_rate'].get(market2, {}).get(market1)

    def create_new_sku(self,market1,market2,brand_name):
        # uploaded_file = 'C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/db_amazon/SE_DE_2024-06-01_2024-06-08_sp_sku_new.csv'
        # df = pd.read_csv(uploaded_file)
        # # 打印 DataFrame 的前几行，以确保成功读取
        # print(df.head())
        # # 将DataFrame转换为一个列表，每个元素是一个字典表示一行数据
        # df_data = json.loads(df.to_json(orient='records'))
        df_data={
  "data": [
    {
      "market": "FR",
      "sum( sales )": 60.62,
      "sum( cost )": 0.38,
      "avg_acos": 0.006269,
      "campaignId": 40092462708290,
      "campaignName": "DeepBI_0509_m19",
      "adGroupId": 276782127037302,
      "adGroupName": "DeepBI_0509_m19",
      "advertisedSku": "LPM17AW00290BLK0XLR1New",
      "new_campaignName": "DeepBI_0502_M29 M30 M31-Auto",
      "new_adGroupName": "DeepBI_0502_M29 M30 M31-Auto"
    }
  ]
}
        exchange_rate = self.get_exchange_rate(market1, market2)
        processed_ids = {'campaign_ids': set(), 'ad_group_ids': set()}
        # 循环处理每一行数据
        for item in df_data["data"]:
            campaign_id = item["campaignId"]
            ad_group_id = item["adGroupId"]
            advertised_sku = item["advertisedSku"]
            new_campaign_name = item["new_campaignName"]
            new_ad_group_name = item["new_adGroupName"]

            if campaign_id not in processed_ids['campaign_ids']:
                # 如果是第一次遇到，执行相应操作
                # 将当前的 campaign_id 添加到已处理的字典中
                processed_ids['campaign_ids'].add(campaign_id)
                processed_ids['ad_group_ids'].add(ad_group_id)
                # 执行创建
                apitool = Gen_campaign()
                res = apitool.list_camapign(campaign_id, market2)
                for item in res:
                    if isinstance(item, dict):
                        if item.get('targetingType') == 'AUTO':
                            answer_message = self.auto_targeting(res, market1, market2, new_campaign_name, ad_group_id, new_ad_group_name, exchange_rate,advertised_sku)
                            print(answer_message)
                            continue
                        else:
                            answer_message = self.manual_targeting(res, market1, market2, new_campaign_name, ad_group_id, new_ad_group_name, exchange_rate,advertised_sku,brand_name)
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



    def auto_targeting(self, res, market1,market2, new_campaign_name, ad_group_id, new_ad_group_name, exchange_rate,advertisedSku):
        dynamicBidding = res[0]['dynamicBidding']
        # 获取当前日期
        today = datetime.today()
        # 格式化输出
        startDate = today.strftime('%Y-%m-%d')
        # name = 'DeepBI_AUTO_test'
        name = new_campaign_name
        apitool = Gen_campaign()
        #new_campaign_id = apitool.create_camapign(market1, name, startDate, dynamicBidding, portfolioId=None, endDate=None, targetingType='AUTO', state='PAUSED', budgetType='DAILY', budget=5*exchange_rate)
        new_campaign_id ='333199332378491'
        if new_campaign_id == "":
            return "No new campaign"
        apitool1 = AdGroupTools()
        #defaultBid_old = exchange_rate * apitool1.get_adGroup_api(market2, ad_group_id)
        new_adgroup_id = 437018151615304
        #new_adgroup_id = create_adgroup(market1, new_campaign_id, new_ad_group_name, defaultBid_old, state='PAUSED')
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
                    targetId = update_adGroup_TargetingClause(market1,target_id, bid, state)

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
                    new_negativeKeywordId= add_adGroup_negative_keyword(market1, new_campaign_id,new_adgroup_id,keyword_text,match_type,state)
                except Exception as e:
                # 处理异常，可以打印异常信息或者进行其他操作
                    print("An error occurred:", e)
                    newdbtool = DbNewSpTools()
                    newdbtool.add_sp_adGroup_negativeKeyword(market1, None, new_adgroup_id, new_campaign_id, None,
                                                             match_type,
                                                             state, keyword_text, None,
                                                             None, "failed", datetime.now())
                # 继续执行下面的代码

                else:
                # 如果没有发生异常，则执行以下代码
                # 这里可以放一些正常情况下的逻辑
                    print("Command executed successfully.")
        # # TODO 变体的添加，暂时只能将模板的sku全部照搬过来
        # apitool2 = ProductTools()
        # product_info = apitool2.get_product_api(market2,ad_group_id)
        # if product_info == None:
        #     pass
        # else:
        #     # 循环遍历列表中的每个字典
        #     for item in product_info:
        #         sku = item['sku']
        #         new_sku = create_productsku(market1, new_campaign_id,new_adgroup_id,sku,asin=None,state='ENABLED')

        apitool3 = DbSpTools()
        apitool4 = ProductTools()
        sku_info = apitool3.select_sp_product_sku(market1, market2, advertisedSku)
        for sku in sku_info:
            try:
                new_sku = create_productsku(market1, new_campaign_id, new_adgroup_id, sku, asin=None, state='ENABLED')
            except Exception as e:
                # 处理异常，可以打印异常信息或者进行其他操作
                print("An error occurred create_productsku:", e)
                newdbtool = DbNewSpTools()
                newdbtool.create_sp_product(market1, new_campaign_id, None, sku, new_adgroup_id, None, "failed",
                                            datetime.now(), "SP")
        return f"{new_campaign_name} create successfully"

    def manual_targeting(self, res, market1, market2, new_campaign_name, ad_group_id, new_ad_group_name, exchange_rate, advertisedSku, brand_name='LAPASA'):
        dynamicBidding = res[0]['dynamicBidding']
        # 获取当前日期
        today = datetime.today()
        # 格式化输出
        startDate = today.strftime('%Y-%m-%d')
        name = new_campaign_name
        # name = 'DeepBI_MANUAL_test'
        apitool = Gen_campaign()
        # new_campaign_id = apitool.create_camapign(market1, name, startDate, dynamicBidding, portfolioId=None,
        #                                           endDate=None, targetingType='MANUAL', state='PAUSED',
        #                                           budgetType='DAILY', budget=5 * exchange_rate)
        new_campaign_id ='375406817475790'
        if new_campaign_id == "":
            return "No new campaign"
        apitool1 = AdGroupTools()
        # defaultBid_old = exchange_rate * apitool1.get_adGroup_api(market2, ad_group_id)
        new_adgroup_id = '477060288930098'
        # new_adgroup_id = create_adgroup(market1, new_campaign_id, new_ad_group_name, defaultBid_old, state='PAUSED')
        apitool3 = DbSpTools()
        apitool4 = ProductTools()

        # product_info = apitool4.get_product_api(market2, ad_group_id)
        # if product_info == None:
        #     pass
        # else:
        #     # 循环遍历列表中的每个字典
        #     for item in product_info:
        #         sku = item['sku']
        #         new_sku = create_productsku(market1, new_campaign_id, new_adgroup_id, sku, asin=None, state='ENABLED')

        sku_info = apitool3.select_sp_product_sku(market1, market2, advertisedSku)
        for sku in sku_info:
            try:
                new_sku = create_productsku(market1, new_campaign_id, new_adgroup_id, sku, asin=None, state='ENABLED')
            except Exception as e:
                # 处理异常，可以打印异常信息或者进行其他操作
                print("An error occurred create_productsku:", e)
                newdbtool = DbNewSpTools()
                newdbtool.create_sp_product(market1, new_campaign_id, None, sku, new_adgroup_id, None, "failed",
                                            datetime.now(), "SP")

        apitool2 = SPKeywordTools()
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
                        targetId = create_adGroup_Targeting2(market1, new_campaign_id, new_adgroup_id, bid, categories_id,target_brand_id)
                        break
        else:
            # 循环遍历列表中的每个字典
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
                        new_keyword_id = add_keyword_toadGroup(market1, new_campaign_id, matchType, state, bid, new_adgroup_id, keywordText)
                    except Exception as e:
                        # 处理异常，可以打印异常信息或者进行其他操作
                        print("An error occurred:", e)
                        dbNewTools = DbNewSpTools()
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
                        new_negativeKeywordId = add_adGroup_negative_keyword(market1, new_campaign_id, new_adgroup_id,
                                                                             keyword_text, match_type, state)
                    except Exception as e:
                        # 处理异常，可以打印异常信息或者进行其他操作
                        print("An error occurred:", e)
                        newdbtool = DbNewSpTools()
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


    def create_new_sp_asin_no_template(self,market,info):
        if market == 'SE':
            exchange_rate = 11.6
        else:
            exchange_rate = 1
        for i in info:
            name1 = f"DeepBI_0514_{i}_ASIN"
            api1 = DbSpTools()
            res = api1.select_sp_campaign_name(market,name1)
            if res[0] == "success":
                continue
            else:
                name = f"DeepBI_0514_{i}_ASIN"
                today = datetime.today()
                # 格式化输出
                startDate = today.strftime('%Y-%m-%d')
                apitool = Gen_campaign()
                new_campaign_id = apitool.create_camapign(market, name, startDate, dynamicBidding={"placementBidding":[],"strategy":"LEGACY_FOR_SALES"}, portfolioId=None,
                                                   endDate=None, targetingType='MANUAL', state='PAUSED',
                                                   budgetType='DAILY', budget=5 * exchange_rate)
                #new_campaign_id = '297477921455980'
                new_adgroup_id = create_adgroup(market, new_campaign_id, name, defaultBid=0.25 * exchange_rate, state='PAUSED')
                #new_adgroup_id = '491456703765912'
                sku_info = api1.select_sd_product_sku(market, i)
                for sku in sku_info:
                    try:
                        new_sku = create_productsku(market, new_campaign_id, new_adgroup_id, sku,asin=None, state="ENABLED")
                    except Exception as e:
                        # 处理异常，可以打印异常信息或者进行其他操作
                        print("An error occurred create_productsku:", e)
                        newdbtool = DbNewSpTools()
                        newdbtool.create_sp_product(market,new_campaign_id,None,sku,new_adgroup_id,None,"failed",datetime.now(),"SP")
                apitool1 = AdGroupTools()
                apitool2 = ProductTools()
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
                    for brand in brand_info['brands']:
                        if brand['name'] == target_brand_name:
                            target_brand_id = brand['id']
                            bid_info = apitool1.list_category_bid_recommendations(market, categories_id,
                                                                                  new_campaign_id, new_adgroup_id)
                            try:
                                # 尝试获取bid值
                                bid = bid_info['bidRecommendations'][0]['bidRecommendationsForTargetingExpressions'][0][
                                    'bidValues'][0]['suggestedBid']
                            except (IndexError, KeyError, TypeError):
                                # 如果在尝试获取bid值时发生任何异常（比如索引错误、键错误或类型错误），则设置bid为0.25
                                bid = 0.25 * exchange_rate
                            targetId = create_adGroup_Targeting2(market, new_campaign_id, new_adgroup_id, bid,
                                                                 categories_id, target_brand_id)
            print(f"{name} create successfully")
        print("all create successfully")

    def create_new_sp_asin_no_template_1(self,market):
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
        if market == 'SE':
            exchange_rate = 11.6
        else:
            exchange_rate = 1
        for nsspu, skus in result.items():
            print(f"nsspu: {nsspu}")
            print(f"skus: {skus}")
            name = f"DeepBI_0615_ASIN_{nsspu}"
            today = datetime.today()
            # 格式化输出
            startDate = today.strftime('%Y-%m-%d')
            apitool = Gen_campaign()
            new_campaign_id = apitool.create_camapign(market, name, startDate, dynamicBidding={"placementBidding": [],
                                                                                               "strategy": "LEGACY_FOR_SALES"},
                                                      portfolioId=None,
                                                      endDate=None, targetingType='MANUAL', state='PAUSED',
                                                      budgetType='DAILY', budget=3 * exchange_rate)
            # new_campaign_id = '350300198986995'
            if new_campaign_id == "":
                print("No new campaign")
                continue
            new_adgroup_id = create_adgroup(market, new_campaign_id, name, defaultBid=0.25 * exchange_rate,
                                            state='PAUSED')
            # new_adgroup_id = '419956427382148'
            # 在这里可以添加你的进一步处理或操作，例如：
            for sku in skus:
                try:
                    new_sku = create_productsku(market, new_campaign_id, new_adgroup_id, sku, asin=None,
                                                state="ENABLED")
                except Exception as e:
                    # 处理异常，可以打印异常信息或者进行其他操作
                    print("An error occurred create_productsku:", e)
                    newdbtool = DbNewSpTools()
                    newdbtool.create_sp_product(market, new_campaign_id, None, sku, new_adgroup_id, None, "failed",
                                                datetime.now(), "SP")
            apitool1 = AdGroupTools()
            apitool2 = ProductTools()
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

                        targetId1 = create_adGroup_Targeting2(market, new_campaign_id, new_adgroup_id, bid1,
                                                             categories_id, target_brand_id)

                try:
                    # 尝试获取bid值
                    bid2 = bid_info['bidRecommendations'][0]['bidRecommendationsForTargetingExpressions'][0][
                        'bidValues'][1]['suggestedBid']
                except (IndexError, KeyError, TypeError):
                    # 如果在尝试获取bid值时发生任何异常（比如索引错误、键错误或类型错误），则设置bid为0.25
                    bid2 = 0.25 * exchange_rate
                targetId2 = create_adGroup_Targeting1(market, new_campaign_id, new_adgroup_id, categories_id, bid2,
                                                      state='ENABLED', type='ASIN_CATEGORY_SAME_AS')
            print("------------------------")
            print(f"{name} create successfully")
        print("all create successfully")

# 创建 Ceate_new_sku 类的实例
ceate_new_sku_instance = Ceate_new_sku()

# 调用 create_new_sku() 方法
#ceate_new_sku_instance.create_new_sku(market1='NL', market2='DE')#exchange rate from market2 to market1
ceate_new_sku_instance.create_new_sp_asin_no_template('DE',['G11', 'K01', 'L01', 'L02', 'L17', 'L39', 'L48', 'L49', 'L52', 'L55', 'L58', 'M05', 'M07', 'M126', 'M19', 'M24', 'M31', 'M35', 'M36', 'M38', 'M39', 'M57', 'M79', 'M92', 'M93'])
#ceate_new_sku_instance.create_new_sp_asin_no_template_1('FR')


