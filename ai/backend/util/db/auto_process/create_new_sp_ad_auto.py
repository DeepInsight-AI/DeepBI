import pandas as pd
import json
from tools_sp_campaign import CampaignTools
from gen_campaign import Gen_campaign
from datetime import datetime
from tools_sp_adGroup import AdGroupTools
from gen_adgroup import create_adgroup, add_adGroup_negative_keyword, update_adGroup_TargetingClause
from tools_sp_product import ProductTools
from gen_product import create_productsku
from tools_sp_keyword import SPKeywordTools
from gen_keyword import add_keyword_toadGroup
from tools_db_new_sp import DbNewSpTools

class Ceate_new_sku:

    def __init__(self):
        self.config = self.load_config()

    def load_config(self):
        exchange_rate_path = './exchange_rate.json'
        with open(exchange_rate_path) as f:
            return json.load(f)

    def get_exchange_rate(self, market1, market2):
        return self.config['exchange_rate'].get(market2, {}).get(market1)

    def create_new_sku(self,market1,market2):
        uploaded_file = 'C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/db_amazon/SE_FR_2024-04-21_2024-05-22_sp_sku_new.csv'
        df = pd.read_csv(uploaded_file)
        # 打印 DataFrame 的前几行，以确保成功读取
        print(df.head())
        exchange_rate = self.get_exchange_rate(market1, market2)
        processed_ids = {'campaign_ids': set(), 'ad_group_ids': set()}
        # 循环处理每一行数据
        for index, row in df.iterrows():
            campaign_id = row['campaignId']
            ad_group_id = row['adGroupId']
            new_campaign_name = row['new_campaignName']
            new_ad_group_name = row['new_adGroupName']

            if campaign_id not in processed_ids['campaign_ids']:
                # 如果是第一次遇到，执行相应操作
                # 将当前的 campaign_id 添加到已处理的字典中
                processed_ids['campaign_ids'].add(campaign_id)
                processed_ids['ad_group_ids'].add(ad_group_id)
                # 执行创建
                apitool = Gen_campaign()
                print(index)
                res = apitool.list_camapign(campaign_id, market2)
                for item in res:
                    if isinstance(item, dict):
                        if item.get('targetingType') == 'AUTO':
                            answer_message = self.auto_targeting(res, market1, market2, new_campaign_name, ad_group_id, new_ad_group_name, exchange_rate)
                            print(answer_message)
                            continue
                        else:
                            answer_message = self.manual_targeting(res, market1, market2, new_campaign_name, ad_group_id, new_ad_group_name, exchange_rate)
                            print(answer_message)
                            continue

            else:
                # TODO 同一campaign不同ad_group的处理
                # 如果不是第一次遇到 campaign_id，则再检查 ad_group_id 是否是第一次遇到
                if ad_group_id not in processed_ids['ad_group_ids']:
                    # 如果是第一次遇到 ad_group_id，执行相应操作
                    print(index)
                    # 将当前的 ad_group_id 添加到已处理的字典中
                    processed_ids['ad_group_ids'].add(ad_group_id)
                    print("Unprocessed ad groups:",ad_group_id)
                    # 因为 ad_group_id 是第一次遇到，所以我们可以直接进入下一次迭代
                    continue

                # 如果 ad_group_id 不是第一次遇到，则直接进行下一次迭代
                print(index)
                continue



    def auto_targeting(self, res, market1,market2, new_campaign_name, ad_group_id, new_ad_group_name, exchange_rate):
        dynamicBidding = res[0]['dynamicBidding']
        # 获取当前日期
        today = datetime.today()
        # 格式化输出
        startDate = today.strftime('%Y-%m-%d')
        # name = 'DeepBI_AUTO_test'
        name = new_campaign_name
        apitool = Gen_campaign()
        new_campaign_id = apitool.create_camapign(market1, name, dynamicBidding, startDate, portfolioId=None, endDate=None, targetingType='AUTO', state='PAUSED', budgetType='DAILY', budget=5*exchange_rate)
        # new_campaign_id ='505065862687647'
        apitool1 = AdGroupTools()
        defaultBid_old = exchange_rate * apitool1.get_adGroup_api(market2, ad_group_id)
        # new_adgroup_id = 318872153465155
        new_adgroup_id = create_adgroup(market1, new_campaign_id, new_ad_group_name, defaultBid_old, state='PAUSED')
        market2_TargetingClause = apitool1.list_adGroup_TargetingClause(ad_group_id,market2)
        market1_TargetingClause = apitool1.list_adGroup_TargetingClause(new_adgroup_id, market1)
        state_bid_info = {}
        for item in market2_TargetingClause:
            type_ = item['expression'][0]['type']
            state_bid_info[type_] = {'state': item['state'], 'bid': item['bid']}

        # 将'state'和'bid'信息应用到第二个查询结果中相同类型的条目中
        for item in market1_TargetingClause:
            type_ = item['expression'][0]['type']
            if type_ in state_bid_info:
                target_id = item['targetId']
                state = state_bid_info[type_]['state']
                bid = exchange_rate * state_bid_info[type_]['bid']
                targetId = update_adGroup_TargetingClause(market1,target_id, state, bid)

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
        # TODO 变体的添加，暂时只能将模板的sku全部照搬过来
        apitool2 = ProductTools()
        product_info = apitool2.get_product_api(market2,ad_group_id)
        if product_info == None:
            pass
        else:
            # 循环遍历列表中的每个字典
            for item in product_info:
                sku = item['sku']
                new_sku = create_productsku(market1, new_campaign_id,new_adgroup_id,sku,asin=None,state='ENABLED')
        return state_bid_info

    # TODO Manual Targeting为Product targeting类型的添加
    def manual_targeting(self, res, market1, market2, new_campaign_name, ad_group_id, new_ad_group_name, exchange_rate):
        dynamicBidding = res[0]['dynamicBidding']
        # 获取当前日期
        today = datetime.today()
        # 格式化输出
        startDate = today.strftime('%Y-%m-%d')
        name = new_campaign_name
        # name = 'DeepBI_MANUAL_test'
        apitool = Gen_campaign()
        new_campaign_id = apitool.create_camapign(market1, name, dynamicBidding, startDate, portfolioId=None,
                                                  endDate=None, targetingType='MANUAL', state='PAUSED',
                                                  budgetType='DAILY', budget=5 * exchange_rate)
        # new_campaign_id ='293153114778136'
        apitool1 = AdGroupTools()
        defaultBid_old = exchange_rate * apitool1.get_adGroup_api(market2, ad_group_id)
        # new_adgroup_id = 299968987495182
        new_adgroup_id = create_adgroup(market1, new_campaign_id, new_ad_group_name, defaultBid_old, state='PAUSED')
        apitool2 = SPKeywordTools()
        spkeyword_info = apitool2.get_spkeyword_api(market2, ad_group_id)
        if spkeyword_info == None:
            pass
        else:
            # 循环遍历列表中的每个字典
            for item in spkeyword_info:
                matchType = item['matchType']
                state = item['state']
                bid = exchange_rate * item['bid']
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
                    newdbtool.add_sp_adGroup_negativeKeyword(market1, None, new_adgroup_id, new_campaign_id, None, match_type,
                                                             state, keyword_text, None,
                                                             None, "failed", datetime.now())
                # 继续执行下面的代码

                else:
                    # 如果没有发生异常，则执行以下代码
                    # 这里可以放一些正常情况下的逻辑
                    print("Command executed successfully.")
        # TODO 变体的添加，暂时只能将模板的sku全部照搬过来
        apitool3 = ProductTools()
        product_info = apitool3.get_product_api(market2, ad_group_id)
        if product_info == None:
            pass

        else:
            # 循环遍历列表中的每个字典
            for item in product_info:
                sku = item['sku']
                new_sku = create_productsku(market1, new_campaign_id, new_adgroup_id, sku, asin=None, state='ENABLED')
        return product_info



# 创建 Ceate_new_sku 类的实例
ceate_new_sku_instance = Ceate_new_sku()

# 调用 create_new_sku() 方法
ceate_new_sku_instance.create_new_sku(market1='SE', market2='FR')#exchange rate from market2 to market1


