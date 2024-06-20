import time

import pandas as pd
import json
from tools_sd_campaign import CampaignTools
from gen_sd_campaign import Gen_campaign
from datetime import datetime
from tools_sd_adGroup  import AdGroupTools_SD
from gen_sd_adgroup import create_adgroup, add_adGroup_negative_keyword, create_adGroup_Targeting1,create_adGroup_Targeting2
from tools_sd_product import ProductTools
from gen_sd_product import create_productsku,create_creatives
from tools_db_new_sp import DbNewSpTools
from tools_db_sp import DbSpTools
from ai.backend.util.db.db_amazon.amazon_mysql_rag_util_new_sd import AmazonMysqlNEWSDRagUitl
from tools_sp_adGroup import AdGroupTools




class Ceate_new_sd:

    def __init__(self):
        self.config = self.load_config()

    def load_config(self):
        exchange_rate_path = './exchange_rate.json'
        with open(exchange_rate_path) as f:
            return json.load(f)

    def get_exchange_rate(self, market1, market2):
        return self.config['exchange_rate'].get(market2, {}).get(market1)

    def sku_change(self,sku):
        db_info = {'host': '192.168.5.114', 'user': 'test_deepdata', 'passwd': 'test123!@#', 'port': 3308,
                   'db': 'amazon_ads',
                   'charset': 'utf8mb4', 'use_unicode': True, }

        amazon = AmazonMysqlNEWSDRagUitl(db_info)
        new_sku = amazon.get_sd_sku_change(sku)
        return new_sku

    def create_new_sd_template(self,market1,market2):
        uploaded_file = 'C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/db_amazon/IT_FR_2024-04-23_2024-05-20_sd_sku_new.csv'
        df = pd.read_csv(uploaded_file)
        # 打印 DataFrame 的前几行，以确保成功读取
        print(df.head())
        exchange_rate = self.get_exchange_rate(market1, market2)
        processed_ids = {'campaign_ids': set(), 'ad_group_ids': set()}
        # 循环处理每一行数据
        for index, row in df.iterrows():
            campaign_id = row['campaignId']
            ad_group_id = row['adGroupId']
            promotedSku = row['promotedSku']
            new_campaign_name = row['new_campaignName']
            new_ad_group_name = row['new_adGroupName']

            if campaign_id not in processed_ids['campaign_ids']:
                # 如果是第一次遇到，执行相应操作
                # 将当前的 campaign_id 添加到已处理的字典中
                processed_ids['campaign_ids'].add(campaign_id)
                processed_ids['ad_group_ids'].add(ad_group_id)
                # 执行创建
                apitool = CampaignTools()
                print(index)
                res = apitool.list_campaigns_api(campaign_id, market2)
                if res['tactic'] == 'T00020':
                    answer_message = self.auto_targeting(res, market1, market2, new_campaign_name, ad_group_id,
                                                         new_ad_group_name, exchange_rate, promotedSku)
                    print(answer_message)
                    continue
                elif res['tactic'] == 'T00030':
                    # answer_message = self.manual_targeting(res, market1, market2, new_campaign_name, ad_group_id,
                    #                                        new_ad_group_name, exchange_rate)
                    # print(answer_message)
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



    def auto_targeting(self, res, market1,market2, new_campaign_name, ad_group_id, new_ad_group_name, exchange_rate, promotedSku,brand_name='LAPASA'):
        costType = res['costType']
        # 获取当前日期
        today = datetime.today()
        # 格式化输出
        startDate = today.strftime('%Y%m%d')
        #name = 'DeepBI_sd_test'
        name = new_campaign_name
        apitool = Gen_campaign()
        new_campaign_id = apitool.create_camapign(market1, name, startDate, costType, portfolioId=None, endDate=None, tactic='T00020', state='paused', budgetType='daily', budget=5*exchange_rate)
        #new_campaign_id =337150496688097
        apitool1 = AdGroupTools_SD()
        ad_group_info = apitool1.get_adGroup_api(market2, ad_group_id)
        defaultBid = exchange_rate * ad_group_info["defaultBid"]
        bidOptimization = ad_group_info["bidOptimization"]
        creativeType = ad_group_info["creativeType"]
        #new_adgroup_id = 542331733479809
        new_adgroup_id = create_adgroup(market1, new_campaign_id, new_ad_group_name,bidOptimization,creativeType,state='paused', defaultBid=2.49*exchange_rate)
        # TODO 变体的添加，暂时只能将模板的sku全部照搬过来
        apitool2 = ProductTools()
        apitool3 = DbSpTools()
        # product_info = apitool2.get_product_api(market2, ad_group_id)
        # print(product_info)
        # if product_info == None:
        #     pass
        # else:
        #     if market2 == 'US':
        #         # 循环遍历列表中的每个字典
        #         for item in product_info:
        #             sku = self.sku_change(item['sku'])
        #             state = item['state']
        #             new_sku = create_productsku(market1, new_campaign_id, new_adgroup_id, sku, state)
        #     else:
        #         for item in product_info:
        #             sku = item['sku']
        #             state = item['state']
        #             new_sku = create_productsku(market1, new_campaign_id, new_adgroup_id, sku, state)
        sku_info = apitool3.select_sp_product_sku(market1, market2, promotedSku)
        for sku in sku_info:
            try:
                new_sku = create_productsku(market1, new_campaign_id, new_adgroup_id, sku, state="enabled")
            except Exception as e:
                # 处理异常，可以打印异常信息或者进行其他操作
                print("An error occurred create_productsku:", e)
                newdbtool = DbNewSpTools()
                newdbtool.create_sp_product(market1, new_campaign_id, None, sku, new_adgroup_id, None, "failed",
                                            datetime.now(), "SD")
        adgroup_Targeting_info = apitool1.list_adGroup_Targeting(market2, ad_group_id)
        print(adgroup_Targeting_info)
        #循环处理第二个查询结果
        for result in adgroup_Targeting_info:
            bid = result['bid']
            expression_type = result['expressionType']
            state = result['state']
            resolved_expressions = result['resolvedExpression']

            # 如果 resolvedExpression 中只有一个字典，则提取相关信息
            if len(resolved_expressions) == 1:
                resolved_expression = resolved_expressions[0]
                if resolved_expression['type'] == 'similarProduct':
                    try:
                        new_targetId = create_adGroup_Targeting1(market1, new_adgroup_id,expression_type, state, bid)
                    except Exception as e:
                        # 处理异常，可以打印异常信息或者进行其他操作
                        print("An error occurred:", e)
                else:
                    pass
            else:
                pass

        new_product_info = apitool2.get_product_api(market1, new_adgroup_id)
        product = [{"asin": result["asin"]} for result in new_product_info]
        recommendations = apitool1.list_adGroup_Targetingrecommendations(market1, new_adgroup_id, product)
        # 选取满足条件的元素
        # selected_categories = [category for category in recommendations['recommendations']['categories'] if
        #                        1 <= category['rank'] <= 20]
        selected_categories = recommendations['recommendations']['categories']
        # 打印选取的元素
        print(selected_categories)
        # for category in selected_categories:
        #     category = category['category']
        #     try:
        #         new_targetId = create_adGroup_Targeting2(market1, new_adgroup_id, category, brand_id=None , expression_type='manual',
        #                                                  state='enabled', bid=2.49*exchange_rate)
        #     except Exception as e:
        #         # 处理异常，可以打印异常信息或者进行其他操作
        #         print("An error occurred:", e)
        for category in selected_categories:
            category = category['category']
            api2 = AdGroupTools()
            brand_info = api2.list_category_refinements(market1, category)
            # 检查是否存在名为"LAPASA"的品牌
            target_brand_name = brand_name
            target_brand_id = None
            for brand in brand_info['brands']:
                if brand['name'] == target_brand_name:
                    target_brand_id = brand['id']
                    try:
                        new_targetId = create_adGroup_Targeting2(market1, new_adgroup_id, category, target_brand_id,
                                                                 expression_type='manual', state='enabled', bid=2.49)
                    except Exception as e:
                        # 处理异常，可以打印异常信息或者进行其他操作
                        print("An error occurred:", e)
        creativeId = create_creatives(market1, new_adgroup_id)
        return recommendations

    # TODO Manual Targeting为Product targeting类型的添加
    def manual_targeting(self, res, market1, market2, new_campaign_name, ad_group_id, new_ad_group_name, exchange_rate):
        pass

    def create_new_sd_no_template(self,market,info,brand_name='LAPASA'):
        for i in info:
            name1 = f"DeepBI_0509_{i}"
            ct = CampaignTools()
            res = ct.list_all_campaigns_api(market)
            #if len(res) == 0:
            if any(result['name'].lower() == name1.lower() for result in res):
                print(f"{name1} is already exist")
            else:
                api1 = DbSpTools()
            # res = api1.select_sd_campaign_name(market,name1)
            # if res[0] == "success":
            #     continue
            # else:
                name = f"DeepBI_0509_{i}_AUTOGENE"
                today = datetime.today()
                # 格式化输出
                startDate = today.strftime('%Y%m%d')
                apitool = Gen_campaign()
                new_campaign_id = apitool.create_camapign(market, name, startDate,costType='vcpm', portfolioId=None,
                                                          endDate=None, tactic='T00020', state='paused',
                                                          budgetType='daily', budget=5)
                # new_campaign_id = 386392012954542
                new_adgroup_id = create_adgroup(market, new_campaign_id, name, bidOptimization='reach',
                                                creativeType='IMAGE', state='paused', defaultBid=2.49)
                # new_adgroup_id = 485848574838633
                sku_info = api1.select_sd_product_sku(market, i)
                for sku in sku_info:
                    try:
                        new_sku = create_productsku(market, new_campaign_id, new_adgroup_id, sku, state="enabled")
                    except Exception as e:
                        # 处理异常，可以打印异常信息或者进行其他操作
                        print("An error occurred create_productsku:", e)
                        newdbtool = DbNewSpTools()
                        newdbtool.create_sp_product(market,new_campaign_id,None,sku,new_adgroup_id,None,"failed",datetime.now(),"SD")
                apitool2 = ProductTools()
                time.sleep(10)
                new_product_info = apitool2.get_product_api(market, new_adgroup_id)
                product = [{"asin": result["asin"]} for result in new_product_info if "asin" in result]
                apitool1 = AdGroupTools_SD()
                recommendations = apitool1.list_adGroup_Targetingrecommendations(market, new_adgroup_id, product)
                # 选取满足条件的元素
                # selected_categories = [category for category in recommendations['recommendations']['categories'] if
                #                        1 <= category['rank'] <= 10]
                selected_categories = recommendations['recommendations']['categories']

                # 打印选取的元素
                print(selected_categories)
                for category in selected_categories:
                    category = category['category']
                    api2 = AdGroupTools()
                    brand_info = api2.list_category_refinements(market, category)
                    # 检查是否存在名为"LAPASA"的品牌
                    target_brand_name = brand_name
                    target_brand_id = None
                    for brand in brand_info['brands']:
                        if brand['name'] == target_brand_name:
                            target_brand_id = brand['id']
                            try:
                                new_targetId = create_adGroup_Targeting2(market, new_adgroup_id, category, target_brand_id, expression_type='manual', state='enabled', bid=2.49)
                            except Exception as e:
                                # 处理异常，可以打印异常信息或者进行其他操作
                                print("An error occurred:", e)
                creativeId = create_creatives(market, new_adgroup_id)

        pass

    def create_new_sd_no_template_1(self,market,brand_name='LAPASA'):
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
        for nsspu, skus in result.items():
            name1 = f"DeepBI_0615_Category_{nsspu}"
            ct = CampaignTools()
            res = ct.list_all_campaigns_api(market)
            #if len(res) == 0:
            if any(result['name'].lower() == name1.lower() for result in res):
                print(f"{name1} is already exist")
            else:
                api1 = DbSpTools()
            # res = api1.select_sd_campaign_name(market,name1)
            # if res[0] == "success":
            #     continue
            # else:
                name = f"DeepBI_0615_Category_{nsspu}"
                today = datetime.today()
                # 格式化输出
                startDate = today.strftime('%Y%m%d')
                apitool = Gen_campaign()
                new_campaign_id = apitool.create_camapign(market, name, startDate,costType='vcpm', portfolioId=None,
                                                          endDate=None, tactic='T00020', state='paused',
                                                          budgetType='daily', budget=5)
                # new_campaign_id = 386392012954542
                if new_campaign_id == "":
                    print("No new campaign")
                    continue
                new_adgroup_id = create_adgroup(market, new_campaign_id, name, bidOptimization='reach',
                                                creativeType='IMAGE', state='paused', defaultBid=2.49)
                # new_adgroup_id = 485848574838633
                for sku in skus:
                    try:
                        new_sku = create_productsku(market, new_campaign_id, new_adgroup_id, sku, state="enabled")
                    except Exception as e:
                        # 处理异常，可以打印异常信息或者进行其他操作
                        print("An error occurred create_productsku:", e)
                        newdbtool = DbNewSpTools()
                        newdbtool.create_sp_product(market,new_campaign_id,None,sku,new_adgroup_id,None,"failed",datetime.now(),"SD")
                apitool2 = ProductTools()
                time.sleep(10)
                new_product_info = apitool2.get_product_api(market, new_adgroup_id)
                product = [{"asin": result["asin"]} for result in new_product_info if "asin" in result]
                apitool1 = AdGroupTools_SD()
                recommendations = apitool1.list_adGroup_Targetingrecommendations(market, new_adgroup_id, product)
                # 选取满足条件的元素
                # selected_categories = [category for category in recommendations['recommendations']['categories'] if
                #                        1 <= category['rank'] <= 10]
                selected_categories = recommendations['recommendations']['categories']

                # 打印选取的元素
                print(selected_categories)
                for category in selected_categories:
                    category = category['category']
                    api2 = AdGroupTools()
                    brand_info = api2.list_category_refinements(market, category)
                    # 检查是否存在名为"LAPASA"的品牌
                    target_brand_name = brand_name
                    target_brand_id = None
                    for brand in brand_info['brands']:
                        if brand['name'] == target_brand_name:
                            target_brand_id = brand['id']
                            try:
                                new_targetId = create_adGroup_Targeting2(market, new_adgroup_id, category, target_brand_id, expression_type='manual', state='enabled', bid=2.49)
                            except Exception as e:
                                # 处理异常，可以打印异常信息或者进行其他操作
                                print("An error occurred:", e)
                creativeId = create_creatives(market, new_adgroup_id)

        pass

    def create_new_sd_no_template_error(self, market, info, brand_name='LAPASA'):
        for i in info:
            new_adgroup_id = i
            apitool2 = ProductTools()
            new_product_info = apitool2.get_product_api(market, new_adgroup_id)
            product = [{"asin": result["asin"]} for result in new_product_info if "asin" in result]
            apitool1 = AdGroupTools_SD()
            recommendations = apitool1.list_adGroup_Targetingrecommendations(market, new_adgroup_id, product)
            # 选取满足条件的元素
            # selected_categories = [category for category in recommendations['recommendations']['categories'] if
            #                        1 <= category['rank'] <= 10]
            selected_categories = recommendations['recommendations']['categories']

            # 打印选取的元素
            print(selected_categories)
            for category in selected_categories:
                time.sleep(5)
                category = category['category']
                api2 = AdGroupTools()
                brand_info = api2.list_category_refinements(market, category)
                # 检查是否存在名为"LAPASA"的品牌
                target_brand_name = brand_name
                target_brand_id = None
                for brand in brand_info['brands']:
                    if brand['name'] == target_brand_name:
                        target_brand_id = brand['id']
                        try:
                            new_targetId = create_adGroup_Targeting2(market, new_adgroup_id, category, target_brand_id,
                                                                     expression_type='manual', state='enabled', bid=2.49)
                        except Exception as e:
                            # 处理异常，可以打印异常信息或者进行其他操作
                            print("An error occurred:", e)

# 创建 Ceate_new_sku 类的实例
ceate_new_sku_instance = Ceate_new_sd()

# 调用 create_new_sku() 方法
#ceate_new_sku_instance.create_new_sd_template(market1='IT', market2='FR')#exchange rate from market2 to market1
ceate_new_sku_instance.create_new_sd_no_template(market='DE',info=['K01', 'L01', 'L02', 'L17', 'L39', 'L48', 'L49', 'L52', 'L55', 'L58', 'M05', 'M07', 'M126', 'M19', 'M24', 'M31', 'M35', 'M36', 'M38', 'M39', 'M57', 'M79', 'M92', 'M93'])
#ceate_new_sku_instance.create_new_sd_no_template_error(market='DE',info=['378889236536746'])
#ceate_new_sku_instance.create_new_sd_no_template_1('FR')
# res = ceate_new_sku_instance.sku_change('LPM17AW0038NBRD0XSA1')
# print(res)
#378889236536746  1981106031 RANK11
#'G11', 缺少创意

#(market='NL',info=[ 'L58', 'M63', 'L02', 'G19', 'B14', 'M122', 'M05', 'M08', 'K01', 'L48', 'M35', 'M131', 'M19', 'M36', 'L55', 'L52', 'M93'])
