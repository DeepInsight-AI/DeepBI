from datetime import datetime
import os

import pandas as pd
import json
from ai.backend.util.db.auto_process.gen_sp_campaign import Gen_campaign
from ai.backend.util.db.auto_process.gen_sp_keyword import Gen_keyword  #add_keyword_toadGroup_v0,update_keyword_toadGroup
from ai.backend.util.db.auto_process.gen_sp_adgroup import Gen_adgroup  #add_adGroup_negative_keyword_v0,update_adGroup_TargetingClause
from ai.backend.util.db.auto_process.gen_sp_product import Gen_product
from ai.backend.util.db.auto_process.tools_db_new_sp import DbNewSpTools
from ai.backend.util.db.auto_process.tools_sp_adGroup import AdGroupTools

class auto_api:
    def __init__(self,brand):
        self.brand = brand

    def update_sp_ad_budget(self,market, path):
        uploaded_file = path
        if os.stat(uploaded_file).st_size > 2:
            df = pd.read_csv(uploaded_file)
            # 打印 DataFrame 的前几行，以确保成功读取
            print(df.head())
            # 将DataFrame转换为一个列表，每个元素是一个字典表示一行数据
            df_data = json.loads(df.to_json(orient='records'))
            print(df_data)

            for item in df_data:
                campaign_id = item["campaignId"]
                campaign_name = item["campaignName"]
                Budget = item["Budget"]
                new_Budget = item["New_Budget"]
                api1 = Gen_campaign(self.brand)
                if new_Budget == "关闭":
                    api1.update_camapign_v0(market, str(campaign_id), campaign_name, Budget, budget_new=Budget, state="PAUSED")
                else:

                    api1.update_camapign_v0(market, str(campaign_id), campaign_name, Budget, float(new_Budget), state="ENABLED")

    def rollback_sp_ad_budget(self,market, path):
        uploaded_file = path
        df = pd.read_csv(uploaded_file)
        # 打印 DataFrame 的前几行，以确保成功读取
        print(df.head())
        # 将DataFrame转换为一个列表，每个元素是一个字典表示一行数据
        df_data = json.loads(df.to_json(orient='records'))
        print(df_data)

        for item in df_data:
            campaign_id = item["campaignId"]
            campaign_name = item["campaignName"]
            Budget = item["Budget"]
            new_Budget = item["New_Budget"]
            api1 = Gen_campaign(self.brand)
            if new_Budget == "关闭":
                api1.update_camapign_v0(market, str(campaign_id), campaign_name, Budget, budget_new=Budget, state="ENABLED")
            else:
                api1.update_camapign_v0(market, str(campaign_id), campaign_name, new_Budget, float(Budget), state="ENABLED")


    def update_sp_ad_placement(self,market, path):
        uploaded_file = path
        if os.stat(uploaded_file).st_size > 2:
            df = pd.read_csv(uploaded_file)
            # 打印 DataFrame 的前几行，以确保成功读取
            print(df.head())
            # 将DataFrame转换为一个列表，每个元素是一个字典表示一行数据
            df_data = json.loads(df.to_json(orient='records'))
            print(df_data)
        #     df_data={
        # "data": [
        # {
        #   "market": "FR",
        #   "sum( sales )": 60.62,
        #   "sum( cost )": 0.38,
        #   "Budget": 8,
        #   "campaignId": 323158262143023,
        #   "campaignName": "DeepBI_0509_m19",
        #   "adGroupId": 438024412988838,
        #   "adGroupName": "DeepBI_0509_m19",
        #   "placementClassification": "Top of Search on-Amazon",
        #   "new_campaignName": "DeepBI_0514_k01_ASIN",
        #   "new_adGroupName": "DeepBI_0514_k01_ASIN",
        #   "new_Budget": 10
        # }
        # ]
        # }

            for item in df_data:
                campaign_id = item["campaignId"]
                Budget = item["bid"]
                new_Budget = item["new_bid"]
                placementClassification = item["placementClassification"]
                if placementClassification == "Top of Search on-Amazon":
                    placementClassification = "PLACEMENT_TOP"
                elif placementClassification == "Detail Page on-Amazon":
                    placementClassification = "PLACEMENT_PRODUCT_PAGE"
                elif placementClassification == "Other on-Amazon":
                    placementClassification = "PLACEMENT_REST_OF_SEARCH"
                api1 = Gen_campaign(self.brand)
                api1.update_campaign_placement(market, str(campaign_id), Budget, new_Budget, placementClassification)

    def rollback_sp_ad_placement(self,market, path):
        uploaded_file = path
        if os.stat(uploaded_file).st_size > 2:
            df = pd.read_csv(uploaded_file)
            # 打印 DataFrame 的前几行，以确保成功读取
            print(df.head())
            # 将DataFrame转换为一个列表，每个元素是一个字典表示一行数据
            df_data = json.loads(df.to_json(orient='records'))
            print(df_data)

            for item in df_data:
                campaign_id = item["campaignId"]
                Budget = item["bid"]
                new_Budget = item["new_bid"]
                placementClassification = item["placementClassification"]
                if placementClassification == "Top of Search on-Amazon":
                    placementClassification = "PLACEMENT_TOP"
                elif placementClassification == "Detail Page on-Amazon":
                    placementClassification = "PLACEMENT_PRODUCT_PAGE"
                elif placementClassification == "Other on-Amazon":
                    placementClassification = "PLACEMENT_REST_OF_SEARCH"
                api1 = Gen_campaign(self.brand)
                api1.update_campaign_placement(market, str(campaign_id), new_Budget, Budget, placementClassification)


    def add_sp_ad_searchTerm_keyword(self,market, path):
        uploaded_file = path
        if os.stat(uploaded_file).st_size > 2:
            df = pd.read_csv(uploaded_file)
            # 打印 DataFrame 的前几行，以确保成功读取
            print(df.head())
            # 将DataFrame转换为一个列表，每个元素是一个字典表示一行数据
            df_data = json.loads(df.to_json(orient='records'))
            print(df_data)
        #     df_data={
        # "data": [
        # {
        #   "market": "FR",
        #   "sum( sales )": 60.62,
        #   "sum( cost )": 0.38,
        #   "Budget": 8,
        #   "campaignId": 323158262143023,
        #   "campaignName": "DeepBI_0509_m19",
        #   "adGroupId": 438024412988838,
        #   "adGroupName": "DeepBI_0509_m19",
        #   "placementClassification": "Top of Search on-Amazon",
        #   "new_campaignName": "DeepBI_0514_k01_ASIN",
        #   "new_adGroupName": "DeepBI_0514_k01_ASIN",
        #   "new_Budget": 10
        # }
        # ]
        # }
            api = Gen_keyword(self.brand)
            for item in df_data:
                campaign_id = item["campaignId"]
                adGroupId = item["adGroupId"]
                searchTerm = item["searchTerm"]
                api.add_keyword_toadGroup_v0(market, str(campaign_id), str(adGroupId), searchTerm, matchType="EXACT", state="ENABLED", bid=0.5)
                api.add_keyword_toadGroup_v0(market, str(campaign_id), str(adGroupId), searchTerm, matchType="PHRASE", state="ENABLED", bid=0.5)
                api.add_keyword_toadGroup_v0(market, str(campaign_id), str(adGroupId), searchTerm, matchType="BROAD",
                                             state="ENABLED", bid=0.5)

    def add_sp_ad_auto_searchTerm_keyword(self,market, path):
        uploaded_file = path
        if os.stat(uploaded_file).st_size > 2:
            df = pd.read_csv(uploaded_file)
            # 打印 DataFrame 的前几行，以确保成功读取
            print(df.head())
            # 将DataFrame转换为一个列表，每个元素是一个字典表示一行数据
            df_data = json.loads(df.to_json(orient='records'))
            print(df_data)
        #     df_data={
        # "data": [
        # {
        #   "market": "FR",
        #   "sum( sales )": 60.62,
        #   "sum( cost )": 0.38,
        #   "Budget": 8,
        #   "campaignId": 323158262143023,
        #   "campaignName": "DeepBI_0509_m19",
        #   "adGroupId": 438024412988838,
        #   "adGroupName": "DeepBI_0509_m19",
        #   "placementClassification": "Top of Search on-Amazon",
        #   "new_campaignName": "DeepBI_0514_k01_ASIN",
        #   "new_adGroupName": "DeepBI_0514_k01_ASIN",
        #   "new_Budget": 10
        # }
        # ]
        # }
            api = Gen_keyword(self.brand)
            for item in df_data:
                if "new_campaignId" in item and item["new_campaignId"]:
                    campaign_id = item["new_campaignId"]
                    adGroupId = item["new_adGroupId"]
                    searchTerm = item["searchTerm"]
                    api.add_keyword_toadGroup_v0(market, str(int(campaign_id)), str(int(adGroupId)), searchTerm, matchType="EXACT", state="ENABLED", bid=0.5)
                    api.add_keyword_toadGroup_v0(market, str(int(campaign_id)), str(int(adGroupId)), searchTerm, matchType="PHRASE", state="ENABLED", bid=0.5)
                    api.add_keyword_toadGroup_v0(market, str(int(campaign_id)), str(int(adGroupId)), searchTerm, matchType="BROAD", state="ENABLED", bid=0.5)


    def rollback_sp_ad_searchTerm_keyword(self,market, info):
        for keywordId in info:
            api = Gen_keyword(self.brand)
            api.update_keyword_toadGroup(market, str(keywordId), bid_old=None, bid_new=None, state="PAUSED")



    def add_sp_ad_searchTerm_negative_keyword(self,market, path):
        uploaded_file = path
        if os.stat(uploaded_file).st_size > 2:
            df = pd.read_csv(uploaded_file)
            # 打印 DataFrame 的前几行，以确保成功读取
            print(df.head())
            # 将DataFrame转换为一个列表，每个元素是一个字典表示一行数据
            df_data = json.loads(df.to_json(orient='records'))
            print(df_data)
        #     df_data={
        # "data": [
        # {
        #   "market": "FR",
        #   "sum( sales )": 60.62,
        #   "sum( cost )": 0.38,
        #   "Budget": 8,
        #   "campaignId": 323158262143023,
        #   "campaignName": "DeepBI_0509_m19",
        #   "adGroupId": 438024412988838,
        #   "adGroupName": "DeepBI_0509_m19",
        #   "placementClassification": "Top of Search on-Amazon",
        #   "new_campaignName": "DeepBI_0514_k01_ASIN",
        #   "new_adGroupName": "DeepBI_0514_k01_ASIN",
        #   "new_Budget": 10
        # }
        # ]
        # }
            api1 = Gen_adgroup(self.brand)
            for item in df_data:
                campaign_id = item["campaignId"]
                adGroupId = item["adGroupId"]
                searchTerm = item["searchTerm"]
                try:
                    api1.add_adGroup_negative_keyword_v0(market, str(campaign_id), str(adGroupId), searchTerm, matchType="NEGATIVE_EXACT", state="ENABLED")
                except Exception as e:
                    print("An error occurred:", e)
                    newdbtool = DbNewSpTools(self.brand,market)
                    newdbtool.add_sp_adGroup_negativeKeyword(market, None, adGroupId, campaign_id, None,"NEGATIVE_EXACT",
                                                             "ENABLED", searchTerm, "failed",datetime.now()
                                                             ,None ,None )


    def update_sp_ad_sku(self,market, path):
        uploaded_file = path
        if os.stat(uploaded_file).st_size > 2:
            df = pd.read_csv(uploaded_file)
            # 打印 DataFrame 的前几行，以确保成功读取
            print(df.head())
            # 将DataFrame转换为一个列表，每个元素是一个字典表示一行数据
            df_data = json.loads(df.to_json(orient='records'))
            print(df_data)
        #     df_data={
        # "data": [
        # {
        #   "market": "FR",
        #   "sum( sales )": 60.62,
        #   "sum( cost )": 0.38,
        #   "Budget": 8,
        #   "campaignId": 323158262143023,
        #   "campaignName": "DeepBI_0509_m19",
        #   "adGroupId": 438024412988838,
        #   "adGroupName": "DeepBI_0509_m19",
        #   "placementClassification": "Top of Search on-Amazon",
        #   "new_campaignName": "DeepBI_0514_k01_ASIN",
        #   "new_adGroupName": "DeepBI_0514_k01_ASIN",
        #   "new_Budget": 10
        # }
        # ]
        # }
            api = Gen_product(self.brand)
            for item in df_data:
                adId = item["adId"]
                api.update_product(market, str(adId), state="PAUSED")


    def update_sp_ad_keyword(self,market,path):
        uploaded_file = path
        if os.stat(uploaded_file).st_size > 2:
            df = pd.read_csv(uploaded_file)
            # 打印 DataFrame 的前几行，以确保成功读取
            print(df.head())
            # 将DataFrame转换为一个列表，每个元素是一个字典表示一行数据
            df_data = json.loads(df.to_json(orient='records'))
            print(df_data)
        #     df_data={
        # "data": [
        # {
        #   "market": "FR",
        #   "sum( sales )": 60.62,
        #   "sum( cost )": 0.38,
        #   "Budget": 8,
        #   "campaignId": 323158262143023,
        #   "campaignName": "DeepBI_0509_m19",
        #   "adGroupId": 438024412988838,
        #   "adGroupName": "DeepBI_0509_m19",
        #   "placementClassification": "Top of Search on-Amazon",
        #   "new_campaignName": "DeepBI_0514_k01_ASIN",
        #   "new_adGroupName": "DeepBI_0514_k01_ASIN",
        #   "new_Budget": 10
        # }
        # ]
        # }
            api = Gen_keyword(self.brand)
            for item in df_data:
                keywordId = item["keywordId"]
                keywordBid = item["keywordBid"]
                new_keywordBid = item["new_keywordBid"]
                if new_keywordBid == "关闭":
                    api.update_keyword_toadGroup(market, str(keywordId), keywordBid, bid_new=None, state="PAUSED")
                else:
                    api.update_keyword_toadGroup(market, str(keywordId), keywordBid, round(float(new_keywordBid), 2), state="ENABLED")

    def rollback_sp_ad_keyword(self,market,path):
        uploaded_file = path
        df = pd.read_csv(uploaded_file)
        # 打印 DataFrame 的前几行，以确保成功读取
        print(df.head())
        # 将DataFrame转换为一个列表，每个元素是一个字典表示一行数据
        df_data = json.loads(df.to_json(orient='records'))
        print(df_data)

        api = Gen_keyword(self.brand)
        for item in df_data:
            keywordId = item["keywordId"]
            keywordBid = item["keywordBid"]
            new_keywordBid = item["new_keywordBid"]
            if new_keywordBid == "关闭":
                api.update_keyword_toadGroup(market, str(keywordId), keywordBid, bid_new=None, state="ENABLED")
            else:
                api.update_keyword_toadGroup(market, str(keywordId), new_keywordBid, round(float(keywordBid), 2), state="ENABLED")


    def update_sp_ad_automatic_targeting(self,market, path):
        uploaded_file = path
        if os.stat(uploaded_file).st_size > 2:
            df = pd.read_csv(uploaded_file)
            # 打印 DataFrame 的前几行，以确保成功读取
            print(df.head())
            # 将DataFrame转换为一个列表，每个元素是一个字典表示一行数据
            df_data = json.loads(df.to_json(orient='records'))
            print(df_data)
        #     df_data={
        # "data": [
        # {
        #   "market": "FR",
        #   "sum( sales )": 60.62,
        #   "sum( cost )": 0.38,
        #   "Budget": 8,
        #   "campaignId": 323158262143023,
        #   "campaignName": "DeepBI_0509_m19",
        #   "adGroupId": 438024412988838,
        #   "adGroupName": "DeepBI_0509_m19",
        #   "placementClassification": "Top of Search on-Amazon",
        #   "new_campaignName": "DeepBI_0514_k01_ASIN",
        #   "new_adGroupName": "DeepBI_0514_k01_ASIN",
        #   "new_Budget": 10
        # }
        # ]
        # }
            api1 = Gen_adgroup(self.brand)
            for item in df_data:
                keywordId = item["keywordId"]
                new_Budget = item["New_keywordBid"]
                if new_Budget == '关闭':
                    api1.update_adGroup_TargetingClause(market, str(keywordId), bid=None, state="PAUSED")
                else:
                    try:
                        api1.update_adGroup_TargetingClause(market, str(keywordId), float(new_Budget), state="ENABLED")
                    except Exception as e:
                        print(e)

    def rollback_sp_ad_automatic_targeting(self,market, path):
        uploaded_file = path
        df = pd.read_csv(uploaded_file)
        # 打印 DataFrame 的前几行，以确保成功读取
        print(df.head())
        # 将DataFrame转换为一个列表，每个元素是一个字典表示一行数据
        df_data = json.loads(df.to_json(orient='records'))
        print(df_data)

        api1 = Gen_adgroup(self.brand)
        for item in df_data:
            keywordId = item["keywordId"]
            keywordBid = item["keywordBid"]
            new_Budget = item["New_keywordBid"]
            if new_Budget == '关闭':
                api1.update_adGroup_TargetingClause(market, str(keywordId), bid=None, state="ENABLED")
            else:
                try:
                    api1.update_adGroup_TargetingClause(market, str(keywordId), float(keywordBid), state="ENABLED")
                except Exception as e:
                    print(e)

    def add_sp_ad_searchTerm_product(self,market, path):
        uploaded_file = path
        if os.stat(uploaded_file).st_size > 2:
            df = pd.read_csv(uploaded_file)
            # 打印 DataFrame 的前几行，以确保成功读取
            print(df.head())
            # 将DataFrame转换为一个列表，每个元素是一个字典表示一行数据
            df_data = json.loads(df.to_json(orient='records'))
            print(df_data)
        #     df_data={
        # "data": [
        # {
        #   "market": "FR",
        #   "sum( sales )": 60.62,
        #   "sum( cost )": 0.38,
        #   "Budget": 8,
        #   "campaignId": 323158262143023,
        #   "campaignName": "DeepBI_0509_m19",
        #   "adGroupId": 438024412988838,
        #   "adGroupName": "DeepBI_0509_m19",
        #   "placementClassification": "Top of Search on-Amazon",
        #   "new_campaignName": "DeepBI_0514_k01_ASIN",
        #   "new_adGroupName": "DeepBI_0514_k01_ASIN",
        #   "new_Budget": 10
        # }
        # ]
        # }
            apitool1 = AdGroupTools(self.brand)
            api1 = Gen_adgroup(self.brand)
            for item in df_data:
                campaignId = item["campaignId"]
                adGroupId = item["adGroupId"]
                searchTerm = item["searchTerm"]

                try:
                    bid_info = apitool1.list_product_bid_recommendations(market, searchTerm.upper(), campaignId,
                                                                         adGroupId)
                    bid = bid_info['bidRecommendations'][0]['bidRecommendationsForTargetingExpressions'][0]['bidValues'][1]['suggestedBid']
                except Exception as e:
                    print(e)
                    bid = 0.25
                try:
                    api1.create_adGroup_Targeting1(market,str(campaignId),str(adGroupId),searchTerm.upper(),bid,state='ENABLED',type='ASIN_SAME_AS')
                except Exception as e:
                    print(e)

    def rollback_sp_ad_searchTerm_product(self,market, path):
        uploaded_file = path
        df = pd.read_csv(uploaded_file)
        # 打印 DataFrame 的前几行，以确保成功读取
        print(df.head())
        # 将DataFrame转换为一个列表，每个元素是一个字典表示一行数据
        df_data = json.loads(df.to_json(orient='records'))
        print(df_data)

        apitool1 = AdGroupTools(self.brand)
        api1 = Gen_adgroup(self.brand)
        for item in df_data:
            campaignId = item["campaignId"]
            adGroupId = item["adGroupId"]
            searchTerm = item["searchTerm"]

            try:
                bid_info = apitool1.list_product_bid_recommendations(market, searchTerm.upper(), campaignId,
                                                                     adGroupId)
                bid = bid_info['bidRecommendations'][0]['bidRecommendationsForTargetingExpressions'][0]['bidValues'][1]['suggestedBid']
            except Exception as e:
                print(e)
                bid = 0.25
            try:
                api1.create_adGroup_Targeting1(market,str(campaignId),str(adGroupId),searchTerm.upper(),bid,state='ENABLED',type='ASIN_SAME_AS')
            except Exception as e:
                print(e)

    #update_sp_ad_budget("FR")
    #update_sp_ad_keyword("ES")
    # uploaded_file = 'C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/输出结果/ES_2024-06-24\手动_优质广告位_ES_2024-06-24.csv'
    # print(os.stat(uploaded_file).st_size)
# api = auto_api('OutdoorMaster')
# api.rollback_sp_ad_searchTerm_keyword('DE',['98694428174471', '235632184898928', '82611585277597', '197505898890190', '27892349377300', '173708856986455', '156290370049881', '115943468738805', '80969330872440', '273584015820247', '4707999201012', '91228569407766', '199569187028116', '86227092372582', '196562546708270', '262090903801008', '256666750027965', '48242430512073', '194062216290235', '199210132788508', '280096551728131', '48987416951606', '180866350789834', '225504613026493', '199235330362926', '269725413222860', '184222370478425', '224368632264771', '212095386428038', '62894745139789', '272483772022567', '115637986579270', '226234224805795', '209848410125868', '126380133602257', '110375678390076', '7386080990766', '52134079001755', '120263466132582', '222952891314074', '205612186594848', '189672983991880', '59119078816256', '64467587323075', '28005088860423', '169535484633371', '64043340510462', '271105875817752', '91724675853062', '21329495367415', '131936188495829', '250339366407242', '105940447998131', '61827564814472', '133035608512237', '264474197787186', '260415676683511', '253505778683612', '95267088972418', '123618050600083', '3081019739246', '229944911534221', '52253705496125', '211700652904429', '24988539656572', '177625402110703', '1387854541739', '249730595876456', '137923753681324', '140287522969659', '25293691326378', '232625997657670', '247345830589136', '5351344713407', '184808326192684', '113330321484403', '67384059246723', '209525084859596', '104133027559026', '122823482134714', '123383548178104', '79394054523376', '276659263435403', '76634256365774', '5529952723426', '123406955110334', '246706859985397', '14073366714031', '173783208301402', '14233700823585', '249286298886990', '274998302662800', '224607265689994', '239045032597718', '201410429332206', '211838233232070', '222334444746805', '221883553948726', '42025527653264', '102661469934058', '193954632512135', '146744988702657', '182603303265239', '94741112117518', '113976590175433', '274183796144592', '255752090675270', '28338610242562', '26098395155842', '223096100056881', '54405649984981', '224485685489153', '82917882554822', '164966650067820', '281324084506988', '82758682016034', '115769457290866', '168693093362161', '111494738831054', '137135115610888', '8987586338373', '205011196864093', '274214108637796', '172911859553662', '16150416043612', '140680636154913', '167749773225305', '176550264493190', '180423825615587', '54405941749537', '255007238463950', '54665788616691', '11761867635125', '144571446312612', '216793592816743', '171416247949554', '263500255477042', '224043973253943', '118504552280047', '59392864513337', '235247511708350', '88829843649805', '196967737589004', '11147909204544', '148415623352273', '117502761184185', '84150139297086', '49755577405266', '227908663907187', '101112270460884', '10931469212044', '143622813909001', '259955054073118', '108732898594547', '189865706306068', '217680080071410', '62483581436439', '262099491089146', '505569453981', '154461691091740', '172921060861047', '209849686020697', '169282342951050', '178048470220475', '224298980153601', '279521757196201', '209100644368374', '175544470352430', '91604718415872', '27386287524733', '42536746610092', '175274190402947', '36497171058105', '76301427512620', '163113070312483', '215781445226134', '32862590991043', '98117797827240', '83089200099563', '41258310190828', '142419708324668', '169509373678602', '21956157508912', '35824443258678', '178539014555592', '243183751650251', '44663021963827', '246168573527868', '144277837677165', '256952412633530', '97378015007150', '127255145976254', '34058459719195', '18790786400564', '264235327277613', '95850449835891', '212884078250446', '69998444838550', '155957647916330', '199789079164670', '150013511455464', '184291440814106', '192766173541504', '226626048855659', '250199553782043', '268925155199296', '69809131281395', '18874241742119', '177782587970088', '252898184065278', '120990359880978', '9782419966129', '200062453750166', '99909794249068', '87637907904579', '111151961797787', '88696940876447', '177592573699846', '236400596140707', '113701091681961'])
