import asyncio

from ai.backend.util.db.auto_process.tools_sp_adGroup import AdGroupTools
from ai.backend.util.db.auto_process.tools_db_sp import DbSpTools
from ai.backend.util.db.auto_process.tools_db_new_sp import DbNewSpTools
from datetime import datetime
from ai.backend.util.db.db_amazon.generate_tools import ask_question


db_info = {'host': '****', 'user': '****', 'passwd': '****', 'port': 3306,
               'db': '****',
               'charset': 'utf8mb4', 'use_unicode': True, }

class Gen_adgroup:
    def __init__(self,brand):
        self.brand = brand
# 创建广告组
    def create_adgroup(self,market,campaignId,name,defaultBid,state):
        adgroup_info = {
            "adGroups": [
                {
                    "campaignId": campaignId,
                    "name": name,
                    "state": state,
                    "defaultBid": defaultBid
                }
            ]
        }
        apitool = AdGroupTools(self.brand)
        res = apitool.create_adGroup_api(adgroup_info,market)
        # 根据结果更新log
        #     def create_sp_adgroups(self,market,campaignId,adGroupName,adGroupId,state,defaultBid,adGroupState,update_time):
        dbNewTools = DbNewSpTools(self.brand)
        if res[0]=="success":
            dbNewTools.create_sp_adgroups(market,campaignId,name,res[1],state,defaultBid,"success",datetime.now(),None,"SP")
        else:
            dbNewTools.create_sp_adgroups(market,campaignId,name,res[1],state,defaultBid,"failed",datetime.now(),None,"SP")
        return res[1]
    # 新建测试
    # res = create_adgroup('US','513987903939456','20240507test','PAUSED',2)
    # print(res)

    # 更新广告组v0 简单参数
    def update_adgroup_v0(self,market,adGroupName,adGroupId,state,defaultBid_new):
        adgroupInfo = {
            "adGroups": [
                {
                    "name": adGroupName,
                    "state": state,
                    "adGroupId": adGroupId,
                    "defaultBid": defaultBid_new
                }
            ]
        }
        # 执行更新
        apitool = AdGroupTools(self.brand)
        apires = apitool.update_adGroup_api(adgroupInfo)
        # 记录
        #      def update_sp_adgroups(self,market,adGroupName,adGroupId,bid_old,bid_new,standards_acos,acos,beizhu,status,update_time):
        newdbtool = DbNewSpTools(self.brand)
        if apires[0] == "success":
            print("api update success")
            newdbtool.update_sp_adgroups(market, adGroupName, adGroupId, None,defaultBid_new,None, None, None, "success", datetime.now())
        else:
            print("api update failed")
            newdbtool.update_sp_adgroups(market, adGroupName, adGroupId, None, defaultBid_new, None, None, None, "failed",
                                         datetime.now())

    # 测试
    # update_adgroup_v0('US','adgroupB09ZQLY99J','311043566627712','PAUSED',4.00)


    # 更新广告组
    def update_batch_adgroup(self,market,startdate,enddate,start_acos,end_acos,adjuest):
        '''1.先查找需要更新的adgroup
                2.将需要更新的数据插入到log表记录
                3.开始逐条api更新
                4.更新log表states记录更新状态'''

        apitool = AdGroupTools()
        newdbtool = DbNewSpTools()

        # 1.查找广告组
        dst = DbSpTools(db_info)
        res = dst.get_sp_adgroup_update(market, startdate, enddate, start_acos, end_acos, adjuest)
        print(type(res))
        for i in range(len(res)):
            row = res.iloc[i]
            print(row)
            # 接下来更新操作
            # 新增：adgroup的bid去api获取再去更新 20240506
            adGroupId = row['adGroupId']
            defaultBid_old = apitool.get_adGroup_api(market,adGroupId)
            defaultBid_new = defaultBid_old*(1+adjuest)
            #
            adgroupInfo = {
                "adGroups": [
                    {
                        "name": row['adGroupName'],
                        "state": "ENABLED",
                        "adGroupId": row['adGroupId'],
                        "defaultBid": defaultBid_new
                    }
                ]
            }
            apires = apitool.update_adGroup_api(adgroupInfo)
            if apires[0]=="success":
                print("api update success")
                newdbtool.update_sp_adgroups(row['market'],row['adGroupName'],row['adGroupId'],defaultBid_old,defaultBid_new,
                                             row['standards_acos'],row['acos'],adjuest,"success",datetime.now())
            else:
                print("api update failed")
                newdbtool.update_sp_adgroups(row['market'], row['adGroupName'], row['adGroupId'], defaultBid_old,
                                             defaultBid_new,
                                             row['standards_acos'], row['acos'], adjuest, "failed", datetime.now())


    def add_adGroup_negative_keyword(self,market,campaignId,adGroupId,keywordText,matchType,state):
        translate_kw = asyncio.get_event_loop().run_until_complete(ask_question(keywordText, market))
        keywordText_new = eval(translate_kw)[0]
        # keywordText_new = keywordText
        #
        adGroup_negative_keyword_info = {
      "negativeKeywords": [
        {
          "campaignId": str(campaignId),
          "matchType": matchType,
          "state": state,
          "adGroupId": str(adGroupId),
          "keywordText": keywordText_new
        }
      ]
    }
        # api更新
        apitool = AdGroupTools(self.brand)
        apires = apitool.add_adGroup_negativekw(adGroup_negative_keyword_info,market)
        # 结果写入日志
        #  def add_sp_adGroup_negativeKeyword(self, market, adGroupName, adGroupId, campaignId, campaignName, matchType,
        #                                         keyword_state, keywordText, campaignNegativeKeywordId, operation_state,
        #                                         update_time):
        newdbtool = DbNewSpTools(self.brand)
        if apires[0] == "success":
            newdbtool.add_sp_adGroup_negativeKeyword(market, None, adGroupId, campaignId, None, matchType, state,keywordText, "success", datetime.now(),apires[1],keywordText_new)
        else:
            newdbtool.add_sp_adGroup_negativeKeyword(market, None, adGroupId, campaignId, None, matchType, state,keywordText, "success", datetime.now(),None,keywordText_new)
        return apires[1]


    def add_adGroup_negative_keyword_v0(self,market,campaignId,adGroupId,keywordText,matchType,state):

        adGroup_negative_keyword_info = {
      "negativeKeywords": [
        {
          "campaignId": str(campaignId),
          "matchType": matchType,
          "state": state,
          "adGroupId": str(adGroupId),
          "keywordText": keywordText
        }
      ]
    }
        # api更新
        apitool = AdGroupTools(self.brand)
        apires = apitool.add_adGroup_negativekw(adGroup_negative_keyword_info,market)
        # 结果写入日志
        #  def add_sp_adGroup_negativeKeyword(self, market, adGroupName, adGroupId, campaignId, campaignName, matchType,
        #                                         keyword_state, keywordText, campaignNegativeKeywordId, operation_state,
        #                                         update_time):
        newdbtool = DbNewSpTools(self.brand)
        if apires[0] == "success":
            newdbtool.add_sp_adGroup_negativeKeyword(market, None, adGroupId, campaignId, None, matchType, state,keywordText, "success", datetime.now(),apires[1],keywordText)
        else:
            newdbtool.add_sp_adGroup_negativeKeyword(market, None, adGroupId, campaignId, None, matchType, state,keywordText, "success", datetime.now(),apires[1],keywordText)
        return apires[1]

    # 给广告组更新否定关键词
    def update_adGroup_negative_keyword(self,market,adGroupNegativeKeywordId,keyword_state):
        adGroup_negativekw_info = {
        "negativeKeywords": [
        {
        "keywordId": adGroupNegativeKeywordId,
        "state": keyword_state
        }
        ]
        }
        # api更新
        apitool = AdGroupTools(self.brand)
        apires = apitool.update_adGroup_negativekw(adGroup_negativekw_info)
        # 结果写入日志
        # def update_sp_adGroup_negativeKeyword(self, market, keyword_state, keywordText, campaignNegativeKeywordId,
        #                                            operation_state, update_time):
        newdbtool = DbNewSpTools(self.brand)
        if apires[0]=="success":
            newdbtool.update_sp_adGroup_negativeKeyword(market,keyword_state,None,adGroupNegativeKeywordId,"success",datetime.now())
        else:
            newdbtool.update_sp_adGroup_negativeKeyword(market,keyword_state,None,adGroupNegativeKeywordId,"failed",datetime.now())

    def list_adGroup_negative_product(self,adGroupId,market):
        campaigninfo = {
    "maxResults": 0,
    "nextToken": None,
    "adGroupIdFilter": {
    "include": [
        str(adGroupId)
    ]
    },
    "includeExtendedDataFields": True
    }
        # 执行创建
        apitool = AdGroupTools(self.brand)
        res = apitool.list_adGroup_negative_pd(campaigninfo, market)

        # 根据创建结果更新log
        # dbNewTools = DbNewSpTools()
        # if res[0] == "success":
        #     dbNewTools.create_sp_campaigin(market,portfolioId,endDate,name,res[1],targetingType,state,startDate,budgetType,budget,"success",datetime.now())
        # else:
        #     dbNewTools.create_sp_campaigin(market,portfolioId,endDate,name,res[1],targetingType,state,startDate,budgetType,budget,"failed",datetime.now())

        return print(res[1])
    #list_adGroup_negative_product(29481039123844,'UK')
    def update_adGroup_TargetingClause(self,market,target_id,bid,state):
        adGroup_info = {
      "targetingClauses": [
        {
          "targetId": target_id,
          "state": state,
          "bid": bid
        }
      ]
    }
        # api更新
        apitool = AdGroupTools(self.brand)
        apires = apitool.update_adGroup_TargetingC(adGroup_info,market)
        # 结果写入日志
        newdbtool = DbNewSpTools(self.brand)
        if apires[0] == "success":
            newdbtool.update_sd_adGroup_Targeting(market, None, bid, state, target_id, "SP",
                                               "success", datetime.now())
            return apires[1]["targetId"]
        else:
            newdbtool.update_sd_adGroup_Targeting(market, None, bid, state, target_id, "SP",
                                               "failed", datetime.now())
            return None



    def create_adGroup_Targeting1(self,market,new_campaign_id,new_adgroup_id,asin,bid,state,type):
        adGroup_info = {
      "targetingClauses": [
        {
          "expression": [
            {
              "type": type,
              "value": asin
            }
          ],
          "campaignId": new_campaign_id,
          "expressionType": "MANUAL",
          "state": state,
          "bid": bid,
          "adGroupId": new_adgroup_id
        }
      ]
    }
        # api更新
        apitool = AdGroupTools(self.brand)
        apires = apitool.create_adGroup_TargetingC(adGroup_info,market)
        #结果写入日志
        newdbtool = DbNewSpTools(self.brand)
        if apires[0]=="success":
            newdbtool.add_sd_adGroup_Targeting(market, new_adgroup_id, bid, type, state, asin, "SP",
                                               "success", datetime.now())
        else:
            newdbtool.add_sd_adGroup_Targeting(market, new_adgroup_id, bid, type, state, asin, "SP",
                                               "failed", datetime.now())
        return apires[1]["targetId"]

    def create_adGroup_Targeting2(self,market,new_campaign_id,new_adgroup_id,bid,categories_id,brand_id):
        adGroup_info = {
      "targetingClauses": [
        {
          "expression": [
              {
                  "type": "ASIN_CATEGORY_SAME_AS",
                  "value": categories_id
              },
              {
                  "type": "ASIN_BRAND_SAME_AS",
                  "value": brand_id
              }
          ],
          "campaignId": new_campaign_id,
          "expressionType": "MANUAL",
          "state": "ENABLED",
          "bid": bid,
          "adGroupId": new_adgroup_id
        }
      ]
    }
        # api更新
        apitool = AdGroupTools(self.brand)
        apires = apitool.create_adGroup_TargetingC(adGroup_info,market)
        #结果写入日志
        newdbtool = DbNewSpTools(self.brand)
        expression = f"Category={categories_id},brand={brand_id}"
        if apires[0]=="success":
            newdbtool.add_sd_adGroup_Targeting(market, new_adgroup_id, bid, "MANUAL", "ENABLED", expression, "SP",
                                               "success", datetime.now())
        else:
            newdbtool.add_sd_adGroup_Targeting(market, new_adgroup_id, bid, "MANUAL", "ENABLED", expression, "SP",
                                               "failed", datetime.now())
        return apires[1]["targetId"]

    def create_adGroup_Negative_Targeting_by_asin(self,market,new_campaign_id,new_adgroup_id,asin):
        adGroup_info = {
  "negativeTargetingClauses": [
    {
      "expression": [
        {
          "type": "ASIN_SAME_AS",
          "value": asin
        }
      ],
      "campaignId": str(new_campaign_id),
      "state": "ENABLED",
      "adGroupId": str(new_adgroup_id)
    }
  ]
}
        # api更新
        apitool = AdGroupTools(self.brand)
        apires = apitool.create_adGroup_Negative_TargetingClauses(adGroup_info,market)
        #结果写入日志
        newdbtool = DbNewSpTools(self.brand)
        expression = f"asin={asin}"
        if apires[0]=="success":
            newdbtool.add_sd_adGroup_Targeting(market, new_adgroup_id, None, "Negative", "ENABLED", expression, "SP",
                                               "success", datetime.now())
        else:
            newdbtool.add_sd_adGroup_Targeting(market, new_adgroup_id, None, "Negative", "ENABLED", expression, "SP",
                                               "failed", datetime.now())
        return apires[1]["targetId"]

# ASIN = 'b00eea9zks'
# Gen_adgroup('LAPASA').create_adGroup_Negative_Targeting_by_asin('FR',370475500006660,516468538668241,ASIN.upper())

