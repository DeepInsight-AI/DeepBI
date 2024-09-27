import asyncio
import json

from ai.backend.util.db.auto_process.tools_sd_adGroup import AdGroupTools_SD
from ai.backend.util.db.auto_process.tools_db_sp import DbSpTools
from ai.backend.util.db.auto_process.tools_db_new_sp import DbNewSpTools
from datetime import datetime
from ai.backend.util.db.db_amazon.generate_tools import ask_question


class Gen_adgroup(AdGroupTools_SD):
    def __init__(self, db, brand, market):
        super().__init__(db, brand, market)
# 创建广告组

    def create_adgroup(self,campaignId,name,bidOptimization,creativeType,state,defaultBid,user='test'):
        adgroup_info = [
      {
        "name": name,
        "campaignId": campaignId,
        "defaultBid": defaultBid,
        "bidOptimization": bidOptimization,
        "state": state,
        "creativeType": creativeType
      }
    ]
        res = self.create_adGroup_api(adgroup_info)
        # 根据结果更新log
            #def create_sp_adgroups(self,market,campaignId,adGroupName,adGroupId,state,defaultBid,adGroupState,update_time):
        dbNewTools = DbNewSpTools(self.db, self.brand,self.market)
        if res[0] == "success":
            dbNewTools.create_sp_adgroups(self.market,campaignId,name,res[1],state,defaultBid,"success",datetime.now(),creativeType,"SD",user)
        else:
            dbNewTools.create_sp_adgroups(self.market,campaignId,name,res[1],state,defaultBid,"failed",datetime.now(),creativeType,"SD",user)
        return res[1]
    # 新建测试
    # res = create_adgroup('US','513987903939456','20240507test','PAUSED',2)
    # print(res)

    # 更新广告组v0 简单参数
    def update_adgroup_v0(self,adGroupName,adGroupId,state,defaultBid_new):
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
        apires = self.update_adGroup_api(adgroupInfo)
        # 记录
        #      def update_sp_adgroups(self,market,adGroupName,adGroupId,bid_old,bid_new,standards_acos,acos,beizhu,status,update_time):
        newdbtool = DbNewSpTools(self.db, self.brand,self.market)
        if apires[0] == "success":
            print("api update success")
            newdbtool.update_sp_adgroups(self.market, adGroupName, adGroupId, None,defaultBid_new,None, None, None, "success", datetime.now())
        else:
            print("api update failed")
            newdbtool.update_sp_adgroups(self.market, adGroupName, adGroupId, None, defaultBid_new, None, None, None, "failed",
                                         datetime.now())

    # 测试
    # update_adgroup_v0('US','adgroupB09ZQLY99J','311043566627712','PAUSED',4.00)


    # 更新广告组
    def update_batch_adgroup(self,market,startdate,enddate,start_acos,end_acos,adjuest):
        '''1.先查找需要更新的adgroup
                2.将需要更新的数据插入到log表记录
                3.开始逐条api更新
                4.更新log表states记录更新状态'''

        apitool = AdGroupTools_SD()
        newdbtool = DbNewSpTools()

        # 1.查找广告组
        dst = DbSpTools()
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

    def create_adGroup_negative_targeting(self,adGroupId,asin,expressionType,state):
        #
        adGroup_negative_keyword_info = [
  {
    "state": state,
    "adGroupId": adGroupId,
    "expression": [
      {
        "type": "asinSameAs",
        "value": asin
      }
    ],
    "expressionType": expressionType
  }
]
        # api更新
        apires = self.create_adGroup_negative_targeting1(adGroup_negative_keyword_info)
        # 结果写入日志
        #  def add_sp_adGroup_negativeKeyword(self, market, adGroupName, adGroupId, campaignId, campaignName, matchType,
        #                                         keyword_state, keywordText, campaignNegativeKeywordId, operation_state,
        #                                         update_time):
        newdbtool = DbNewSpTools(self.db, self.brand,self.market)
        expression = f"asin={asin}"
        if apires[0] == "success":
            newdbtool.add_sd_adGroup_Targeting(self.market,adGroupId,None,expressionType,state,expression,"SD-negative","success",datetime.now())
        else:
            newdbtool.add_sd_adGroup_Targeting(self.market,adGroupId,None,expressionType,state,expression,"SD-negative","failed",datetime.now())
        return apires[1]


    # 给广告组更新否定关键词
    def update_adGroup_negative_keyword(self,adGroupNegativeKeywordId,keyword_state):
        adGroup_negativekw_info = {
        "negativeKeywords": [
        {
        "keywordId": adGroupNegativeKeywordId,
        "state": keyword_state
        }
        ]
        }
        # api更新
        apires = self.update_adGroup_negativekw(adGroup_negativekw_info)
        # 结果写入日志
        # def update_sp_adGroup_negativeKeyword(self, market, keyword_state, keywordText, campaignNegativeKeywordId,
        #                                            operation_state, update_time):
        newdbtool = DbNewSpTools(self.db, self.brand,self.market)
        if apires[0]=="success":
            newdbtool.update_sp_adGroup_negativeKeyword(self.market,keyword_state,None,adGroupNegativeKeywordId,"success",datetime.now())
        else:
            newdbtool.update_sp_adGroup_negativeKeyword(self.market,keyword_state,None,adGroupNegativeKeywordId,"failed",datetime.now())

    def list_adGroup_negative_product(self,adGroupId):
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
        res = self.list_adGroup_negative_pd(campaigninfo)

        # 根据创建结果更新log
        # dbNewTools = DbNewSpTools()
        # if res[0] == "success":
        #     dbNewTools.create_sp_campaigin(market,portfolioId,endDate,name,res[1],targetingType,state,startDate,budgetType,budget,"success",datetime.now())
        # else:
        #     dbNewTools.create_sp_campaigin(market,portfolioId,endDate,name,res[1],targetingType,state,startDate,budgetType,budget,"failed",datetime.now())

        return print(res[1])
    #list_adGroup_negative_product(29481039123844,'UK')

    def create_adGroup_Targeting1(self,adGroupId,expression_type,state,bid,user='test'):

        adGroup_info = [
      {
        "expression":[
       {
           "type": "similarProduct",
       }
      ],
        "bid": bid,
        "adGroupId": adGroupId,
        "expressionType": expression_type,
        "state": state
      }
    ]
        # api更新
        apires = self.create_adGroup_Targeting(adGroup_info)
        # 结果写入日志
        newdbtool = DbNewSpTools(self.db, self.brand,self.market)
        if apires[0]=="success":
            newdbtool.add_sd_adGroup_Targeting(self.market,adGroupId,bid,expression_type,state,"similarProduct","SD","success",datetime.now(),apires[1],user)
        else:
            newdbtool.add_sd_adGroup_Targeting(self.market,adGroupId,bid,expression_type,state,"similarProduct","SD","failed",datetime.now(),None,user)
        return apires[1]

    def create_adGroup_Targeting2(self,adGroupId,categoryid,brand_id,expression_type,state,bid, user='test'):
        # if market == 'US':
        #     brand_id = '15062523011'
        # elif market == 'FR':
        #     brand_id = '19768705031'
        # elif market == 'NL':
        #     brand_id = None
        # elif market == 'ES':
        #     brand_id = '11337706031'
        # elif market == 'IT':
        #     brand_id = '19758267031'

        adGroup_info = [
      {
        "expression":[
            {
                "type": "asinCategorySameAs",
                "value": str(categoryid)
            },
            {'type': 'asinBrandSameAs',
             'value': brand_id
             }
        ],
        "bid": bid,
        "adGroupId": adGroupId,
        "expressionType": expression_type,
        "state": state
      }
    ]
        # api更新
        apires = self.create_adGroup_Targeting(adGroup_info)
        # 结果写入日志
        expression = f"Category={categoryid},brand={brand_id}"
        newdbtool = DbNewSpTools(self.db, self.brand,self.market)
        if apires[0] == "success":
            newdbtool.add_sd_adGroup_Targeting(self.market, adGroupId, bid, expression_type, state, expression, "SD",
                                               "success", datetime.now(),apires[1], user)
        else:
            newdbtool.add_sd_adGroup_Targeting(self.market, adGroupId, bid, expression_type, state, expression, "SD",
                                               "failed", datetime.now(),None, user)
        return apires[1]

    def create_adGroup_Targeting3(self,adGroupId,asin,expression_type,state,bid, user='test'):

        adGroup_info = [
      {
        "expression":[
            {
                "type": "asinSameAs",
                "value": str(asin)
            }
      ],
        "bid": bid,
        "adGroupId": adGroupId,
        "expressionType": expression_type,
        "state": state
      }
    ]
        # api更新
        apires = self.create_adGroup_Targeting(adGroup_info)
        # 结果写入日志
        expression = f"asin={asin}"
        newdbtool = DbNewSpTools(self.db, self.brand,self.market)
        if apires[0]=="success":
            newdbtool.add_sd_adGroup_Targeting(self.market,adGroupId,bid,expression_type,state,expression,"SD","success",datetime.now(),apires[1], user)
        else:
            newdbtool.add_sd_adGroup_Targeting(self.market,adGroupId,bid,expression_type,state,expression,"SD","failed",datetime.now(),None, user)
        return apires[1]

    def create_adGroup_Targeting4(self,adGroupId,expression,expression_type,state,bid, user='test'):

        adGroup_info = [
      {
        "expression": expression,
        "bid": bid,
        "adGroupId": adGroupId,
        "expressionType": expression_type,
        "state": state
      }
    ]
        # api更新
        apires = self.create_adGroup_Targeting(adGroup_info)
        # 结果写入日志
        newdbtool = DbNewSpTools(self.db, self.brand,self.market)
        if apires[0]=="success":
            newdbtool.add_sd_adGroup_Targeting(self.market,adGroupId,bid,expression_type,state,json.dumps(expression),"SD","success",datetime.now(),apires[1], user)
        else:
            newdbtool.add_sd_adGroup_Targeting(self.market,adGroupId,bid,expression_type,state,json.dumps(expression),"SD","failed",datetime.now(),None, user)
        return apires[1]

    def update_adGroup_Targeting(self,target_id,bid,state, user='test'):

        adGroup_info = [
  {
    "state": state,
    "bid": bid,
    "targetId": target_id
  }
]
        # api更新
        apires = self.update_adGroup_Targeting1(adGroup_info)
        # 结果写入日志
        newdbtool = DbNewSpTools(self.db, self.brand,self.market)
        if apires[0] == "success":
            newdbtool.update_sd_adGroup_Targeting(self.market, None,None, bid, state, target_id, "SD",
                                                  "success", datetime.now(), user)
            return apires[1]
        else:
            newdbtool.update_sd_adGroup_Targeting(self.market, None,None, bid, state, target_id, "SD",
                                                  "failed", datetime.now(), user)
            return None

# Gen_adgroup('LAPASA').create_adGroup_negative_targeting('DE',295184860814336,'B0D5LN4TZC','manual', 'enabled')
if __name__ == "__main__":
    Gen_adgroup("Gotoly").update_adGroup_Targeting("US","500110736054193",0.92,"enabled")
