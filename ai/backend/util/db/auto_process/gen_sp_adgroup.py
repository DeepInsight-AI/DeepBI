import asyncio

from ai.backend.util.db.auto_process.tools_sp_adGroup import AdGroupTools
from ai.backend.util.db.auto_process.tools_db_sp import DbSpTools
from ai.backend.util.db.auto_process.tools_db_new_sp import DbNewSpTools
from datetime import datetime
from ai.backend.util.db.db_amazon.generate_tools import ask_question


class Gen_adgroup(AdGroupTools):
    def __init__(self, db, brand, market):
        super().__init__(db, brand, market)

# 创建广告组
    def create_adgroup(self,campaignId,name,defaultBid,state,user='test'):
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
        res = self.create_adGroup_api(adgroup_info)
        # 根据结果更新log
        #     def create_sp_adgroups(self,market,campaignId,adGroupName,adGroupId,state,defaultBid,adGroupState,update_time):
        dbNewTools = DbNewSpTools(self.db, self.brand,self.market)
        if res[0]=="success":
            dbNewTools.create_sp_adgroups(self.market,campaignId,name,res[1],state,defaultBid,"success",datetime.now(),None,"SP",user)
        else:
            dbNewTools.create_sp_adgroups(self.market,campaignId,name,res[1],state,defaultBid,"failed",datetime.now(),None,"SP",user)
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


    def add_adGroup_negative_keyword(self,campaignId,adGroupId,keywordText,matchType,state):
        translate_kw = asyncio.get_event_loop().run_until_complete(ask_question(keywordText, self.market))
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
        apires = self.add_adGroup_negativekw(adGroup_negative_keyword_info)
        # 结果写入日志
        #  def add_sp_adGroup_negativeKeyword(self, market, adGroupName, adGroupId, campaignId, campaignName, matchType,
        #                                         keyword_state, keywordText, campaignNegativeKeywordId, operation_state,
        #                                         update_time):
        newdbtool = DbNewSpTools(self.db, self.brand,self.market)
        if apires[0] == "success":
            newdbtool.add_sp_adGroup_negativeKeyword(self.market, None, adGroupId, campaignId, None, matchType, state,keywordText, "success", datetime.now(),apires[1],keywordText_new)
        else:
            newdbtool.add_sp_adGroup_negativeKeyword(self.market, None, adGroupId, campaignId, None, matchType, state,keywordText, "success", datetime.now(),None,keywordText_new)
        return apires[1]


    def add_adGroup_negative_keyword_v0(self,campaignId,adGroupId,keywordText,matchType,state, user = 'test'):

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
        apires = self.add_adGroup_negativekw(adGroup_negative_keyword_info)
        # 结果写入日志
        #  def add_sp_adGroup_negativeKeyword(self, market, adGroupName, adGroupId, campaignId, campaignName, matchType,
        #                                         keyword_state, keywordText, campaignNegativeKeywordId, operation_state,
        #                                         update_time):
        newdbtool = DbNewSpTools(self.db, self.brand,self.market)
        if apires[0] == "success":
            newdbtool.add_sp_adGroup_negativeKeyword(self.market, None, adGroupId, campaignId, None, matchType, state,keywordText, "success", datetime.now(),apires[1],keywordText,user)
        else:
            newdbtool.add_sp_adGroup_negativeKeyword(self.market, None, adGroupId, campaignId, None, matchType, state,keywordText, "success", datetime.now(),apires[1],keywordText,user)
        return apires[1]

    def add_adGroup_negative_keyword_batch(self,info, user = 'test'):

        adGroup_negative_keyword_info = {
          "negativeKeywords": []
        }
        for item in info:
            adGroup_negative_keyword_info["negativeKeywords"].append(
                {
                    "campaignId": str(item['campaignId']),
                    "matchType": item['matchType'],
                    "state": "ENABLED",
                    "adGroupId": str(item['adGroupId']),
                    "keywordText": item['keywordText']
                }
            )
        # api更新
        res = self.add_adGroup_negativekw_batch(adGroup_negative_keyword_info)
        # 存储更新记录到数据库
        dbNewTools = DbNewSpTools(self.db, self.brand, self.market)

        # 获取成功的 index
        success_indices = {item['index']: item['negativeKeywordId'] for item in res['negativeKeywords']['success']}

        updates = []

        for idx, item in enumerate(info):
            # 检查当前的索引是否在成功的索引中
            if idx in success_indices:
                targeting_state = "success"
                target_id = success_indices[idx]
            else:
                targeting_state = "failed"
                target_id = None  # 或者设置为其他默认值

            updates.append({
                'market': self.market,
                'adGroupName': None,
                'adGroupId': item['adGroupId'],
                'campaignId': item['campaignId'],
                'campaignName': None,
                'matchType': item['matchType'],
                'keyword_state': "ENABLED",
                'keywordText': item['keywordText'],
                'operation_state': targeting_state,  # Assuming you have this value in `info`
                'update_time': datetime.now(),
                'campaignNegativeKeywordId': target_id,
                'keywordText_new': item['keywordText'],
                'user': user
            })

        # 批量插入到数据库
        dbNewTools.batch_add_sp_adGroup_negativeKeyword(updates)

    # 给广告组更新否定关键词
    def update_adGroup_negative_keyword(self,adGroupNegativeKeywordId,keyword_state,user='test'):
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
            newdbtool.update_sp_adGroup_negativeKeyword(self.market,keyword_state,None,adGroupNegativeKeywordId,"success",datetime.now(),user)
        else:
            newdbtool.update_sp_adGroup_negativeKeyword(self.market,keyword_state,None,adGroupNegativeKeywordId,"failed",datetime.now(),user)

    def delete_adGroup_negative_keyword(self, adGroupNegativeKeywordId, user='test'):
        info = self.to_iterable(adGroupNegativeKeywordId)
        adGroup_info = {
            "negativeKeywordIdFilter": {
                "include": []
            }
        }
        for item in info:
            adGroup_info["negativeKeywordIdFilter"]["include"].append(str(item))
        # api更新
        res = self.delete_adGroup_negativekw(adGroup_info)
        # 存储更新记录到数据库
        dbNewTools = DbNewSpTools(self.db, self.brand, self.market)
        # 获取成功的 index
        success_indices = {item['index']: item['negativeKeywordId'] for item in res['negativeKeywords']['success']}
        print(success_indices)
        updates = []
        for idx, item in enumerate(info):
            # 检查当前的索引是否在成功的索引中
            if idx in success_indices:
                targeting_state = "success"
                target_id = success_indices[idx]
            else:
                targeting_state = "failed"
                target_id = None  # 或者设置为其他默认值

            for item in info:
                updates.append({
                    'market': self.market,
                    'keyword_state': "ARCHIVED",
                    'keywordText': "Negative",
                    'campaignNegativeKeywordId': item,
                    'operation': "DELETE",
                    'operation_state': targeting_state,
                    'update_time': datetime.now(),
                    'user': user
                })
                # 批量插入到数据库
        dbNewTools.batch_update_sp_adGroup_negativeKeyword(updates)

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
    def update_adGroup_TargetingClause(self,target_id,bid,state, user='test'):
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
        apires = self.update_adGroup_TargetingC(adGroup_info)
        # 结果写入日志
        newdbtool = DbNewSpTools(self.db, self.brand,self.market)
        if apires[0] == "success":
            newdbtool.update_sd_adGroup_Targeting(self.market, None,None, bid, state, target_id, "SP",
                                               "success", datetime.now(), user)
            return apires[1]["targetId"]
        else:
            newdbtool.update_sd_adGroup_Targeting(self.market, None,None, bid, state, target_id, "SP",
                                               "failed", datetime.now(), user)
            return None

    def update_adGroup_TargetingClause_batch(self,info, user='test'):

        adGroup_info = {
            "targetingClauses": []
        }

        for item in info:
            adGroup_info["targetingClauses"].append({
                "targetId": str(item['keywordId']),
                "state": item['state'],
                "bid": item['bid_new']
            })
        print(adGroup_info)
        # 修改关键词操作
        res = self.update_adGroup_TargetingC_batch(adGroup_info)
        print(res)
        # 存储更新记录到数据库
        dbNewTools = DbNewSpTools(self.db, self.brand, self.market)
        # 获取成功的 index
        success_indices = {item['index']: item['targetId'] for item in res['targetingClauses']['success']}
        print(success_indices)
        updates = []
        for idx, item in enumerate(info):
            # 检查当前的索引是否在成功的索引中
            if idx in success_indices:
                targeting_state = "success"
                target_id = success_indices[idx]
            else:
                targeting_state = "failed"
                target_id = None  # 或者设置为其他默认值

            updates.append({
                'market': self.market,
                'adGroupId': None,
                'bid_old': item['bid'],
                'state': item['state'],
                'expression': item['keywordId'],  # Assuming you have this value in `info`
                'targetingType': 'SP',
                'targetingState': targeting_state,
                'update_time': datetime.now(),
                'user': user,
                'bid_new': item['bid_new']
            })
        # 批量插入到数据库
        dbNewTools.batch_update_adGroup_Targeting(updates)

    def create_adGroup_Targeting1(self,new_campaign_id,new_adgroup_id,asin,bid,state,type, user='test'):
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
        apires = self.create_adGroup_TargetingC(adGroup_info)
        #结果写入日志
        newdbtool = DbNewSpTools(self.db, self.brand,self.market)
        if apires[0]=="success":
            newdbtool.add_sd_adGroup_Targeting(self.market, new_adgroup_id, bid, type, state, asin, "SP",
                                               "success", datetime.now(),apires[1], user)
        else:
            newdbtool.add_sd_adGroup_Targeting(self.market, new_adgroup_id, bid, type, state, asin, "SP",
                                               "failed", datetime.now(),None, user)
        return apires[1]

    def create_adGroup_Targeting2(self,new_campaign_id,new_adgroup_id,bid,categories_id,brand_id, user='test'):
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
        apires = self.create_adGroup_TargetingC(adGroup_info)
        #结果写入日志
        newdbtool = DbNewSpTools(self.db, self.brand,self.market)
        expression = f"Category={categories_id},brand={brand_id}"
        if apires[0]=="success":
            newdbtool.add_sd_adGroup_Targeting(self.market, new_adgroup_id, bid, "MANUAL", "ENABLED", expression, "SP",
                                               "success", datetime.now(),apires[1], user)
        else:
            newdbtool.add_sd_adGroup_Targeting(self.market, new_adgroup_id, bid, "MANUAL", "ENABLED", expression, "SP",
                                               "failed", datetime.now(),None, user)
        return apires[1]

    def create_adGroup_Targeting_by_asin_batch(self,info,user='test'):
        adGroup_info = {
          "targetingClauses": []
        }
        for item in info:
            adGroup_info["targetingClauses"].append({
                    "expression": [
                        {
                            "type": str(item['type']),
                            "value": str(item['asin'])
                        }
                    ],
                    "campaignId": str(item['campaignId']),
                    "expressionType": "MANUAL",
                    "state": "ENABLED",
                    "bid": float(item['bid']),
                    "adGroupId": str(item['adGroupId'])
            })

        # api更新
        res = self.create_adGroup_TargetingC_batch(adGroup_info)
        #结果写入日志
        print(res)
        # 存储更新记录到数据库
        dbNewTools = DbNewSpTools(self.db, self.brand, self.market)

        # 获取成功的 index
        success_indices = {item['index']: item['targetId'] for item in res['targetingClauses']['success']}
        print(success_indices)
        updates = []
        for idx, item in enumerate(info):
            # 检查当前的索引是否在成功的索引中
            if idx in success_indices:
                targeting_state = "success"
                target_id = success_indices[idx]
            else:
                targeting_state = "failed"
                target_id = None  # 或者设置为其他默认值

            updates.append({
                'market': self.market,
                'adGroupId': item['adGroupId'],
                'bid': item['bid'],
                'expressionType': "MANUAL",
                'state': "ENABLED",
                'expression': f"{item['type']}={item['asin']}",  # Assuming you have this value in `info`
                'targetingType': "SP",
                'targetingState': targeting_state,
                'update_time': datetime.now(),
                'user': user,
                'targetId': target_id,
            })

        # 批量插入到数据库
        dbNewTools.batch_add_sd_adGroup_Targeting(updates)

    def create_adGroup_Negative_Targeting_by_asin(self,new_campaign_id,new_adgroup_id,asin,user='test'):
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
        apires = self.create_adGroup_Negative_TargetingClauses(adGroup_info)
        #结果写入日志
        newdbtool = DbNewSpTools(self.db, self.brand,self.market)
        expression = f"asin={asin}"
        if apires[0]=="success":
            newdbtool.add_sd_adGroup_Targeting(self.market, new_adgroup_id, None, "Negative", "ENABLED", expression, "SP",
                                               "success", datetime.now(),None,user)
            return apires[1]["targetId"]
        else:
            newdbtool.add_sd_adGroup_Targeting(self.market, new_adgroup_id, None, "Negative", "ENABLED", expression, "SP",
                                               "failed", datetime.now(),None,user)
            return None

    def create_adGroup_Negative_Targeting_by_asin_batch(self,info,user='test'):
        adGroup_info = {
          "negativeTargetingClauses": []
        }
        for item in info:
            adGroup_info["negativeTargetingClauses"].append({
                    "expression": [
                        {
                            "type": "ASIN_SAME_AS",
                            "value": str(item['asin'])
                        }
                    ],
                    "campaignId": str(item['campaignId']),
                    "state": "ENABLED",
                    "adGroupId": str(item['adGroupId'])
            })

        # api更新
        res = self.create_adGroup_Negative_TargetingClauses_batch(adGroup_info)
        #结果写入日志
        print(res)
        # 存储更新记录到数据库
        dbNewTools = DbNewSpTools(self.db, self.brand, self.market)

        # 获取成功的 index
        success_indices = {item['index']: item['targetId'] for item in res['negativeTargetingClauses']['success']}
        print(success_indices)
        updates = []
        for idx, item in enumerate(info):
            # 检查当前的索引是否在成功的索引中
            if idx in success_indices:
                targeting_state = "success"
                target_id = success_indices[idx]
            else:
                targeting_state = "failed"
                target_id = None  # 或者设置为其他默认值

            updates.append({
                'market': self.market,
                'adGroupId': item['adGroupId'],
                'bid': None,
                'expressionType': "Negative",
                'state': "ENABLED",
                'expression': f"asin={item['asin']}",  # Assuming you have this value in `info`
                'targetingType': "SP",
                'targetingState': targeting_state,
                'update_time': datetime.now(),
                'user': user,
                'targetId': target_id,
            })

        # 批量插入到数据库
        dbNewTools.batch_add_sd_adGroup_Targeting(updates)

    def update_adGroup_Negative_Targeting(self,targetId,state,user='test'):
        adGroup_info = {
          "negativeTargetingClauses": [
            {
              "targetId": str(targetId),
              "state": state
            }
          ]
        }
        # api更新
        apires = self.update_adGroup_Negative_TargetingClauses(adGroup_info)
        #结果写入日志
        newdbtool = DbNewSpTools(self.db, self.brand,self.market)
        if apires[0]=="success":
            newdbtool.update_sd_adGroup_Targeting(self.market, "Negative", None, state, state, targetId, "SP",
                                               "success", datetime.now(), user)
            return apires[1]
        else:
            newdbtool.update_sd_adGroup_Targeting(self.market, "Negative", None, state, state, targetId, "SP",
                                               "failed", datetime.now(), user)
            return None

    def delete_adGroup_Negative_Targeting(self,targetId,user='test'):
        info = self.to_iterable(targetId)
        adGroup_info = {
          "negativeTargetIdFilter": {
            "include": []
          }
        }
        for item in info:
            adGroup_info["negativeTargetIdFilter"]["include"].append(str(item))
        # api更新
        res = self.delete_adGroup_Negative_TargetingClauses(adGroup_info)
        # 存储更新记录到数据库
        dbNewTools = DbNewSpTools(self.db, self.brand, self.market)
        # 获取成功的 index
        success_indices = {item['index']: item['targetId'] for item in res['negativeTargetingClauses']['success']}
        print(success_indices)
        updates = []
        for idx, item in enumerate(info):
            # 检查当前的索引是否在成功的索引中
            if idx in success_indices:
                targeting_state = "success"
                target_id = success_indices[idx]
            else:
                targeting_state = "failed"
                target_id = None  # 或者设置为其他默认值

            for item in info:
                updates.append({
                    'market': self.market,
                    'adGroupId': "Negative",
                    'bid_old': None,
                    'state': "ARCHIVED",
                    'expression': item,  # Assuming you have this value in `info`
                    'targetingType': 'SP',
                    'targetingState': targeting_state,
                    'update_time': datetime.now(),
                    'user': user,
                    'bid_new': None
                })
        # 批量插入到数据库
        dbNewTools.batch_update_adGroup_Targeting(updates)

if __name__ == "__main__":
    # res = ['351388264320161', '365939904097078', '397574951723968', '510156903057793', '536626933665279', '386936079471516', '479623043802884', '415561823776403', '297463420656977', '427300641407372', '386771929703150', '413831898714768', '360183036925031', '301322349925964', '377149157951836', '444283893094892', '360497898301246', '319038378512978', '338104714081678', '445651338415553', '506528464131838', '323965893198020', '468773280390716', '295148153019184', '533767669358972', '289784634687833']
    # for adgroupid in res:
    #     Gen_adgroup('Gotoly').update_adgroup_v0('US',None,adgroupid,'ENABLED',None)
    Gen_adgroup('amazon_ads','LAPASA','UK').delete_adGroup_negative_keyword(155010353421654)
# ASIN = 'b00eea9zks'
# Gen_adgroup('syndesmos').update_adgroup_v0('DE',None,'361896893484449','ENABLED',0.25)
#Gen_adgroup('Veement').update_adgroup_v0('UK',None,'560403892575481','ENABLED',None)
# data = [{"adGroupId":"444068021520744","bid":0.39,"campaignId":"504483176255430","expression":[{"type":"QUERY_HIGH_REL_MATCHES"}],"expressionType":"AUTO","resolvedExpression":[{"type":"QUERY_HIGH_REL_MATCHES"}],"state":"PAUSED","targetId":"82267378953653"},{"adGroupId":"444068021520744","bid":0.32,"campaignId":"504483176255430","expression":[{"type":"QUERY_BROAD_REL_MATCHES"}],"expressionType":"AUTO","resolvedExpression":[{"type":"QUERY_BROAD_REL_MATCHES"}],"state":"PAUSED","targetId":"70245985914470"},{"adGroupId":"444068021520744","bid":0.19,"campaignId":"504483176255430","expression":[{"type":"ASIN_ACCESSORY_RELATED"}],"expressionType":"AUTO","resolvedExpression":[{"type":"ASIN_ACCESSORY_RELATED"}],"state":"PAUSED","targetId":"220623036199644"},{"adGroupId":"444068021520744","bid":0.19,"campaignId":"504483176255430","expression":[{"type":"ASIN_SUBSTITUTE_RELATED"}],"expressionType":"AUTO","resolvedExpression":[{"type":"ASIN_SUBSTITUTE_RELATED"}],"state":"ENABLED","targetId":"241043650862800"},{"adGroupId":"509540793397910","bid":0.62,"campaignId":"376349740275921","expression":[{"type":"ASIN_CATEGORY_SAME_AS","value":"1342939031"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_CATEGORY_SAME_AS","value":"Car On-Dash Mounted Cameras"}],"state":"ENABLED","targetId":"190681200374933"},{"adGroupId":"320672326843757","bid":0.39,"campaignId":"376349740275921","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B082WTG24X"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B082WTG24X"}],"state":"PAUSED","targetId":"149810000466307"},{"adGroupId":"320672326843757","bid":0.42,"campaignId":"376349740275921","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B0CFFGGNKX"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B0CFFGGNKX"}],"state":"PAUSED","targetId":"108421352425894"},{"adGroupId":"320672326843757","bid":0.78,"campaignId":"376349740275921","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B0CL454XB3"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B0CL454XB3"}],"state":"ENABLED","targetId":"197093916836215"},{"adGroupId":"320672326843757","bid":0.39,"campaignId":"376349740275921","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B0C33X6QBC"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B0C33X6QBC"}],"state":"PAUSED","targetId":"241472105290547"},{"adGroupId":"320672326843757","bid":0.39,"campaignId":"376349740275921","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B0C2Z1GF9N"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B0C2Z1GF9N"}],"state":"PAUSED","targetId":"143894609470991"},{"adGroupId":"320672326843757","bid":0.68,"campaignId":"376349740275921","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B0C9QH8W43"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B0C9QH8W43"}],"state":"ENABLED","targetId":"75842083001044"},{"adGroupId":"320672326843757","bid":0.68,"campaignId":"376349740275921","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B0C38HVQ4J"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B0C38HVQ4J"}],"state":"ENABLED","targetId":"104173223523571"},{"adGroupId":"320672326843757","bid":0.39,"campaignId":"376349740275921","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B0C9MDX92D"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B0C9MDX92D"}],"state":"PAUSED","targetId":"28679334934157"},{"adGroupId":"320672326843757","bid":0.39,"campaignId":"376349740275921","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B01C0B0TMK"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B01C0B0TMK"}],"state":"PAUSED","targetId":"146732898608816"},{"adGroupId":"531327088764683","bid":0.62,"campaignId":"413325751671700","expression":[{"type":"QUERY_HIGH_REL_MATCHES"}],"expressionType":"AUTO","resolvedExpression":[{"type":"QUERY_HIGH_REL_MATCHES"}],"state":"ENABLED","targetId":"9746156799596"},{"adGroupId":"531327088764683","bid":0.39,"campaignId":"413325751671700","expression":[{"type":"QUERY_BROAD_REL_MATCHES"}],"expressionType":"AUTO","resolvedExpression":[{"type":"QUERY_BROAD_REL_MATCHES"}],"state":"PAUSED","targetId":"238648534517743"},{"adGroupId":"531327088764683","bid":0.6,"campaignId":"413325751671700","expression":[{"type":"ASIN_ACCESSORY_RELATED"}],"expressionType":"AUTO","resolvedExpression":[{"type":"ASIN_ACCESSORY_RELATED"}],"state":"ENABLED","targetId":"101935842176917"},{"adGroupId":"531327088764683","bid":0.39,"campaignId":"413325751671700","expression":[{"type":"ASIN_SUBSTITUTE_RELATED"}],"expressionType":"AUTO","resolvedExpression":[{"type":"ASIN_SUBSTITUTE_RELATED"}],"state":"ENABLED","targetId":"7929404967384"},{"adGroupId":"486635292140914","bid":0.8,"campaignId":"340275257752503","expression":[{"type":"QUERY_HIGH_REL_MATCHES"}],"expressionType":"AUTO","resolvedExpression":[{"type":"QUERY_HIGH_REL_MATCHES"}],"state":"ENABLED","targetId":"15612073823400"},{"adGroupId":"486635292140914","bid":0.7,"campaignId":"340275257752503","expression":[{"type":"QUERY_BROAD_REL_MATCHES"}],"expressionType":"AUTO","resolvedExpression":[{"type":"QUERY_BROAD_REL_MATCHES"}],"state":"ENABLED","targetId":"99616252436502"},{"adGroupId":"486635292140914","bid":0.39,"campaignId":"340275257752503","expression":[{"type":"ASIN_ACCESSORY_RELATED"}],"expressionType":"AUTO","resolvedExpression":[{"type":"ASIN_ACCESSORY_RELATED"}],"state":"PAUSED","targetId":"203314783509256"},{"adGroupId":"486635292140914","bid":0.39,"campaignId":"340275257752503","expression":[{"type":"ASIN_SUBSTITUTE_RELATED"}],"expressionType":"AUTO","resolvedExpression":[{"type":"ASIN_SUBSTITUTE_RELATED"}],"state":"PAUSED","targetId":"100975732955037"},{"adGroupId":"407804080232137","bid":0.69,"campaignId":"317020672931766","expression":[{"type":"ASIN_CATEGORY_SAME_AS","value":"22222775031"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_CATEGORY_SAME_AS","value":"PlayStation 5 Headsets"}],"state":"ENABLED","targetId":"157092058472016"},{"adGroupId":"407804080232137","bid":0.65,"campaignId":"317020672931766","expression":[{"type":"ASIN_CATEGORY_SAME_AS","value":"13978377031"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_CATEGORY_SAME_AS","value":"PlayStation 4 Gaming Headsets"}],"state":"ENABLED","targetId":"127301329285241"},{"adGroupId":"407804080232137","bid":0.6,"campaignId":"317020672931766","expression":[{"type":"ASIN_CATEGORY_SAME_AS","value":"13978672031"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_CATEGORY_SAME_AS","value":"Xbox One Gaming Headsets"}],"state":"PAUSED","targetId":"79257493429795"},{"adGroupId":"407804080232137","bid":0.59,"campaignId":"317020672931766","expression":[{"type":"ASIN_CATEGORY_SAME_AS","value":"13978617031"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_CATEGORY_SAME_AS","value":"Xbox 360 Gaming Headsets"}],"state":"PAUSED","targetId":"128773622091968"},{"adGroupId":"407804080232137","bid":0.67,"campaignId":"317020672931766","expression":[{"type":"ASIN_CATEGORY_SAME_AS","value":"13978272031"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_CATEGORY_SAME_AS","value":"PC Gaming Headsets"}],"state":"ENABLED","targetId":"100720609154497"},{"adGroupId":"460482069085186","bid":0.1,"campaignId":"555082757479902","expression":[{"type":"QUERY_HIGH_REL_MATCHES"}],"expressionType":"AUTO","resolvedExpression":[{"type":"QUERY_HIGH_REL_MATCHES"}],"state":"ENABLED","targetId":"189510891010774"},{"adGroupId":"460482069085186","bid":0.2,"campaignId":"555082757479902","expression":[{"type":"QUERY_BROAD_REL_MATCHES"}],"expressionType":"AUTO","resolvedExpression":[{"type":"QUERY_BROAD_REL_MATCHES"}],"state":"ENABLED","targetId":"257777759561777"},{"adGroupId":"460482069085186","bid":0.2,"campaignId":"555082757479902","expression":[{"type":"ASIN_ACCESSORY_RELATED"}],"expressionType":"AUTO","resolvedExpression":[{"type":"ASIN_ACCESSORY_RELATED"}],"state":"ENABLED","targetId":"134918965407819"},{"adGroupId":"460482069085186","bid":0.1,"campaignId":"555082757479902","expression":[{"type":"ASIN_SUBSTITUTE_RELATED"}],"expressionType":"AUTO","resolvedExpression":[{"type":"ASIN_SUBSTITUTE_RELATED"}],"state":"ENABLED","targetId":"224398810703880"},{"adGroupId":"320672326843757","bid":0.39,"campaignId":"376349740275921","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B089CWVHZK"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B089CWVHZK"}],"state":"PAUSED","targetId":"17588911983080"},{"adGroupId":"401776764238472","bid":0.49,"campaignId":"339386037376295","expression":[{"type":"ASIN_CATEGORY_SAME_AS","value":"13978272031"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_CATEGORY_SAME_AS","value":"PC Gaming Headsets"}],"state":"ENABLED","targetId":"70856684431731"},{"adGroupId":"401776764238472","bid":0.59,"campaignId":"339386037376295","expression":[{"type":"ASIN_CATEGORY_SAME_AS","value":"13978377031"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_CATEGORY_SAME_AS","value":"PlayStation 4 Gaming Headsets"}],"state":"ENABLED","targetId":"61466710849620"},{"adGroupId":"366468148194339","bid":0.39,"campaignId":"435605480801197","expression":[{"type":"QUERY_HIGH_REL_MATCHES"}],"expressionType":"AUTO","resolvedExpression":[{"type":"QUERY_HIGH_REL_MATCHES"}],"state":"ENABLED","targetId":"123581958847616"},{"adGroupId":"366468148194339","bid":0.39,"campaignId":"435605480801197","expression":[{"type":"QUERY_BROAD_REL_MATCHES"}],"expressionType":"AUTO","resolvedExpression":[{"type":"QUERY_BROAD_REL_MATCHES"}],"state":"ENABLED","targetId":"215130820542650"},{"adGroupId":"366468148194339","bid":1.09,"campaignId":"435605480801197","expression":[{"type":"ASIN_ACCESSORY_RELATED"}],"expressionType":"AUTO","resolvedExpression":[{"type":"ASIN_ACCESSORY_RELATED"}],"state":"ENABLED","targetId":"112999202475579"},{"adGroupId":"366468148194339","bid":1.09,"campaignId":"435605480801197","expression":[{"type":"ASIN_SUBSTITUTE_RELATED"}],"expressionType":"AUTO","resolvedExpression":[{"type":"ASIN_SUBSTITUTE_RELATED"}],"state":"ENABLED","targetId":"61341713663353"},{"adGroupId":"346548630739145","bid":1.9,"campaignId":"336812809776358","expression":[{"type":"ASIN_CATEGORY_SAME_AS","value":"20862636031"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_CATEGORY_SAME_AS","value":"PlayStation 5 Accessories"}],"state":"ENABLED","targetId":"68823353108001"},{"adGroupId":"346548630739145","bid":0.23,"campaignId":"336812809776358","expression":[{"type":"ASIN_CATEGORY_SAME_AS","value":"20862635031"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_CATEGORY_SAME_AS","value":"PlayStation 5 Consoles, Games & Accessories"}],"state":"ENABLED","targetId":"56881360139414"},{"adGroupId":"346548630739145","bid":0.2,"campaignId":"336812809776358","expression":[{"type":"ASIN_CATEGORY_SAME_AS","value":"26975861031"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_CATEGORY_SAME_AS","value":"PlayStation 5 Skins"}],"state":"ENABLED","targetId":"132832937629921"},{"adGroupId":"346548630739145","bid":0.5,"campaignId":"336812809776358","expression":[{"type":"ASIN_CATEGORY_SAME_AS","value":"430593031"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_CATEGORY_SAME_AS","value":"Computer Headsets"}],"state":"ENABLED","targetId":"137265070862380"},{"adGroupId":"437040516222583","bid":0.19,"campaignId":"439301548297230","expression":[{"type":"QUERY_HIGH_REL_MATCHES"}],"expressionType":"AUTO","resolvedExpression":[{"type":"QUERY_HIGH_REL_MATCHES"}],"state":"PAUSED","targetId":"200222828598278"},{"adGroupId":"437040516222583","bid":0.19,"campaignId":"439301548297230","expression":[{"type":"QUERY_BROAD_REL_MATCHES"}],"expressionType":"AUTO","resolvedExpression":[{"type":"QUERY_BROAD_REL_MATCHES"}],"state":"PAUSED","targetId":"104903444044504"},{"adGroupId":"437040516222583","bid":0.59,"campaignId":"439301548297230","expression":[{"type":"ASIN_ACCESSORY_RELATED"}],"expressionType":"AUTO","resolvedExpression":[{"type":"ASIN_ACCESSORY_RELATED"}],"state":"PAUSED","targetId":"90575579852812"},{"adGroupId":"437040516222583","bid":0.39,"campaignId":"439301548297230","expression":[{"type":"ASIN_SUBSTITUTE_RELATED"}],"expressionType":"AUTO","resolvedExpression":[{"type":"ASIN_SUBSTITUTE_RELATED"}],"state":"ENABLED","targetId":"83234685024053"},{"adGroupId":"382070636758268","bid":0.58,"campaignId":"443410069276096","expression":[{"type":"QUERY_HIGH_REL_MATCHES"}],"expressionType":"AUTO","resolvedExpression":[{"type":"QUERY_HIGH_REL_MATCHES"}],"state":"ENABLED","targetId":"219846999320917"},{"adGroupId":"382070636758268","bid":0.29,"campaignId":"443410069276096","expression":[{"type":"QUERY_BROAD_REL_MATCHES"}],"expressionType":"AUTO","resolvedExpression":[{"type":"QUERY_BROAD_REL_MATCHES"}],"state":"ENABLED","targetId":"221606379486356"},{"adGroupId":"382070636758268","bid":2.02,"campaignId":"443410069276096","expression":[{"type":"ASIN_ACCESSORY_RELATED"}],"expressionType":"AUTO","resolvedExpression":[{"type":"ASIN_ACCESSORY_RELATED"}],"state":"ENABLED","targetId":"136744850870268"},{"adGroupId":"382070636758268","bid":0.91,"campaignId":"443410069276096","expression":[{"type":"ASIN_SUBSTITUTE_RELATED"}],"expressionType":"AUTO","resolvedExpression":[{"type":"ASIN_SUBSTITUTE_RELATED"}],"state":"ENABLED","targetId":"254383015971050"},{"adGroupId":"495321625082338","bid":0.05,"campaignId":"329619392003507","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B0CL6FCJ89"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B0CL6FCJ89"}],"state":"ENABLED","targetId":"188052820122525"},{"adGroupId":"495321625082338","bid":0.05,"campaignId":"329619392003507","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B0C33X6QBC"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B0C33X6QBC"}],"state":"ENABLED","targetId":"9157793768703"},{"adGroupId":"495321625082338","bid":0.05,"campaignId":"329619392003507","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B09M7ZRGG7"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B09M7ZRGG7"}],"state":"ENABLED","targetId":"257408191588400"},{"adGroupId":"495321625082338","bid":0.05,"campaignId":"329619392003507","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B0C2Z1GF9N"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B0C2Z1GF9N"}],"state":"ENABLED","targetId":"87140320019149"},{"adGroupId":"495321625082338","bid":0.05,"campaignId":"329619392003507","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B0C9QH8W43"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B0C9QH8W43"}],"state":"ENABLED","targetId":"7396983700192"},{"adGroupId":"495321625082338","bid":0.55,"campaignId":"329619392003507","expression":[{"type":"ASIN_CATEGORY_SAME_AS","value":"1342939031"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_CATEGORY_SAME_AS","value":"Car On-Dash Mounted Cameras"}],"state":"PAUSED","targetId":"37473258231708"},{"adGroupId":"524337407196364","bid":1.78,"campaignId":"339386037376295","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B0B7X8M694"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B0B7X8M694"}],"state":"ENABLED","targetId":"204065005574971"},{"adGroupId":"524337407196364","bid":0.35,"campaignId":"339386037376295","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B08ZDDHWSJ"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B08ZDDHWSJ"}],"state":"ENABLED","targetId":"100909244924807"},{"adGroupId":"524337407196364","bid":0.76,"campaignId":"339386037376295","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B07W5JKB8Z"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B07W5JKB8Z"}],"state":"ENABLED","targetId":"74043930785990"},{"adGroupId":"524337407196364","bid":0.4,"campaignId":"339386037376295","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B0CBQ3DVQT"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B0CBQ3DVQT"}],"state":"ENABLED","targetId":"240583032048840"},{"adGroupId":"524337407196364","bid":0.58,"campaignId":"339386037376295","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B07NR39TXK"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B07NR39TXK"}],"state":"ENABLED","targetId":"175532989354376"},{"adGroupId":"524337407196364","bid":0.84,"campaignId":"339386037376295","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B09ZLRD7Z9"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B09ZLRD7Z9"}],"state":"ENABLED","targetId":"233667918780873"},{"adGroupId":"524337407196364","bid":0.57,"campaignId":"339386037376295","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B0CBQ5VRNR"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B0CBQ5VRNR"}],"state":"ENABLED","targetId":"77168634278386"},{"adGroupId":"524337407196364","bid":0.64,"campaignId":"339386037376295","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B0B7X66NR5"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B0B7X66NR5"}],"state":"ENABLED","targetId":"275357346796225"},{"adGroupId":"524337407196364","bid":0.59,"campaignId":"339386037376295","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B01MYW8COY"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B01MYW8COY"}],"state":"ENABLED","targetId":"166021397651581"},{"adGroupId":"524337407196364","bid":0.88,"campaignId":"339386037376295","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B0B7X6WRNY"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B0B7X6WRNY"}],"state":"ENABLED","targetId":"101309174472334"},{"adGroupId":"524337407196364","bid":0.87,"campaignId":"339386037376295","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B0B7X8D45M"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B0B7X8D45M"}],"state":"ENABLED","targetId":"45940748560442"},{"adGroupId":"524337407196364","bid":0.59,"campaignId":"339386037376295","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B0C4YCLF3Q"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B0C4YCLF3Q"}],"state":"ENABLED","targetId":"76320669353643"},{"adGroupId":"524337407196364","bid":0.6,"campaignId":"339386037376295","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B07NQXBZM9"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B07NQXBZM9"}],"state":"ENABLED","targetId":"162825244049358"},{"adGroupId":"524337407196364","bid":0.8,"campaignId":"339386037376295","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B0B7X6PWMR"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B0B7X6PWMR"}],"state":"ENABLED","targetId":"71535174902457"},{"adGroupId":"524337407196364","bid":0.59,"campaignId":"339386037376295","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B0CP71YMY3"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B0CP71YMY3"}],"state":"ENABLED","targetId":"21437492842815"},{"adGroupId":"524337407196364","bid":0.55,"campaignId":"339386037376295","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B08H99878P"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B08H99878P"}],"state":"ENABLED","targetId":"70284193430563"},{"adGroupId":"524337407196364","bid":0.47,"campaignId":"339386037376295","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B08P3W5R4G"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B08P3W5R4G"}],"state":"ENABLED","targetId":"207008248702661"},{"adGroupId":"524337407196364","bid":0.87,"campaignId":"339386037376295","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B0BX7375ZQ"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B0BX7375ZQ"}],"state":"ENABLED","targetId":"133389164059001"},{"adGroupId":"524337407196364","bid":0.63,"campaignId":"339386037376295","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B0CBQ1Y9QB"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B0CBQ1Y9QB"}],"state":"ENABLED","targetId":"167126106904546"},{"adGroupId":"524337407196364","bid":0.47,"campaignId":"339386037376295","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B00ZC3S818"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B00ZC3S818"}],"state":"ENABLED","targetId":"78194575992947"},{"adGroupId":"524337407196364","bid":0.73,"campaignId":"339386037376295","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B07Y2MJRW6"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B07Y2MJRW6"}],"state":"ENABLED","targetId":"7434925976304"},{"adGroupId":"524337407196364","bid":0.82,"campaignId":"339386037376295","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B09ZLRCH1H"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B09ZLRCH1H"}],"state":"ENABLED","targetId":"8310072692362"},{"adGroupId":"524337407196364","bid":0.55,"campaignId":"339386037376295","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B09T32445M"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B09T32445M"}],"state":"ENABLED","targetId":"40531077562002"},{"adGroupId":"524337407196364","bid":0.34,"campaignId":"339386037376295","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B0BX722HFN"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B0BX722HFN"}],"state":"ENABLED","targetId":"187952399858883"},{"adGroupId":"524337407196364","bid":0.61,"campaignId":"339386037376295","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B07NH6Q4LB"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B07NH6Q4LB"}],"state":"ENABLED","targetId":"68477268235656"},{"adGroupId":"524337407196364","bid":0.78,"campaignId":"339386037376295","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B0B7X8392G"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B0B7X8392G"}],"state":"ENABLED","targetId":"55344098769110"},{"adGroupId":"524337407196364","bid":0.61,"campaignId":"339386037376295","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B08486NP2G"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B08486NP2G"}],"state":"ENABLED","targetId":"157074656967254"},{"adGroupId":"524337407196364","bid":0.51,"campaignId":"339386037376295","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B09K139986"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B09K139986"}],"state":"ENABLED","targetId":"29925703488788"},{"adGroupId":"524337407196364","bid":0.48,"campaignId":"339386037376295","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B07TLX61W7"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B07TLX61W7"}],"state":"ENABLED","targetId":"158338949632867"},{"adGroupId":"524337407196364","bid":0.43,"campaignId":"339386037376295","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B00SAYCXWG"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B00SAYCXWG"}],"state":"ENABLED","targetId":"180030215388548"},{"adGroupId":"524337407196364","bid":0.58,"campaignId":"339386037376295","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B07NR2TJP9"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B07NR2TJP9"}],"state":"ENABLED","targetId":"148637144114670"},{"adGroupId":"524337407196364","bid":0.63,"campaignId":"339386037376295","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B0B7X7PK9S"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B0B7X7PK9S"}],"state":"ENABLED","targetId":"228375938945209"},{"adGroupId":"524337407196364","bid":0.48,"campaignId":"339386037376295","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B07MTXLFXV"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B07MTXLFXV"}],"state":"ENABLED","targetId":"214601922255009"},{"adGroupId":"524337407196364","bid":0.52,"campaignId":"339386037376295","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B00YXO5UKY"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B00YXO5UKY"}],"state":"ENABLED","targetId":"38075257010822"},{"adGroupId":"524337407196364","bid":0.58,"campaignId":"339386037376295","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B09GBY7632"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B09GBY7632"}],"state":"ENABLED","targetId":"208531192521090"},{"adGroupId":"524337407196364","bid":0.57,"campaignId":"339386037376295","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B09DPR2LZW"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B09DPR2LZW"}],"state":"ENABLED","targetId":"260609084575580"},{"adGroupId":"524337407196364","bid":0.46,"campaignId":"339386037376295","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B07NQX1J99"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B07NQX1J99"}],"state":"ENABLED","targetId":"272886670467900"},{"adGroupId":"524337407196364","bid":0.77,"campaignId":"339386037376295","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B0C4Y9L5V9"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B0C4Y9L5V9"}],"state":"ENABLED","targetId":"188274076660110"},{"adGroupId":"524337407196364","bid":0.45,"campaignId":"339386037376295","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B09ZLS8LB3"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B09ZLS8LB3"}],"state":"ENABLED","targetId":"180650048146806"},{"adGroupId":"524337407196364","bid":0.57,"campaignId":"339386037376295","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B0CCHL2C2V"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B0CCHL2C2V"}],"state":"ENABLED","targetId":"42584643773883"},{"adGroupId":"524337407196364","bid":0.57,"campaignId":"339386037376295","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B08D46V1TK"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B08D46V1TK"}],"state":"ENABLED","targetId":"67341708099473"},{"adGroupId":"524337407196364","bid":0.42,"campaignId":"339386037376295","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B0BKDX43K8"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B0BKDX43K8"}],"state":"ENABLED","targetId":"24686856968992"},{"adGroupId":"524337407196364","bid":0.7,"campaignId":"339386037376295","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B0CCP1QP4V"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B0CCP1QP4V"}],"state":"ENABLED","targetId":"217627446093193"},{"adGroupId":"524337407196364","bid":0.45,"campaignId":"339386037376295","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B0BYNTQM7D"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B0BYNTQM7D"}],"state":"ENABLED","targetId":"257411664023744"},{"adGroupId":"553621540386674","bid":0.3,"campaignId":"314656998006033","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B00YXO5UKY"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B00YXO5UKY"}],"state":"ENABLED","targetId":"113408323717160"},{"adGroupId":"524337407196364","bid":0.59,"campaignId":"339386037376295","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B0C7ZW5PYY"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B0C7ZW5PYY"}],"state":"ENABLED","targetId":"61621265542879"},{"adGroupId":"553621540386674","bid":0.2,"campaignId":"314656998006033","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B0BX7375ZQ"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B0BX7375ZQ"}],"state":"ENABLED","targetId":"135601529168160"},{"adGroupId":"553621540386674","bid":0.3,"campaignId":"314656998006033","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B09T32445M"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B09T32445M"}],"state":"ENABLED","targetId":"123041166305708"},{"adGroupId":"553621540386674","bid":0.2,"campaignId":"314656998006033","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B0B7X6PWMR"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B0B7X6PWMR"}],"state":"ENABLED","targetId":"104624085313323"},{"adGroupId":"553621540386674","bid":0.3,"campaignId":"314656998006033","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B07NH6Q4LB"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B07NH6Q4LB"}],"state":"ENABLED","targetId":"65552179054739"},{"adGroupId":"553621540386674","bid":0.1,"campaignId":"314656998006033","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B0CN7448TQ"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B0CN7448TQ"}],"state":"ENABLED","targetId":"281188689271140"},{"adGroupId":"553621540386674","bid":0.15,"campaignId":"314656998006033","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B09C5SZQTH"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B09C5SZQTH"}],"state":"ENABLED","targetId":"16336185438821"},{"adGroupId":"553621540386674","bid":0.15,"campaignId":"314656998006033","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B09BD75Y21"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B09BD75Y21"}],"state":"ENABLED","targetId":"53182685719545"},{"adGroupId":"553621540386674","bid":0.15,"campaignId":"314656998006033","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B08V56DKKS"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B08V56DKKS"}],"state":"ENABLED","targetId":"97651398241338"},{"adGroupId":"553621540386674","bid":0.1,"campaignId":"314656998006033","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B0CN185GTC"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B0CN185GTC"}],"state":"ENABLED","targetId":"40242496906995"},{"adGroupId":"553621540386674","bid":0.15,"campaignId":"314656998006033","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B07Y1WRD1H"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B07Y1WRD1H"}],"state":"ENABLED","targetId":"267722101985351"},{"adGroupId":"553621540386674","bid":0.15,"campaignId":"314656998006033","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B07S9886QV"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B07S9886QV"}],"state":"ENABLED","targetId":"31507168366227"},{"adGroupId":"553621540386674","bid":0.15,"campaignId":"314656998006033","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B07J4W9XVH"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B07J4W9XVH"}],"state":"ENABLED","targetId":"2144252281461"},{"adGroupId":"553621540386674","bid":0.1,"campaignId":"314656998006033","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B0CP71YMY3"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B0CP71YMY3"}],"state":"ENABLED","targetId":"8602916753986"},{"adGroupId":"553621540386674","bid":0.1,"campaignId":"314656998006033","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B0BWJGT1ZL"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B0BWJGT1ZL"}],"state":"ENABLED","targetId":"169126639187005"},{"adGroupId":"553621540386674","bid":0.15,"campaignId":"314656998006033","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B098TV8C93"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B098TV8C93"}],"state":"ENABLED","targetId":"263494624470441"},{"adGroupId":"553621540386674","bid":0.15,"campaignId":"314656998006033","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B09HBY415N"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B09HBY415N"}],"state":"ENABLED","targetId":"194247509693523"},{"adGroupId":"553621540386674","bid":0.15,"campaignId":"314656998006033","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B07ZHPNDX6"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B07ZHPNDX6"}],"state":"ENABLED","targetId":"250846362575866"},{"adGroupId":"553621540386674","bid":0.15,"campaignId":"314656998006033","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B07X557P7S"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B07X557P7S"}],"state":"ENABLED","targetId":"107574536219666"},{"adGroupId":"553621540386674","bid":0.1,"campaignId":"314656998006033","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B0C46N5G7Z"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B0C46N5G7Z"}],"state":"ENABLED","targetId":"276829423459452"},{"adGroupId":"553621540386674","bid":0.1,"campaignId":"314656998006033","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B0C49F6NG6"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B0C49F6NG6"}],"state":"ENABLED","targetId":"265288451547827"},{"adGroupId":"553621540386674","bid":0.1,"campaignId":"314656998006033","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B0B973VCZX"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B0B973VCZX"}],"state":"ENABLED","targetId":"51408143921755"},{"adGroupId":"553621540386674","bid":0.1,"campaignId":"314656998006033","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B0B96KRW9P"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B0B96KRW9P"}],"state":"ENABLED","targetId":"270223789055440"},{"adGroupId":"553621540386674","bid":0.1,"campaignId":"314656998006033","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B0CH346J32"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B0CH346J32"}],"state":"ENABLED","targetId":"133701333834619"},{"adGroupId":"553621540386674","bid":0.15,"campaignId":"314656998006033","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B09GB2SSFC"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B09GB2SSFC"}],"state":"ENABLED","targetId":"112461378501422"},{"adGroupId":"553621540386674","bid":0.1,"campaignId":"314656998006033","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B0B8PGDMWK"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B0B8PGDMWK"}],"state":"ENABLED","targetId":"179608863344636"},{"adGroupId":"553621540386674","bid":0.1,"campaignId":"314656998006033","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B0CJ3H9XD9"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B0CJ3H9XD9"}],"state":"ENABLED","targetId":"158175698819356"},{"adGroupId":"553621540386674","bid":0.1,"campaignId":"314656998006033","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B09ZLRD7Z9"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B09ZLRD7Z9"}],"state":"ENABLED","targetId":"233313402728856"},{"adGroupId":"553621540386674","bid":0.15,"campaignId":"314656998006033","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B08JVH5TZS"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B08JVH5TZS"}],"state":"ENABLED","targetId":"63238673138505"},{"adGroupId":"553621540386674","bid":0.15,"campaignId":"314656998006033","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B08BTHXJFN"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B08BTHXJFN"}],"state":"ENABLED","targetId":"32349407815369"},{"adGroupId":"553621540386674","bid":0.15,"campaignId":"314656998006033","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B093BPQTVQ"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B093BPQTVQ"}],"state":"ENABLED","targetId":"196217754848686"},{"adGroupId":"553621540386674","bid":0.1,"campaignId":"314656998006033","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B0CH7WLFRN"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B0CH7WLFRN"}],"state":"ENABLED","targetId":"112235519085753"},{"adGroupId":"553621540386674","bid":0.15,"campaignId":"314656998006033","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B09GBY7632"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B09GBY7632"}],"state":"ENABLED","targetId":"166078510955329"},{"adGroupId":"553621540386674","bid":0.1,"campaignId":"314656998006033","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B0B7X9BPBH"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B0B7X9BPBH"}],"state":"ENABLED","targetId":"239743410416082"},{"adGroupId":"553621540386674","bid":0.15,"campaignId":"314656998006033","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B07JZ2Z588"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B07JZ2Z588"}],"state":"ENABLED","targetId":"277020877060607"},{"adGroupId":"553621540386674","bid":0.15,"campaignId":"314656998006033","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B07MTXLFXV"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B07MTXLFXV"}],"state":"ENABLED","targetId":"83286941436492"},{"adGroupId":"553621540386674","bid":0.1,"campaignId":"314656998006033","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B0CP3V6SSD"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B0CP3V6SSD"}],"state":"ENABLED","targetId":"235702357445858"},{"adGroupId":"553621540386674","bid":0.15,"campaignId":"314656998006033","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B09V29DJ78"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B09V29DJ78"}],"state":"ENABLED","targetId":"164723420431485"},{"adGroupId":"553621540386674","bid":0.15,"campaignId":"314656998006033","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B09KM4XXG6"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B09KM4XXG6"}],"state":"ENABLED","targetId":"165544263515659"},{"adGroupId":"553621540386674","bid":0.15,"campaignId":"314656998006033","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B07RMC5BRL"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B07RMC5BRL"}],"state":"ENABLED","targetId":"99321867655676"},{"adGroupId":"553621540386674","bid":0.15,"campaignId":"314656998006033","expression":[{"type":"ASIN_EXPANDED_FROM","value":"B08GDHCB49"}],"expressionType":"MANUAL","resolvedExpression":[{"type":"ASIN_EXPANDED_FROM","value":"B08GDHCB49"}],"state":"ENABLED","targetId":"237858705040586"}]
#
# for item in data:
#     print(f"bid: {item['bid']}, targetId: {item['targetId']}")
#     target_id = item['targetId']
#     state = item['state']
#     bid = item.get('bid')
#     if bid is not None:
#         Gen_adgroup('Veement').update_adGroup_TargetingClause('UK',target_id,bid,state)
