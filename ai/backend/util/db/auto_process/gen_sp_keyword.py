import asyncio

from ai.backend.util.db.auto_process.tools_db_new_sp import DbNewSpTools
from datetime import datetime
from ai.backend.util.db.auto_process.tools_sp_keyword import SPKeywordTools
from ai.backend.util.db.db_amazon.generate_tools import ask_question


class Gen_keyword:
    def __init__(self,brand):
        self.brand = brand

    def add_keyword_toadGroup(self,market,campaignId,matchType,state,bid,adGroupId,keywordText):

        # 这里需要将新传入的根据国家进行翻译成对应国家语言
        translate_kw = asyncio.get_event_loop().run_until_complete(ask_question(keywordText,market))
        keywordText_new= eval(translate_kw)[0]
        # 翻译完成进行添加
        keyword_info={
      "keywords": [
        {
          "campaignId": str(campaignId),
          "matchType": matchType,
          "state": state,
          "bid": bid,
          "adGroupId": str(adGroupId),
          "keywordText": keywordText_new
        }
      ]
    }
        # 新增关键词操作
        apitool = SPKeywordTools(self.brand)
        res = apitool.create_spkeyword_api(keyword_info, market)

        # 根据结果更新log
        dbNewTools = DbNewSpTools(self.brand,market)
        if res[0]=="success":
            dbNewTools.add_sp_keyword_toadGroup(market,res[1],campaignId,matchType,state,bid,adGroupId,keywordText,keywordText_new,"success",datetime.now())
        else:
            dbNewTools.add_sp_keyword_toadGroup(market,res[1],campaignId,matchType,state,bid,adGroupId,keywordText,keywordText_new,"failed",datetime.now())
        return res[1]


    def add_keyword_toadGroup_v0(self,market,campaignId,adGroupId,keywordText,matchType,state,bid):
        # 翻译完成进行添加
        keyword_info={
      "keywords": [
        {
          "campaignId": str(campaignId),
          "matchType": matchType,
          "state": state,
          "bid": bid,
          "adGroupId": str(adGroupId),
          "keywordText": keywordText
        }
      ]
    }
        # 新增关键词操作
        apitool = SPKeywordTools(self.brand)
        res = apitool.create_spkeyword_api(keyword_info, market)

        # 根据结果更新log
        dbNewTools = DbNewSpTools(self.brand,market)
        if res[0]=="success":
            dbNewTools.add_sp_keyword_toadGroup(market,res[1],campaignId,matchType,state,bid,adGroupId,None,keywordText,"success",datetime.now())
        else:
            dbNewTools.add_sp_keyword_toadGroup(market,res[1],campaignId,matchType,state,bid,adGroupId,None,keywordText,"failed",datetime.now())
        return res[1]

    def update_keyword_toadGroup(self,market,keywordId,bid_old,bid_new,state):

        # 修改广告组关键词信息
        keyword_info={
      "keywords": [
        {
          "keywordId": str(keywordId),
          "state": state,
          "bid": bid_new
        }
      ]
    }
        # 修改关键词操作
        apitool = SPKeywordTools(self.brand)
        res = apitool.update_spkeyword_api(keyword_info,market)

        # 根据结果更新log
        # def update_sp_keyword_toadGroup(self,market,keywordId,state,bid,operation_state,create_time):
        dbNewTools = DbNewSpTools(self.brand,market)
        if res[0]=="success":
            dbNewTools.update_sp_keyword_toadGroup(market,keywordId,state,bid_old,bid_new,"success",datetime.now())
        else:
            dbNewTools.update_sp_keyword_toadGroup(market,keywordId,state,bid_old,bid_new,"failed",datetime.now())

    def delete_keyword_toadGroup(self,market,keywordId):

        # 修改广告组关键词信息
        keyword_info = {
  "keywordIdFilter": {
    "include": [
      str(keywordId)
    ]
  }
}
        # 修改关键词操作
        apitool = SPKeywordTools(self.brand)
        res = apitool.delete_spkeyword_api(keyword_info,market)

        # 根据结果更新log
        # def update_sp_keyword_toadGroup(self,market,keywordId,state,bid,operation_state,create_time):
        dbNewTools = DbNewSpTools(self.brand,market)
        if res[0]=="success":
            dbNewTools.update_sp_keyword_toadGroup(market,keywordId,'delete',None,None,"success",datetime.now())
        else:
            dbNewTools.update_sp_keyword_toadGroup(market,keywordId,'delete',None,None,"failed",datetime.now())


    # 新增测试
    # add_keyword_toadGroup('US','513987903939456','EXACT','PAUSED',0.9,'484189822427360','thermal underwear')
    # 修改测试
    # update_keyword_toadGroup('US','405003352192308','PAUSED',0.3)
# Gen_keyword('LAPASA').add_keyword_toadGroup_v0('IT','153630823947693','235290135936438','pigiama pile uomo','EXACT','ENABLED',None)
# 177235977989981
