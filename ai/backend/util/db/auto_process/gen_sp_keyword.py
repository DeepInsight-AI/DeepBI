import asyncio

from ai.backend.util.db.auto_process.tools_db_new_sp import DbNewSpTools
from datetime import datetime
from ai.backend.util.db.auto_process.tools_sp_keyword import SPKeywordTools
from ai.backend.util.db.db_amazon.generate_tools import ask_question


class Gen_keyword(SPKeywordTools):
    def __init__(self, db, brand, market):
        super().__init__(db, brand, market)

    def add_keyword_toadGroup(self,campaignId,matchType,state,bid,adGroupId,keywordText):

        # 这里需要将新传入的根据国家进行翻译成对应国家语言
        translate_kw = asyncio.get_event_loop().run_until_complete(ask_question(keywordText,self.market))
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
        res = self.create_spkeyword_api(keyword_info)

        # 根据结果更新log
        dbNewTools = DbNewSpTools(self.db, self.brand,self.market)
        if res[0]=="success":
            dbNewTools.add_sp_keyword_toadGroup(self.market,res[1],campaignId,matchType,state,bid,adGroupId,keywordText,keywordText_new,"success",datetime.now())
        else:
            dbNewTools.add_sp_keyword_toadGroup(self.market,res[1],campaignId,matchType,state,bid,adGroupId,keywordText,keywordText_new,"failed",datetime.now())
        return res[1]


    def add_keyword_toadGroup_v0(self,campaignId,adGroupId,keywordText,matchType,state,bid, user='test'):
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
        res = self.create_spkeyword_api(keyword_info)

        # 根据结果更新log
        dbNewTools = DbNewSpTools(self.db, self.brand,self.market)
        if res[0]=="success":
            dbNewTools.add_sp_keyword_toadGroup(self.market,res[1],campaignId,matchType,state,bid,adGroupId,None,keywordText,"success",datetime.now(), user)
        else:
            dbNewTools.add_sp_keyword_toadGroup(self.market,res[1],campaignId,matchType,state,bid,adGroupId,None,keywordText,"failed",datetime.now(), user)
        return res[1]

    def update_keyword_toadGroup(self,keywordId,bid_old,bid_new,state, user='test'):

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
        res = self.update_spkeyword_api(keyword_info)

        # 根据结果更新log
        # def update_sp_keyword_toadGroup(self,market,keywordId,state,bid,operation_state,create_time):
        dbNewTools = DbNewSpTools(self.db, self.brand,self.market)
        if res[0]=="success":
            dbNewTools.update_sp_keyword_toadGroup(self.market,keywordId,state,bid_old,bid_new,"success",datetime.now(), user)
        else:
            dbNewTools.update_sp_keyword_toadGroup(self.market,keywordId,state,bid_old,bid_new,"failed",datetime.now(), user)

    def update_keyword_toadGroup_batch(self,info, user='test'):

        keyword_info = {
            "keywords": []
        }

        for item in info:
            keyword_info["keywords"].append({
                "keywordId": str(item['keywordId']),
                "state": item['state'],
                "bid": float(item['bid_new'])
            })
        print(keyword_info)
        # 修改关键词操作
        res = self.update_spkeyword_api_batch(keyword_info)
        print(res)
        # 存储更新记录到数据库
        dbNewTools = DbNewSpTools(self.db, self.brand, self.market)
        updates = []

        if res[0] == "success":
            status = "success"
        else:
            status = "failed"

        for item in info:
            updates.append({
                'market': self.market,
                'keywordId': item['keywordId'],
                'state': item['state'],
                'bid_old': item['bid'],  # Assuming you have this value in `info`
                'bid_new': item['bid_new'],
                'operation_state': status,
                'create_time': datetime.now(),
                'user': user
            })

        # 批量插入到数据库
        dbNewTools.batch_update_sp_keywords(updates)


    def delete_keyword_toadGroup(self,keywordId):

        # 修改广告组关键词信息
        keyword_info = {
  "keywordIdFilter": {
    "include": [
      str(keywordId)
    ]
  }
}
        # 修改关键词操作
        res = self.delete_spkeyword_api(keyword_info)

        # 根据结果更新log
        # def update_sp_keyword_toadGroup(self,market,keywordId,state,bid,operation_state,create_time):
        dbNewTools = DbNewSpTools(self.db, self.brand,self.market)
        if res[0]=="success":
            dbNewTools.update_sp_keyword_toadGroup(self.market,keywordId,'delete',None,None,"success",datetime.now())
        else:
            dbNewTools.update_sp_keyword_toadGroup(self.market,keywordId,'delete',None,None,"failed",datetime.now())


    # 新增测试
    # add_keyword_toadGroup('US','513987903939456','EXACT','PAUSED',0.9,'484189822427360','thermal underwear')
    # 修改测试
    # update_keyword_toadGroup('US','405003352192308','PAUSED',0.3)
# Gen_keyword('LAPASA').add_keyword_toadGroup_v0('IT','153630823947693','235290135936438','pigiama pile uomo','EXACT','ENABLED',None)
# 177235977989981
