import asyncio

from tools_db_new_sp import DbNewSpTools
from datetime import datetime
from tools_keyword import SPKeywordTools
from ai.backend.util.db.db_amazon.generate_tools import ask_question



db_info = {'host': '****', 'user': '****', 'passwd': '****', 'port': 3306,
               'db': '****',
               'charset': 'utf8mb4', 'use_unicode': True, }
def add_keyword_toadGroup(market,campaignId,matchType,state,bid,adGroupId,keywordText):

    # 这里需要将新传入的根据国家进行翻译成对应国家语言
    translate_kw = asyncio.get_event_loop().run_until_complete(ask_question(keywordText,market))
    keywordText_new= eval(translate_kw)[0]
    # 翻译完成进行添加
    keyword_info={
  "keywords": [
    {
      "campaignId": campaignId,
      "matchType": matchType,
      "state": state,
      "bid": bid,
      "adGroupId": adGroupId,
      "keywordText": keywordText_new
    }
  ]
}
    # 新增关键词操作
    apitool = SPKeywordTools()
    res = apitool.create_spkeyword_api(keyword_info)

    # 根据结果更新log
    dbNewTools = DbNewSpTools()
    if res[0]=="success":
        dbNewTools.add_sp_keyword_toadGroup(market,res[1],campaignId,matchType,state,bid,adGroupId,keywordText,keywordText_new,"success",datetime.now())
    else:
        dbNewTools.add_sp_keyword_toadGroup(market,res[1],campaignId,matchType,state,bid,adGroupId,keywordText,keywordText_new,"failed",datetime.now())

def update_keyword_toadGroup(market,keywordId,state,bid):

    # 修改广告组关键词信息
    keyword_info={
  "keywords": [
    {
      "keywordId": keywordId,
      "state": state,
      "bid": bid
    }
  ]
}
    # 修改关键词操作
    apitool = SPKeywordTools()
    res = apitool.update_spkeyword_api(keyword_info)

    # 根据结果更新log
    # def update_sp_keyword_toadGroup(self,market,keywordId,state,bid,operation_state,create_time):
    dbNewTools = DbNewSpTools()
    if res[0]=="success":
        dbNewTools.update_sp_keyword_toadGroup(market,keywordId,state,bid,"success",datetime.now())
    else:
        dbNewTools.update_sp_keyword_toadGroup(market,keywordId,state,bid,"failed",datetime.now())


# 新增测试
# add_keyword_toadGroup('US','513987903939456','EXACT','PAUSED',0.9,'484189822427360','thermal underwear')
# 修改测试
# update_keyword_toadGroup('US','405003352192308','PAUSED',0.3)
