import csv
import json

from flask import render_template, Flask

from amazon_mysql_rag_util_sp import AmazonMysqlRagUitl
from auto_content import REPORT_THOUGHT,REPORT_ANALYST
# -*- coding: utf-8 -*-
from jinja2 import Template
import time
import pandas as pd


def auto_generepot(market1,market2,startdate,endate):
    """该报告针对指定市场指定日期范围内的SP广告进行生成"""
    db_info = {'host': '192.168.5.114', 'user': 'test_deepdata', 'passwd': 'test123!@#', 'port': 3308,
               'db': 'amazon_ads',
               'charset': 'utf8mb4', 'use_unicode': True, }

    amazon = AmazonMysqlRagUitl(db_info)

    last_answer = {}

    # 设置标题 SP广告法国优化销售额期望（以德国4.23-4.30为例）
    report_name = "SP广告"+market1+"优化销售额期望（以"+market2+startdate+"-"+endate+"为例）"
    last_answer["report_name"]=report_name

    # 增加问题
    all_report_question=[]
    """构建"""

    # 构建1.法国所有商品与德国前70%销售重叠部分
    module_1_process_1 = []
    keyword_1_q1 = "1.1 在 {} 至 {} 这段时间内，{}_SP广告的前70%销售部分的商品sku，将这些sku信息生成csv文件。".format(startdate, endate, market2)
    keyword_1_a1 = amazon.get_sp_seles_top70_sku(market2, startdate, endate)
    module_1_process_1.append({"question": keyword_1_q1, "answer": keyword_1_a1})
    keyword_1_q2 = "1.2 在 {} 至 {} 这段时间内，{}_SP广告的前70%销售部分的商品sku与{}在售商品重复的sku是什么，将这些sku信息生成csv文件。".format(startdate, endate, market2, market1)
    keyword_1_a2 = amazon.get_repeat_sp_seles_top70_sku(market1, market2, startdate, endate)
    module_1_process_1.append({"question": keyword_1_q2, "answer": keyword_1_a2})
    keyword_1_q3 = "1.3 在 {} 至 {} 这段时间内，{}_SP广告重复的sku的总销售额 ，总广告消耗和平均ACOS是多少？".format(startdate, endate, market2)
    keyword_1_a3 = amazon.get_repeat_sp_seles_top70_sku_info_market2(market1, market2, startdate, endate)
    module_1_process_1.append({"question": keyword_1_q3, "answer": keyword_1_a3})
    keyword_1_q4 = "1.4 在 {} 至 {} 这段时间内，{}_SP广告重复的sku的总销售额 ，总广告消耗和平均ACOS是多少？".format(startdate, endate, market1)
    keyword_1_a4 = amazon.get_repeat_sp_seles_top70_sku_info_market1(market1, market2, startdate, endate)
    module_1_process_1.append({"question": keyword_1_q4, "answer": keyword_1_a4})

    keyword_answer_1 = {}
    keyword_answer_1["analysis_item"] = '1.找出法国所有商品与德国前70%销售重叠部分'
    keyword_answer_1["description"] = '分四步执行'
    keyword_answer_1["analysis_process"] = module_1_process_1

    keyword_answer = []
    keyword_answer.append(keyword_answer_1)

    # 2.找出 定向投放关键词 中的 优质关键词，提价操作
    module_1_process_2 = []
    keyword_2_q1 = "2.1，在 {} 至 {} 这段时间内，{}_SP广告的 平均ACOS ，总广告消耗，去重后定向投放关键词 数量 和 总点击量 分别是多少？".format(startdate,endate,market1)
    keyword_2_a1 = amazon.get_sp_searchterm_keyword_info_target(market1,startdate,endate)
    module_1_process_2.append({"question": keyword_2_q1, "answer": keyword_2_a1})

    keyword_answer_2 = {}
    keyword_answer_2["analysis_item"] = '2.法国所有商品与德国前70%销售重叠部分,进行线性优化'
    keyword_answer_2["description"] = '分五步执行'
    keyword_answer_2["analysis_process"] = module_1_process_2
    keyword_answer.append(keyword_answer_2)


    # 关键词question
    keyword_question={}
    keyword_question["question"]={'report_name': '1.关键词优化分析',
          'description': '目标：关键词的扩充机会，来自于 扩词，通过优秀关键词的共同特征，扩充关键词。  用户搜索的关键词，没有在手动投放关键词中的。扩品，优秀关键词都投放在哪些产品上，这些品的特征是什么。是否有符合特征的品，没有添加过（投放过）这些关键词？扩点击量，哪些关键词还有提价空间\n补充：按产品类型对其中的关键词分类，目标是 同类型产品广告计划中，相关的优秀关键词应该全覆盖。 哪些关键词没有在计划中，可以扩充'}
    keyword_question["answer"]=keyword_answer
    # 关键词部分完成
    all_report_question.append(keyword_question)
    #
    #
    # 商品优化部分
    # 1，未重叠部分
    module_2_process_1 = []
    product_1_q1 = "1.1 在 {} 至 {} 这段时间内，{}_SP广告的前70%销售部分的商品sku中{}_SP广告未在售的sku是什么，将这些sku信息生成csv文件。".format(startdate, endate, market2, market1)
    product_1_a1 = amazon.get_unrepeat_sp_seles_top70_sku(market1, market2, startdate, endate)
    module_2_process_1.append({"question": product_1_q1, "answer": product_1_a1})
    product_1_q2 = "1.2 在 {} 至 {} 这段时间内，{}_SP广告中{}_SP广告未在售的sku的总销售额 ，总广告消耗和平均ACOS是多少？".format(startdate, endate, market2, market1)
    product_1_a2 = amazon.get_unrepeat_sp_seles_top70_sku_info_market2(market1, market2, startdate, endate)
    module_2_process_1.append({"question": product_1_q2, "answer": product_1_a2})
    product_1_q3 = "1.3，根据在 {} 至 {} 这段时间内，{}_SP广告和{}_SP广告重叠sku的数据，预测{}_SP广告未在售sku的总销售额 ，总广告消耗和平均ACOS是多少？".format(startdate,endate,market1,market2,market1)
    product_1_a3 = amazon.predict_unrepeat_sp_seles_top70_sku_info_market1(market1, market2, startdate, endate)
    module_2_process_1.append({"question": product_1_q3, "answer": product_1_a3})

    product_answer_1 = {}
    product_answer_1["analysis_item"] = '1，找出 sku广告 中的 优质广告'
    product_answer_1["description"] = '分五步执行'
    product_answer_1["analysis_process"] = module_2_process_1

    product_answer = []
    product_answer.append(product_answer_1)

    # 产品question
    product_question = {}
    product_question["question"] = {'report_name': '2.商品优化分析',
                                    'description': '##补充：找出ACOS表现较好的品和ACOS表现较差的品进行调整'}
    product_question["answer"] = product_answer
    # 产品部分完成
    all_report_question.append(product_question)
    #
    # 广告计划优化分析
    # 1，找出 campaign 广告活动 中的  优质 与 劣质广告活动
    campaign_analysis_process_1 = []
    campaign_1_q1 = "1.1 在 {} 至 {} 这段时间内，{}_SP广告的前70%销售部分的商品sku与{}在售商品未重复的sku是什么，将这些sku信息生成csv文件。".format(startdate, endate, market2, market1)
    campaign_1_a1 = amazon.get_unrepeat_market1_sp_seles_sku(market1, market2, startdate, endate)
    campaign_analysis_process_1.append({"question":campaign_1_q1,"answer":campaign_1_a1})
    campaign_1_q2 = "1.2 在 {} 至 {} 这段时间内，{}_SP广告未重复的sku的总销售额 ，总广告消耗和平均ACOS是多少？".format(startdate, endate, market1)
    campaign_1_a2 = amazon.get_unrepeat_sp_seles_sku_info_market1(market1, market2, startdate, endate)
    campaign_analysis_process_1.append({"question": campaign_1_q2, "answer": campaign_1_a2})
    campaign_1_q3 = "1.3 在 {} 至 {} 这段时间内，{}_SP广告中未重复的sku中低于 平均ACOS 30%以上的sku的旧总销售额 ，总广告消耗和提价后的期望是多少".format(startdate, endate, market1)
    campaign_1_a3 = amazon.predict_unrepeat_market1_sp_seles_sku_info(market1, market2, startdate, endate)
    campaign_analysis_process_1.append({"question":campaign_1_q3,"answer":campaign_1_a3})
    # campaign_1_q3 = "1.3[优质广告]，找出在 {} 至 {} 这段时间内，{}_SP广告中 低于 平均ACOS值（替换为第一问结论） 30% 以上的  campaign 广告活动。将这些信息生成csv文件，里面记录这些关键词的以下信息，CPC， ACOS,  Clicks，campaignid，spend.".format(startdate,endate,market)
    # campaign_1_a3 = amazon.get_sp_campaign_below30(market,startdate,endate)
    # campaign_analysis_process_1.append({"question":campaign_1_q3,"answer":campaign_1_a3})
    # campaign_1_q4 = "1.4[优质广告]，找出在 {} 至 {} 这段时间内，{}_SP广告中 低于 平均ACOS值（替换为第一问结论） 20% - 30% 的 campaign 广告活动。将这些信息生成csv文件，里面记录这些关键词的以下信息，CPC， ACOS,  Clicks，campaignid，spend.".format(startdate,endate,market)
    # campaign_1_a4 = amazon.get_sp_campaign_below20to30(market,startdate,endate)
    # campaign_analysis_process_1.append({"question":campaign_1_q4,"answer":campaign_1_a4})
    # campaign_1_q5 = "1.5[优质广告]，找出在 {} 至 {} 这段时间内，{}_SP广告中 低于 平均ACOS值（替换为第一问结论） 10% - 20% 的 campaign 广告活动。将这些信息生成csv文件，里面记录这些关键词的以下信息，CPC， ACOS,  Clicks，campaignid，spend.".format(startdate,endate,market)
    # campaign_1_a5 = amazon.get_sp_campaign_below10to20(market,startdate,endate)
    # campaign_analysis_process_1.append({"question":campaign_1_q5,"answer":campaign_1_a5})
    # campaign_1_q6 = "1.6[劣质广告]，在 {} 至 {} 这段时间内，{}_SP广告中 高于 平均ACOS值（替换为第一问结论） 10% 的  campaign 广告活动数量 数量 和 总点击量 是多少".format(
    #     startdate, endate, market)
    # campaign_1_a6 = amazon.spcampaign316(market,startdate,endate)
    # campaign_analysis_process_1.append({"question": campaign_1_q6, "answer": campaign_1_a6})
    # campaign_1_q7 = "1.7[劣质广告]，找出在 {} 至 {} 这段时间内，{}_SP广告中 高于 平均ACOS值（替换为第一问结论） 30% 以上的  campaign 广告活动。将这些信息生成csv文件，里面记录这些关键词的以下信息，CPC， ACOS,  Clicks，campaignid，spend.".format(
    #     startdate, endate, market)
    # campaign_1_a7 = amazon.spcampaign317(market,startdate,endate)
    # campaign_analysis_process_1.append({"question": campaign_1_q7, "answer": campaign_1_a7})
    # campaign_1_q8 = "1.8[劣质广告]，找出在 {} 至 {} 这段时间内，{}_SP广告中 高于 平均ACOS值（替换为第一问结论） 20% - 30% 的 campaign 广告活动。将这些信息生成csv文件，里面记录这些关键词的以下信息，CPC， ACOS,  Clicks，campaignid，spend.".format(
    #     startdate, endate, market)
    # campaign_1_a8 = amazon.apcampaign318(market,startdate,endate)
    # campaign_analysis_process_1.append({"question": campaign_1_q8, "answer": campaign_1_a8})
    # campaign_1_q9 = "1.9[劣质广告]，找出在 {} 至 {} 这段时间内，{}_SP广告中 高于 平均ACOS值（替换为第一问结论） 10% - 20% 的 campaign 广告活动。将这些信息生成csv文件，里面记录这些关键词的以下信息，CPC， ACOS,  Clicks，campaignid，spend.".format(
    #     startdate, endate, market)
    # campaign_1_a9 = amazon.apcampaign319(market,startdate,endate)
    # campaign_analysis_process_1.append({"question": campaign_1_q9, "answer": campaign_1_a9})

    campaign_answer_1 = {}
    campaign_answer_1["analysis_item"] = '1，找出 campaign 广告活动 中的  优质 与 劣质广告活动'
    campaign_answer_1["description"] = '分五步执行'
    campaign_answer_1["analysis_process"] = campaign_analysis_process_1

    campaign_answer = []
    campaign_answer.append(campaign_answer_1)
    #
    # # 2，找出 campaign 广告活动 中的  placement优质位置 与 劣质位置
    # campaign_analysis_process_2 = []
    # campaign_2_q1 = "2.1，在 {} 至 {} 这段时间内，{}_SP广告的 平均ACOS ，总广告消耗， campaign 广告活动中 placement 数量 和 总点击量 分别是多少？".format(startdate,endate,market)
    # campaign_2_a1 = amazon.get_sp_campaignplacement_info(market,startdate,endate)
    # campaign_analysis_process_2.append({"question": campaign_2_q1, "answer": campaign_2_a1})
    # campaign_2_q2 = "2.2[优质位置]，在 {} 至 {} 这段时间内，{}_SP广告中 低于 平均ACOS值（替换为第一问结论） 10% 的  campaign 广告活动中placement 数量 和 总点击量 是多少".format(startdate,endate,market)
    # campaign_2_a2 = amazon.get_sp_campaignplacement_below10(market,startdate,endate)
    # campaign_analysis_process_2.append({"question": campaign_2_q2, "answer": campaign_2_a2})
    # campaign_2_q3 = "2.3[优质位置]，找出在 {} 至 {} 这段时间内，{}_SP广告中 低于 平均ACOS值（替换为第一问结论） 30% 以上的  campaign 广告活动中 placement。将这些信息生成csv文件，里面记录这些关键词的以下信息，CPC， ACOS,  Clicks，campaignid，spend， placement.".format(startdate,endate,market)
    # campaign_2_a3 = amazon.get_sp_campaignplacement_below30(market,startdate,endate)
    # campaign_analysis_process_2.append({"question": campaign_2_q3, "answer": campaign_2_a3})
    # campaign_2_q4 = "2.4[优质位置]，找出在 {} 至 {} 这段时间内，{}_SP广告中 低于 平均ACOS值（替换为第一问结论） 20% - 30% 的 campaign 广告活动中 placement。将这些信息生成csv文件，里面记录这些关键词的以下信息，CPC， ACOS,  Clicks，campaignid，spend， placement.".format(startdate,endate,market)
    # campaign_2_a4 = amazon.get_sp_campaignplacement_below20to30(market,startdate,endate)
    # campaign_analysis_process_2.append({"question": campaign_2_q4, "answer": campaign_2_a4})
    # campaign_2_q5 = "2.5[优质位置]，找出在 {} 至 {} 这段时间内，{}_SP广告中 低于 平均ACOS值（替换为第一问结论） 10% - 20% 的 campaign 广告活动中的 placement。将这些信息生成csv文件，里面记录这些关键词的以下信息，CPC， ACOS,  Clicks，campaignid，spend， placement.".format(startdate,endate,market)
    # campaign_2_a5 = amazon.get_sp_campaignplacement_below10to20(market,startdate,endate)
    # campaign_analysis_process_2.append({"question": campaign_2_q5, "answer": campaign_2_a5})
    #
    # campaign_2_q6 = "2.6[劣质位置]，在 {} 至 {} 这段时间内，{}_SP广告中 高于 平均ACOS值（替换为第一问结论） 10% 的  campaign 广告活动中placement 数量 和 总点击量 是多少".format(
    #     startdate, endate, market)
    # campaign_2_a6 = amazon.apcampaign326(market,startdate,endate)
    # campaign_analysis_process_2.append({"question": campaign_2_q6, "answer": campaign_2_a6})
    # campaign_2_q7 = "2.7[劣质位置]，找出在 {} 至 {} 这段时间内，{}_SP广告中 高于 平均ACOS值（替换为第一问结论） 30% 以上的  campaign 广告活动中 placement。将这些信息生成csv文件，里面记录这些关键词的以下信息，CPC， ACOS,  Clicks，campaignid，spend， placement.".format(
    #     startdate, endate, market)
    # campaign_2_a7 = amazon.apcampaign327(market,startdate,endate)
    # campaign_analysis_process_2.append({"question": campaign_2_q7, "answer": campaign_2_a7})
    # campaign_2_q8 = "2.8[劣质位置]，找出在 {} 至 {} 这段时间内，{}_SP广告中 高于 平均ACOS值（替换为第一问结论） 20% - 30% 的 campaign 广告活动中 placement。将这些信息生成csv文件，里面记录这些关键词的以下信息，CPC， ACOS,  Clicks，campaignid，spend， placement.".format(
    #     startdate, endate, market)
    # campaign_2_a8 = amazon.apcampaign328(market,startdate,endate)
    # campaign_analysis_process_2.append({"question": campaign_2_q8, "answer": campaign_2_a8})
    # campaign_2_q9 = "2.9[劣质位置]，找出在 {} 至 {} 这段时间内，{}_SP广告中 高于 平均ACOS值（替换为第一问结论） 10% - 20% 的 campaign 广告活动中的 placement。将这些信息生成csv文件，里面记录这些关键词的以下信息，CPC， ACOS,  Clicks，campaignid，spend， placement.".format(
    #     startdate, endate, market)
    # campaign_2_a9 = amazon.apcampaign329(market,startdate,endate)
    # campaign_analysis_process_2.append({"question": campaign_2_q9, "answer": campaign_2_a9})
    # campaign_answer_2 = {}
    # campaign_answer_2["analysis_item"] = '2，找出 campaign 广告活动 中的  placement优质位置 与 劣质位置'
    # campaign_answer_2["description"] = '分五步执行'
    # campaign_answer_2["analysis_process"] = campaign_analysis_process_2
    # campaign_answer.append(campaign_answer_2)
    #
    #
    # # 3，找出 广告 adgroup 中的  优质 与 劣质广告组
    # campaign_analysis_process_3 = []
    # campaign_3_q1 = "3.1，在 {} 至 {} 这段时间内，{}_SP广告的 平均ACOS（sum(cost)/sum(sales14d)） ，总广告消耗， 广告 adgroup 数量 和 总点击量 分别是多少？".format(startdate,endate,market)
    # campaign_3_a1 = "暂时无SQL"
    # campaign_analysis_process_3.append({"question": campaign_3_q1, "answer": campaign_3_a1})
    # campaign_3_q2 = "3.2[优质广告]，在 {} 至 {} 这段时间内，{}_SP广告中 低于 平均ACOS值（20.38%） 10% 的  广告 adgroup 数量 数量 和 总点击量 是多少".format(startdate,endate,market)
    # campaign_3_a2 = amazon.adcampaigm331(market,startdate,endate)
    # campaign_analysis_process_3.append({"question": campaign_3_q2, "answer": campaign_3_a2})
    # campaign_3_q3 = "3.3[优质广告]，找出在 {} 至 {} 这段时间内，{}_SP广告中 低于 平均ACOS值（20.38%） 30% 以上的  广告 adgroup。将这些信息生成csv文件，里面记录这些关键词的以下信息，CPC， ACOS,  Clicks，adgroupid，spend.".format(startdate,endate,market)
    # campaign_3_a3 = amazon.adcampaigm333(market,startdate,endate)
    # campaign_analysis_process_3.append({"question": campaign_3_q3, "answer": campaign_3_a3})
    # campaign_3_q4 = "3.4[优质广告]，找出在 {} 至 {} 这段时间内，{}_SP广告中 ACOS 介于 14.2% 与16.3% 之间的 广告 adgroup 。将这些信息生成csv文件，文件命名优质_4.csv，里面记录这些关键词的以下信息，CPC， ACOS, Clicks，adgroupid，spend.[ ACOS =（sum(cost)/sum(sales14d))，查找amazon_ad_group_reports_sp表]".format(startdate,endate,market)
    # campaign_3_a4 = amazon.adcampaigm334(market,startdate,endate)
    # campaign_analysis_process_3.append({"question": campaign_3_q4, "answer": campaign_3_a4})
    # campaign_3_q5 = "3.5[优质广告]，找出在 {} 至 {} 这段时间内，{}_SP广告中 低于 平均ACOS值（20.38%） 10% - 20% 的 广告 adgroup。将这些信息生成csv文件，文件命名优质_5.csv，里面记录这些关键词的以下信息，CPC， ACOS,  Clicks，adgroupid，spend.".format(startdate,endate,market)
    # campaign_3_a5 = amazon.adcampaigm335(market,startdate,endate)
    # campaign_analysis_process_3.append({"question": campaign_3_q5, "answer": campaign_3_a5})
    #
    # campaign_analysis_process_3.append({"question": campaign_3_q1, "answer": campaign_3_a1})
    # campaign_3_q6 = "3.6[劣质广告]，在 {} 至 {} 这段时间内，{}_SP广告中 高于 平均ACOS值（20.38%） 10% 的  广告 adgroup 数量 数量 和 总点击量 是多少".format(startdate,endate,market)
    # campaign_3_a6 = amazon.adcampaigm336(market,startdate,endate)
    # campaign_analysis_process_3.append({"question": campaign_3_q6, "answer": campaign_3_a6})
    # campaign_3_q7 = "3.7[劣质广告]，找出在 {} 至 {} 这段时间内，{}_SP广告中 高于 平均ACOS值（20.38%） 30% 以上的  广告 adgroup。将这些信息生成csv文件，里面记录这些关键词的以下信息，CPC， ACOS,  Clicks，adgroupid，spend.".format(startdate,endate,market)
    # campaign_3_a7 = amazon.adcampaigm337(market,startdate,endate)
    # campaign_analysis_process_3.append({"question": campaign_3_q7, "answer": campaign_3_a7})
    # campaign_3_q8 = "3.8[劣质广告]，找出在 {} 至 {} 这段时间内，{}_SP广告中 高于（20-30%）ACOS 介于 14.2% 与16.3% 之间的 广告 adgroup 。将这些信息生成csv文件，文件命名优质_4.csv，里面记录这些关键词的以下信息，CPC， ACOS, Clicks，adgroupid，spend.[ ACOS =（sum(cost)/sum(sales14d))，查找amazon_ad_group_reports_sp表]".format(startdate,endate,market)
    # campaign_3_a8 = amazon.adcampaigm338(market,startdate,endate)
    # campaign_analysis_process_3.append({"question": campaign_3_q8, "answer": campaign_3_a8})
    # campaign_3_q9 = "3.9[劣质广告]，找出在 {} 至 {} 这段时间内，{}_SP广告中 高于 平均ACOS值（20.38%） 10% - 20% 的 广告 adgroup。将这些信息生成csv文件，文件命名优质_5.csv，里面记录这些关键词的以下信息，CPC， ACOS,  Clicks，adgroupid，spend.".format(startdate,endate,market)
    # campaign_3_a9 = amazon.adcampaigm339(market,startdate,endate)
    # campaign_analysis_process_3.append({"question": campaign_3_q9, "answer": campaign_3_a9})

    # campaign_answer_3 = {}
    # campaign_answer_3["analysis_item"] = '3，找出 广告 adgroup 中的  优质 与 劣质广告组'
    # campaign_answer_3["description"] = '分五步执行'
    # campaign_answer_3["analysis_process"] = campaign_analysis_process_3
    # campaign_answer.append(campaign_answer_3)
    #
    #
    # 广告计划question
    campaign_question={}
    campaign_question["question"]={'report_name': '3.广告计划优化分析',
          'description': '该报告将展示每个客户的购买情况，以了解公司的主要客户群体。'}
    campaign_question["answer"]=campaign_answer
    # 关键词部分完成
    all_report_question.append(campaign_question)



    """执行上述构建"""
    last_answer["report_question"] = all_report_question
    # 增加report_thought
    last_answer["report_thought"]=REPORT_THOUGHT
    # 增加report_analyst
    last_answer["report_analyst"] = REPORT_ANALYST

    print(last_answer)
    return last_answer



# res = auto_generepot('US','2024-04-01','2024-04-14')
def generate():
    last_answer = auto_generepot('FR', 'DE', '2024-04-23', '2024-04-30')
    data = last_answer
    # 给定的数据
    print(last_answer)
    # 读取模板文件
    with open('report_3.html', 'r', encoding='utf-8') as file:
        template_str = file.read()

    # 使用Jinja2渲染模板
    timestamp = int(time.time() * 1000)
    template = Template(template_str)
    rendered_html = template.render(data, timestamp=timestamp)

    # 将渲染后的HTML写入文件
    with open('output_FR_0429.html', 'w', encoding='utf-8') as output_file:
        output_file.write(rendered_html)

    print("HTML文件已生成：output_FR_0429.html")

# generate()
def main():
    # 在这里调用 generate() 函数
    generate()

if __name__ == "__main__":
    main()
