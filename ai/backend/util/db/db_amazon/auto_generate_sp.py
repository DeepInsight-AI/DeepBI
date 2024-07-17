import csv
import json

from flask import render_template, Flask

from amazon_mysql_rag_util_sp import AmazonMysqlRagUitl
from auto_content import REPORT_THOUGHT,REPORT_ANALYST
# -*- coding: utf-8 -*-
from jinja2 import Template
import time
import pandas as pd


def auto_generepot(market,startdate,endate):
    """该报告针对指定市场指定日期范围内的SP广告进行生成"""
    db_info = {'host': '192.168.5.114', 'user': 'test_deepdata', 'passwd': 'test123!@#', 'port': 3308,
               'db': 'amazon_ads',
               'charset': 'utf8mb4', 'use_unicode': True, }

    dwx = AmazonMysqlRagUitl(db_info)

    last_answer = {}

    # 设置标题
    report_name = "amazon_SP_"+market+"体检报告_"+startdate+"_"+endate
    last_answer["report_name"]=report_name

    # 增加问题
    all_report_question=[]
    """构建"""

    # 构建1.关键词优化
    # 1，添加未在投放中的 优质 用户搜索关键词到 定向投放关键词计划中
    keyword_analysis_process_1 = []
    keyword_1_q1="1.1 在 {} 至 {} 这段时间内，{}_SP广告的 平均ACOS ，总广告消耗，去重后用户搜索关键词 数量 和 总点击量 分别是多少？".format(startdate,endate,market)
    keyword_1_a1=dwx.get_sp_searchterm_keyword_info(market,startdate,endate)
    keyword_analysis_process_1.append({"question":keyword_1_q1,"answer":keyword_1_a1})
    keyword_1_q2 = "1.2 在 {} 至 {} 这段时间内，{}_SP广告中 低于 平均ACOS值（替换为第一问结论） 10% 的 去重后用户搜索关键词 数量 和 总点击量 是多少".format(startdate,endate,market)
    keyword_1_a2 = dwx.get_sp_searchterm_keyword_info_belowavg10per(market,startdate,endate)
    keyword_analysis_process_1.append({"question":keyword_1_q2,"answer":keyword_1_a2})
    keyword_1_q3 = "1.3 在 {} 至 {} 这段时间内，{}_SP广告中 低于 平均ACOS值（替换为第一问结论） 10% 的用户搜索关键词 中， 没有在广告定向投放中出现的关键词数量 和 总点击量 是多少".format(startdate,endate,market)
    keyword_1_a3 = dwx.get_sp_searchterm_keyword_info_belowavg10toper_notintarget1(market,startdate,endate)
    keyword_analysis_process_1.append({"question":keyword_1_q3,"answer":keyword_1_a3})
    keyword_1_q4 = "1.4 找出 在 {} 至 {} 这段时间内，{}_SP广告中 低于 平均ACOS值（替换为第一问结论） 10%  ，并且 没有在广告定向投放中出现的 用户搜索关键词。 将这些关键词信息生成csv文件，里面记录这些关键词的以下信息，CPC，SKU/ASIN， ACOS, Clicks.".format(startdate,endate,market)
    keyword_1_a4 = dwx.get_sp_searchterm_keyword_info_belowavg10toper_notintarget(market,startdate,endate)
    keyword_analysis_process_1.append({"question":keyword_1_q4,"answer":keyword_1_a4})

    keyword_answer_1 = {}
    keyword_answer_1["analysis_item"] = '1.添加未在投放中的 优质 用户搜索关键词到 定向投放关键词计划中'
    keyword_answer_1["description"] = '分四步执行'
    keyword_answer_1["analysis_process"] = keyword_analysis_process_1

    keyword_answer = []
    keyword_answer.append(keyword_answer_1)

    # 2.找出 定向投放关键词 中的 优质关键词，提价操作
    keyword_analysis_process_2 = []
    keyword_2_q1 = "2.1，在 {} 至 {} 这段时间内，{}_SP广告的 平均ACOS ，总广告消耗，去重后定向投放关键词 数量 和 总点击量 分别是多少？".format(startdate,endate,market)
    keyword_2_a1 = dwx.get_sp_searchterm_keyword_info_target(market,startdate,endate)
    keyword_analysis_process_2.append({"question": keyword_2_q1, "answer": keyword_2_a1})
    keyword_2_q2 = "2.2，在 {} 至 {} 这段时间内，{}_SP广告中 低于 平均ACOS值（替换为第一问结论） 10% 的 去重后定向投放关键词 数量 和 总点击量 是多少".format(startdate,endate,market)
    keyword_2_a2 = dwx.get_sp_searchterm_keyword_info_target_below10per(market,startdate,endate)
    keyword_analysis_process_2.append({"question": keyword_2_q2, "answer": keyword_2_a2})
    keyword_2_q3 = "2.3，找出在 {} 至 {} 这段时间内，{}_SP广告中 低于 平均ACOS值（替换为第一问结论） 30% 以上的 定向投放关键词。将这些关键词信息生成csv文件，里面记录这些关键词的以下信息，CPC，SKU/ASIN， ACOS, Clicks，adgroupid.".format(startdate,endate,market)
    keyword_2_a3 = dwx.get_sp_searchterm_keyword_info_target_below30per_csv(market,startdate,endate)
    keyword_analysis_process_2.append({"question": keyword_2_q3, "answer": keyword_2_a3})
    keyword_2_q4 = "2.4，找出在 {} 至 {} 这段时间内，{}_SP广告中 低于 平均ACOS值（替换为第一问结论） 20% - 30% 的 定向投放关键词。将这些关键词信息生成csv文件，里面记录这些关键词的以下信息，CPC，SKU/ASIN， ACOS, Clicks，adgroupid.".format(startdate,endate,market)
    keyword_2_a4 = dwx.get_sp_searchterm_keyword_info_target_20and30per_csv(market,startdate,endate)
    keyword_analysis_process_2.append({"question": keyword_2_q4, "answer": keyword_2_a4})
    keyword_2_q5 = "2.5，找出在 {} 至 {} 这段时间内，{}_SP广告中 低于 平均ACOS值（替换为第一问结论） 10% - 20% 的 定向投放关键词。将这些关键词信息生成csv文件，里面记录这些关键词的以下信息，CPC，SKU/ASIN， ACOS, Clicks，adgroupid.".format(startdate,endate,market)
    keyword_2_a5 = dwx.get_sp_searchterm_keyword_info_target_10and20per_csv(market,startdate,endate)
    keyword_analysis_process_2.append({"question": keyword_2_q5, "answer": keyword_2_a5})
    keyword_answer_2 = {}
    keyword_answer_2["analysis_item"] = '2.找出 定向投放关键词 中的 优质关键词，提价操作'
    keyword_answer_2["description"] = '分五步执行'
    keyword_answer_2["analysis_process"] = keyword_analysis_process_2
    keyword_answer.append(keyword_answer_2)

    # 3.同类型产品广告计划中，相关的优秀关键词应该全覆盖
    keyword_analysis_process_3 = []
    keyword_3_q1 = "3.1，将ad group 分组， 出现过同样 SKU/ASIN 的ad group  分到一组。 {}_SP广告中 ad group 可以分为多少产品类别小组？按将这些关键词信息生成csv文件，里面记录这些关键词的以下信息，CPC，SKU/ASIN， ACOS, Clicks.".format(market)
    keyword_3_a1 = "暂时无法分析关键词的类型，暂时未分析"
    keyword_analysis_process_3.append({"question": keyword_3_q1, "answer": keyword_3_a1})
    keyword_3_q2 = "3.2，在 {} 至 {} 这段时间内，{}_SP广告中 低于 平均ACOS值（替换为第一问结论） 10% 的用户搜索关键词 中，分配到各  ad group 产品类别小组 的关键词是哪些？将这些关键词信息生成csv文件，里面记录这些关键词的以下信息，CPC，SKU/ASIN， ACOS, Clicks.".format(startdate,endate,market)
    keyword_3_a2 = "暂时无法分析关键词的类型，暂时未分析"
    keyword_analysis_process_3.append({"question": keyword_3_q2, "answer": keyword_3_a2})
    keyword_3_q3 = "3.3，同ad group 类别小组 中， 按产品类型对其中的关键词分类，是否有相关的优秀关键词没有全覆盖。 哪些关键词没有在计划中"
    keyword_3_a3 = "暂时无法分析关键词的类型，暂时未分析"
    keyword_analysis_process_3.append({"question": keyword_3_q3, "answer": keyword_3_a3})
    keyword_answer_3 = {}
    keyword_answer_3["analysis_item"] = '3.同类型产品广告计划中，相关的优秀关键词应该全覆盖'
    keyword_answer_3["description"] = '分三步执行'
    keyword_answer_3["analysis_process"] = keyword_analysis_process_3
    keyword_answer.append(keyword_answer_3)

    # 4.找出 定向投放关键词 中的 劣质关键词
    keyword_analysis_process_4 = []
    keyword_4_q1 = "4.1，在 {} 至 {} 这段时间内，{}_SP广告的 平均ACOS ，总广告消耗，去重后定向投放关键词 数量 和 总点击量 分别是多少？".format(startdate,endate,market)
    keyword_4_a1 = dwx.get_sp__keyword_info_targetacos(market,startdate,endate)
    keyword_analysis_process_4.append({"question": keyword_4_q1, "answer": keyword_4_a1})
    keyword_4_q2 = "4.2，在 {} 至 {} 这段时间内，{}_SP广告中 高于 平均ACOS值（替换为第一问结论） 10% 的 去重后定向投放关键词 数量 和 总点击量 是多少".format(startdate,endate,market)
    keyword_4_a2 = dwx.get_sp_keyword_target_up10per(market,startdate,endate)
    keyword_analysis_process_4.append({"question": keyword_4_q2, "answer": keyword_4_a2})
    keyword_4_q3 = "4.3，找出在 {} 至 {} 这段时间内，{}_SP广告中 高 平均ACOS值（替换为第一问结论） 30% 以上的 定向投放关键词。将这些关键词信息生成csv文件，里面记录这些关键词的以下信息，CPC，SKU/ASIN， ACOS,  Clicks，adgroupid.".format(startdate,endate,market)
    keyword_4_a3 = dwx.get_sp_searchterm_keyword_info_target_up30per_csv(market,startdate,endate)
    keyword_analysis_process_4.append({"question": keyword_4_q3, "answer": keyword_4_a3})
    keyword_4_q4 = "4.4，找出在 {} 至 {} 这段时间内，{}_SP广告中 高于 平均ACOS值（替换为第一问结论） 20% - 30% 的 定向投放关键词。将这些关键词信息生成csv文件，里面记录这些关键词的以下信息，CPC，SKU/ASIN， ACOS, Clicks，adgroupid.".format(startdate,endate,market)
    keyword_4_a4 = dwx.get_sp_searchterm_keyword_info_target_up20to30per_csv(market,startdate,endate)
    keyword_analysis_process_4.append({"question": keyword_4_q4, "answer": keyword_4_a4})
    keyword_4_q5 = "4.5，找出在 {} 至 {} 这段时间内，{}_SP广告中 高于 平均ACOS值（替换为第一问结论） 10% - 20% 的 定向投放关键词。将这些关键词信息生成csv文件，里面记录这些关键词的以下信息，CPC，SKU/ASIN， ACOS, Clicks，adgroupid.".format(startdate,endate,market)
    keyword_4_a5 = dwx.get_sp_searchterm_keyword_info_target_up10to20per_csv(market,startdate,endate)
    keyword_analysis_process_4.append({"question": keyword_4_q5, "answer": keyword_4_a5})
    keyword_answer_4 = {}
    keyword_answer_4["analysis_item"] = '4.找出 定向投放关键词 中的 劣质关键词'
    keyword_answer_4["description"] = '分五步执行'
    keyword_answer_4["analysis_process"] = keyword_analysis_process_4
    keyword_answer.append(keyword_answer_4)

    # 关键词question
    keyword_question={}
    keyword_question["question"]={'report_name': '1.关键词优化分析',
          'description': '目标：关键词的扩充机会，来自于 扩词，通过优秀关键词的共同特征，扩充关键词。  用户搜索的关键词，没有在手动投放关键词中的。扩品，优秀关键词都投放在哪些产品上，这些品的特征是什么。是否有符合特征的品，没有添加过（投放过）这些关键词？扩点击量，哪些关键词还有提价空间\n补充：按产品类型对其中的关键词分类，目标是 同类型产品广告计划中，相关的优秀关键词应该全覆盖。 哪些关键词没有在计划中，可以扩充'}
    keyword_question["answer"]=keyword_answer
    # 关键词部分完成
    all_report_question.append(keyword_question)


    # 商品优化部分
    # 1，找出 sku广告 中的 优质广告
    product_analysis_process_1 = []
    product_1_q1 = "1.1，在 {} 至 {} 这段时间内，{}_SP广告的 平均ACOS ，总广告消耗，sku广告数量 和 总点击量 分别是多少？".format(startdate,endate,market)
    product_1_a1 = dwx.get_sp_product_info(market,startdate,endate)
    product_analysis_process_1.append({"question": product_1_q1, "answer": product_1_a1})
    product_1_q2 = "1.2，在 {} 至 {} 这段时间内，{}_SP广告中 低于 平均ACOS值（替换为第一问结论） 10% 的 sku广告数量 数量 和 总点击量 是多少".format(startdate,endate,market)
    product_1_a2 = dwx.get_sp_product_acosblow10(market,startdate,endate)
    product_analysis_process_1.append({"question": product_1_q2, "answer": product_1_a2})
    product_1_q3 = "1.3，在 {} 至 {} 这段时间内，{}_SP广告中 低于 平均ACOS值（替换为第一问结论） 30% 以上的 sku广告。将这些信息生成csv文件，里面记录这些关键词的以下信息，CPC，SKU/ASIN， ACOS,  Clicks，adgroupid.".format(startdate,endate,market)
    product_1_a3 = dwx.get_sp_product_acosblow30(market,startdate,endate)
    product_analysis_process_1.append({"question": product_1_q3, "answer": product_1_a3})
    product_1_q4 = "1.4，找出在 {} 至 {} 这段时间内，{}_SP广告中 低于 平均ACOS值（替换为第一问结论） 20% - 30% 的 sku广告。将这些信息生成csv文件，里面记录这些关键词的以下信息，CPC，SKU/ASIN， ACOS, Clicks，adgroupid.".format(startdate,endate,market)
    product_1_a4 = dwx.get_sp_product_belowacos20to30(market,startdate,endate)
    product_analysis_process_1.append({"question": product_1_q4, "answer": product_1_a4})
    product_1_q5 = "1.5，找出在 {} 至 {} 这段时间内，{}_SP广告中 低于 平均ACOS值（替换为第一问结论） 10% - 20% 的 sku广告。将这些信息生成csv文件，里面记录这些关键词的以下信息，CPC，SKU/ASIN， ACOS, Clicks，adgroupid.".format(startdate,endate,market)
    product_1_a5 = dwx.get_sp_product_belowacos10to20(market,startdate,endate)
    product_analysis_process_1.append({"question": product_1_q5, "answer": product_1_a5})

    product_answer_1 = {}
    product_answer_1["analysis_item"] = '1，找出 sku广告 中的 优质广告'
    product_answer_1["description"] = '分五步执行'
    product_answer_1["analysis_process"] = product_analysis_process_1

    product_answer = []
    product_answer.append(product_answer_1)

    # 2，找出 sku广告 中的 劣质广告
    product_analysis_process_2 = []
    product_2_q1 = "2.1，在 {} 至 {} 这段时间内，{}_SP广告的 平均ACOS ，总广告消耗，sku广告数量 和 总点击量 分别是多少？".format(startdate,endate,market)
    product_2_a1 = dwx.get_sp_product_info_2(market,startdate,endate)
    product_analysis_process_2.append({"question": product_2_q1, "answer": product_2_a1})
    product_2_q2 = "2.2，在 {} 至 {} 这段时间内，{}_SP广告中 高于 平均ACOS值（替换为第一问结论） 10% 的 sku广告数量 数量 和 总点击量 是多少".format(startdate,endate,market)
    product_2_a2 = dwx.get_sp_product_acosup10(market,startdate,endate)
    product_analysis_process_2.append({"question": product_2_q2, "answer": product_2_a2})
    product_2_q3 = "2.3，找出在 {} 至 {} 这段时间内，{}_SP广告中 高于 平均ACOS值（替换为第一问结论） 30% 以上的 sku广告。将这些信息生成csv文件，里面记录这些关键词的以下信息，CPC，SKU/ASIN， ACOS,  Clicks，adgroupid.".format(startdate,endate,market)
    product_2_a3 = dwx.get_sp_product_acosup30(market,startdate,endate)
    product_analysis_process_2.append({"question": product_2_q3, "answer": product_2_a3})
    product_2_q4 = "2.4，找出在 {} 至 {} 这段时间内，{}_SP广告中 高于 平均ACOS值（替换为第一问结论） 20% - 30% 的 sku广告。将这些信息生成csv文件，里面记录这些关键词的以下信息，CPC，SKU/ASIN， ACOS, Clicks，adgroupid.".format(startdate,endate,market)
    product_2_a4 = dwx.get_sp_product_upacos20to30(market,startdate,endate)
    product_analysis_process_2.append({"question": product_2_q4, "answer": product_2_a4})
    product_2_q5 = "2.5，找出在 {} 至 {} 这段时间内，{}_SP广告中 高于 平均ACOS值（替换为第一问结论） 10% - 20% 的 sku广告。将这些信息生成csv文件，里面记录这些关键词的以下信息，CPC，SKU/ASIN， ACOS, Clicks，adgroupid.".format(startdate,endate,market)
    product_2_a5 = dwx.get_sp_product_upacos10to20(market,startdate,endate)
    product_analysis_process_2.append({"question": product_2_q5, "answer": product_2_a5})

    product_answer_2 = {}
    product_answer_2["analysis_item"] = '2，找出 sku广告 中的 劣质广告'
    product_answer_2["description"] = '分五步执行'
    product_answer_2["analysis_process"] = product_analysis_process_2
    product_answer.append(product_answer_2)


    # 产品question
    product_question = {}
    product_question["question"] = {'report_name': '2.商品优化分析',
                                    'description': '##补充：找出ACOS表现较好的品和ACOS表现较差的品进行调整'}
    product_question["answer"] = product_answer
    # 产品部分完成
    all_report_question.append(product_question)

    # 广告计划优化分析
    # 1，找出 campaign 广告活动 中的  优质 与 劣质广告活动
    campaign_analysis_process_1 = []
    campaign_1_q1="1.1，在 {} 至 {} 这段时间内，{}_SP广告的 平均ACOS ，总广告消耗， campaign 广告活动数量 和 总点击量 分别是多少？".format(startdate,endate,market)
    campaign_1_a1=dwx.get_sp_campaign_info(market,startdate,endate)
    campaign_analysis_process_1.append({"question":campaign_1_q1,"answer":campaign_1_a1})
    campaign_1_q2 = "1.2[优质广告]，在 {} 至 {} 这段时间内，{}_SP广告中 低于 平均ACOS值（替换为第一问结论） 10% 的  campaign 广告活动数量 数量 和 总点击量 是多少".format(startdate,endate,market)
    campaign_1_a2 = dwx.get_sp_campaign_below10(market,startdate,endate)
    campaign_analysis_process_1.append({"question":campaign_1_q2,"answer":campaign_1_a2})
    campaign_1_q3 = "1.3[优质广告]，找出在 {} 至 {} 这段时间内，{}_SP广告中 低于 平均ACOS值（替换为第一问结论） 30% 以上的  campaign 广告活动。将这些信息生成csv文件，里面记录这些关键词的以下信息，CPC， ACOS,  Clicks，campaignid，spend.".format(startdate,endate,market)
    campaign_1_a3 = dwx.get_sp_campaign_below30(market,startdate,endate)
    campaign_analysis_process_1.append({"question":campaign_1_q3,"answer":campaign_1_a3})
    campaign_1_q4 = "1.4[优质广告]，找出在 {} 至 {} 这段时间内，{}_SP广告中 低于 平均ACOS值（替换为第一问结论） 20% - 30% 的 campaign 广告活动。将这些信息生成csv文件，里面记录这些关键词的以下信息，CPC， ACOS,  Clicks，campaignid，spend.".format(startdate,endate,market)
    campaign_1_a4 = dwx.get_sp_campaign_below20to30(market,startdate,endate)
    campaign_analysis_process_1.append({"question":campaign_1_q4,"answer":campaign_1_a4})
    campaign_1_q5 = "1.5[优质广告]，找出在 {} 至 {} 这段时间内，{}_SP广告中 低于 平均ACOS值（替换为第一问结论） 10% - 20% 的 campaign 广告活动。将这些信息生成csv文件，里面记录这些关键词的以下信息，CPC， ACOS,  Clicks，campaignid，spend.".format(startdate,endate,market)
    campaign_1_a5 = dwx.get_sp_campaign_below10to20(market,startdate,endate)
    campaign_analysis_process_1.append({"question":campaign_1_q5,"answer":campaign_1_a5})
    campaign_1_q6 = "1.6[劣质广告]，在 {} 至 {} 这段时间内，{}_SP广告中 高于 平均ACOS值（替换为第一问结论） 10% 的  campaign 广告活动数量 数量 和 总点击量 是多少".format(
        startdate, endate, market)
    campaign_1_a6 = dwx.spcampaign316(market,startdate,endate)
    campaign_analysis_process_1.append({"question": campaign_1_q6, "answer": campaign_1_a6})
    campaign_1_q7 = "1.7[劣质广告]，找出在 {} 至 {} 这段时间内，{}_SP广告中 高于 平均ACOS值（替换为第一问结论） 30% 以上的  campaign 广告活动。将这些信息生成csv文件，里面记录这些关键词的以下信息，CPC， ACOS,  Clicks，campaignid，spend.".format(
        startdate, endate, market)
    campaign_1_a7 = dwx.spcampaign317(market,startdate,endate)
    campaign_analysis_process_1.append({"question": campaign_1_q7, "answer": campaign_1_a7})
    campaign_1_q8 = "1.8[劣质广告]，找出在 {} 至 {} 这段时间内，{}_SP广告中 高于 平均ACOS值（替换为第一问结论） 20% - 30% 的 campaign 广告活动。将这些信息生成csv文件，里面记录这些关键词的以下信息，CPC， ACOS,  Clicks，campaignid，spend.".format(
        startdate, endate, market)
    campaign_1_a8 = dwx.apcampaign318(market,startdate,endate)
    campaign_analysis_process_1.append({"question": campaign_1_q8, "answer": campaign_1_a8})
    campaign_1_q9 = "1.9[劣质广告]，找出在 {} 至 {} 这段时间内，{}_SP广告中 高于 平均ACOS值（替换为第一问结论） 10% - 20% 的 campaign 广告活动。将这些信息生成csv文件，里面记录这些关键词的以下信息，CPC， ACOS,  Clicks，campaignid，spend.".format(
        startdate, endate, market)
    campaign_1_a9 = dwx.apcampaign319(market,startdate,endate)
    campaign_analysis_process_1.append({"question": campaign_1_q9, "answer": campaign_1_a9})

    campaign_answer_1 = {}
    campaign_answer_1["analysis_item"] = '1，找出 campaign 广告活动 中的  优质 与 劣质广告活动'
    campaign_answer_1["description"] = '分五步执行'
    campaign_answer_1["analysis_process"] = campaign_analysis_process_1

    campaign_answer = []
    campaign_answer.append(campaign_answer_1)

    # 2，找出 campaign 广告活动 中的  placement优质位置 与 劣质位置
    campaign_analysis_process_2 = []
    campaign_2_q1 = "2.1，在 {} 至 {} 这段时间内，{}_SP广告的 平均ACOS ，总广告消耗， campaign 广告活动中 placement 数量 和 总点击量 分别是多少？".format(startdate,endate,market)
    campaign_2_a1 = dwx.get_sp_campaignplacement_info(market,startdate,endate)
    campaign_analysis_process_2.append({"question": campaign_2_q1, "answer": campaign_2_a1})
    campaign_2_q2 = "2.2[优质位置]，在 {} 至 {} 这段时间内，{}_SP广告中 低于 平均ACOS值（替换为第一问结论） 10% 的  campaign 广告活动中placement 数量 和 总点击量 是多少".format(startdate,endate,market)
    campaign_2_a2 = dwx.get_sp_campaignplacement_below10(market,startdate,endate)
    campaign_analysis_process_2.append({"question": campaign_2_q2, "answer": campaign_2_a2})
    campaign_2_q3 = "2.3[优质位置]，找出在 {} 至 {} 这段时间内，{}_SP广告中 低于 平均ACOS值（替换为第一问结论） 30% 以上的  campaign 广告活动中 placement。将这些信息生成csv文件，里面记录这些关键词的以下信息，CPC， ACOS,  Clicks，campaignid，spend， placement.".format(startdate,endate,market)
    campaign_2_a3 = dwx.get_sp_campaignplacement_below30(market,startdate,endate)
    campaign_analysis_process_2.append({"question": campaign_2_q3, "answer": campaign_2_a3})
    campaign_2_q4 = "2.4[优质位置]，找出在 {} 至 {} 这段时间内，{}_SP广告中 低于 平均ACOS值（替换为第一问结论） 20% - 30% 的 campaign 广告活动中 placement。将这些信息生成csv文件，里面记录这些关键词的以下信息，CPC， ACOS,  Clicks，campaignid，spend， placement.".format(startdate,endate,market)
    campaign_2_a4 = dwx.get_sp_campaignplacement_below20to30(market,startdate,endate)
    campaign_analysis_process_2.append({"question": campaign_2_q4, "answer": campaign_2_a4})
    campaign_2_q5 = "2.5[优质位置]，找出在 {} 至 {} 这段时间内，{}_SP广告中 低于 平均ACOS值（替换为第一问结论） 10% - 20% 的 campaign 广告活动中的 placement。将这些信息生成csv文件，里面记录这些关键词的以下信息，CPC， ACOS,  Clicks，campaignid，spend， placement.".format(startdate,endate,market)
    campaign_2_a5 = dwx.get_sp_campaignplacement_below10to20(market,startdate,endate)
    campaign_analysis_process_2.append({"question": campaign_2_q5, "answer": campaign_2_a5})

    campaign_2_q6 = "2.6[劣质位置]，在 {} 至 {} 这段时间内，{}_SP广告中 高于 平均ACOS值（替换为第一问结论） 10% 的  campaign 广告活动中placement 数量 和 总点击量 是多少".format(
        startdate, endate, market)
    campaign_2_a6 = dwx.apcampaign326(market,startdate,endate)
    campaign_analysis_process_2.append({"question": campaign_2_q6, "answer": campaign_2_a6})
    campaign_2_q7 = "2.7[劣质位置]，找出在 {} 至 {} 这段时间内，{}_SP广告中 高于 平均ACOS值（替换为第一问结论） 30% 以上的  campaign 广告活动中 placement。将这些信息生成csv文件，里面记录这些关键词的以下信息，CPC， ACOS,  Clicks，campaignid，spend， placement.".format(
        startdate, endate, market)
    campaign_2_a7 = dwx.apcampaign327(market,startdate,endate)
    campaign_analysis_process_2.append({"question": campaign_2_q7, "answer": campaign_2_a7})
    campaign_2_q8 = "2.8[劣质位置]，找出在 {} 至 {} 这段时间内，{}_SP广告中 高于 平均ACOS值（替换为第一问结论） 20% - 30% 的 campaign 广告活动中 placement。将这些信息生成csv文件，里面记录这些关键词的以下信息，CPC， ACOS,  Clicks，campaignid，spend， placement.".format(
        startdate, endate, market)
    campaign_2_a8 = dwx.apcampaign328(market,startdate,endate)
    campaign_analysis_process_2.append({"question": campaign_2_q8, "answer": campaign_2_a8})
    campaign_2_q9 = "2.9[劣质位置]，找出在 {} 至 {} 这段时间内，{}_SP广告中 高于 平均ACOS值（替换为第一问结论） 10% - 20% 的 campaign 广告活动中的 placement。将这些信息生成csv文件，里面记录这些关键词的以下信息，CPC， ACOS,  Clicks，campaignid，spend， placement.".format(
        startdate, endate, market)
    campaign_2_a9 = dwx.apcampaign329(market,startdate,endate)
    campaign_analysis_process_2.append({"question": campaign_2_q9, "answer": campaign_2_a9})
    campaign_answer_2 = {}
    campaign_answer_2["analysis_item"] = '2，找出 campaign 广告活动 中的  placement优质位置 与 劣质位置'
    campaign_answer_2["description"] = '分五步执行'
    campaign_answer_2["analysis_process"] = campaign_analysis_process_2
    campaign_answer.append(campaign_answer_2)


    # 3，找出 广告 adgroup 中的  优质 与 劣质广告组
    campaign_analysis_process_3 = []
    campaign_3_q1 = "3.1，在 {} 至 {} 这段时间内，{}_SP广告的 平均ACOS（sum(cost)/sum(sales14d)） ，总广告消耗， 广告 adgroup 数量 和 总点击量 分别是多少？".format(startdate,endate,market)
    campaign_3_a1 = "暂时无SQL"
    campaign_analysis_process_3.append({"question": campaign_3_q1, "answer": campaign_3_a1})
    campaign_3_q2 = "3.2[优质广告]，在 {} 至 {} 这段时间内，{}_SP广告中 低于 平均ACOS值（20.38%） 10% 的  广告 adgroup 数量 数量 和 总点击量 是多少".format(startdate,endate,market)
    campaign_3_a2 = dwx.adcampaigm331(market,startdate,endate)
    campaign_analysis_process_3.append({"question": campaign_3_q2, "answer": campaign_3_a2})
    campaign_3_q3 = "3.3[优质广告]，找出在 {} 至 {} 这段时间内，{}_SP广告中 低于 平均ACOS值（20.38%） 30% 以上的  广告 adgroup。将这些信息生成csv文件，里面记录这些关键词的以下信息，CPC， ACOS,  Clicks，adgroupid，spend.".format(startdate,endate,market)
    campaign_3_a3 = dwx.adcampaigm333(market,startdate,endate)
    campaign_analysis_process_3.append({"question": campaign_3_q3, "answer": campaign_3_a3})
    campaign_3_q4 = "3.4[优质广告]，找出在 {} 至 {} 这段时间内，{}_SP广告中 ACOS 介于 14.2% 与16.3% 之间的 广告 adgroup 。将这些信息生成csv文件，文件命名优质_4.csv，里面记录这些关键词的以下信息，CPC， ACOS, Clicks，adgroupid，spend.[ ACOS =（sum(cost)/sum(sales14d))，查找amazon_ad_group_reports_sp表]".format(startdate,endate,market)
    campaign_3_a4 = dwx.adcampaigm334(market,startdate,endate)
    campaign_analysis_process_3.append({"question": campaign_3_q4, "answer": campaign_3_a4})
    campaign_3_q5 = "3.5[优质广告]，找出在 {} 至 {} 这段时间内，{}_SP广告中 低于 平均ACOS值（20.38%） 10% - 20% 的 广告 adgroup。将这些信息生成csv文件，文件命名优质_5.csv，里面记录这些关键词的以下信息，CPC， ACOS,  Clicks，adgroupid，spend.".format(startdate,endate,market)
    campaign_3_a5 = dwx.adcampaigm335(market,startdate,endate)
    campaign_analysis_process_3.append({"question": campaign_3_q5, "answer": campaign_3_a5})

    campaign_analysis_process_3.append({"question": campaign_3_q1, "answer": campaign_3_a1})
    campaign_3_q6 = "3.6[劣质广告]，在 {} 至 {} 这段时间内，{}_SP广告中 高于 平均ACOS值（20.38%） 10% 的  广告 adgroup 数量 数量 和 总点击量 是多少".format(startdate,endate,market)
    campaign_3_a6 = dwx.adcampaigm336(market,startdate,endate)
    campaign_analysis_process_3.append({"question": campaign_3_q6, "answer": campaign_3_a6})
    campaign_3_q7 = "3.7[劣质广告]，找出在 {} 至 {} 这段时间内，{}_SP广告中 高于 平均ACOS值（20.38%） 30% 以上的  广告 adgroup。将这些信息生成csv文件，里面记录这些关键词的以下信息，CPC， ACOS,  Clicks，adgroupid，spend.".format(startdate,endate,market)
    campaign_3_a7 = dwx.adcampaigm337(market,startdate,endate)
    campaign_analysis_process_3.append({"question": campaign_3_q7, "answer": campaign_3_a7})
    campaign_3_q8 = "3.8[劣质广告]，找出在 {} 至 {} 这段时间内，{}_SP广告中 高于（20-30%）ACOS 介于 14.2% 与16.3% 之间的 广告 adgroup 。将这些信息生成csv文件，文件命名优质_4.csv，里面记录这些关键词的以下信息，CPC， ACOS, Clicks，adgroupid，spend.[ ACOS =（sum(cost)/sum(sales14d))，查找amazon_ad_group_reports_sp表]".format(startdate,endate,market)
    campaign_3_a8 = dwx.adcampaigm338(market,startdate,endate)
    campaign_analysis_process_3.append({"question": campaign_3_q8, "answer": campaign_3_a8})
    campaign_3_q9 = "3.9[劣质广告]，找出在 {} 至 {} 这段时间内，{}_SP广告中 高于 平均ACOS值（20.38%） 10% - 20% 的 广告 adgroup。将这些信息生成csv文件，文件命名优质_5.csv，里面记录这些关键词的以下信息，CPC， ACOS,  Clicks，adgroupid，spend.".format(startdate,endate,market)
    campaign_3_a9 = dwx.adcampaigm339(market,startdate,endate)
    campaign_analysis_process_3.append({"question": campaign_3_q9, "answer": campaign_3_a9})

    campaign_answer_3 = {}
    campaign_answer_3["analysis_item"] = '3，找出 广告 adgroup 中的  优质 与 劣质广告组'
    campaign_answer_3["description"] = '分五步执行'
    campaign_answer_3["analysis_process"] = campaign_analysis_process_3
    campaign_answer.append(campaign_answer_3)


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

    # 新增结果处csv展示
    def getfilepath(filename):
        res="C:\\Users\\1\\Desktop\\api\\generate_report\\"+str(filename.split("：")[-1])
        return res

    # last_answer["result_csvs"]=result_csv

    # 需要增加展示CSV的功能在最后
    csvfiles = [{'title': '1.2.3',
                 'file_path': getfilepath(keyword_2_a3),
                 'introduction': '以下为 低于 平均ACOS值 30% 以上的',
                 'operate': '建议执行的操作：关键词提高出价10%'},
                {'title': '1.2.4',
                 'file_path': getfilepath(keyword_2_a4),
                 'introduction': '以下为 低于 平均ACOS值 20% - 30%',
                 'operate': '建议执行的操作：关键词提高出价5%'},
                {'title': '1.2.5',
                 'file_path': getfilepath(keyword_2_a5),
                 'introduction': '以下为 低于 平均ACOS值 10% - 20% 的',
                 'operate': '建议执行的操作：关键词提高出价3%'},
                {'title': '1.4.3',
                 'file_path': getfilepath(keyword_4_a3),
                 'introduction': '以下为 高于 平均ACOS值 30% 以上的',
                 'operate': '建议执行的操作：关闭关键词投放'},
                {'title': '1.4.4',
                 'file_path': getfilepath(keyword_4_a4),
                 'introduction': '以下为 高于 平均ACOS值 20% - 30% 的',
                 'operate': '建议执行的操作：降低出价 20%'},
                {'title': '1.4.5',
                 'file_path': getfilepath(keyword_4_a5),
                 'introduction': '以下为 高于 平均ACOS值 10% - 20% 的',
                 'operate': '建议执行的操作：降低出价 10%'},

                # 继续
                {'title': '2.1.3',
                 'file_path': getfilepath(product_1_a3),
                 'introduction': '以下为 低于 平均ACOS值 30%以上的 sku广告',
                 'operate': '建议执行的操作：新开计划（关键词和原计划一样），预算增加30%，关键词价格上调10%。 '},
                {'title': '2.1.4',
                 'file_path': getfilepath(product_1_a4),
                 'introduction': '以下为 低于 平均ACOS值 20% - 30% 的 sku广告',
                 'operate': '建议执行的操作：暂不操作'},
                {'title': '2.1.5',
                 'file_path': getfilepath(product_1_a5),
                 'introduction': '以下为 低于 平均ACOS值 10% - 20% 的 sku广告',
                 'operate': '建议执行的操作：暂不操作'},

                {'title': '2.2.3',
                 'file_path': getfilepath(product_2_a3),
                 'introduction': '以下为 高于 平均ACOS值 30%以上的 sku广告',
                 'operate': '建议执行的操作：暂不操作'},
                {'title': '2.2.4',
                 'file_path': getfilepath(product_2_a4),
                 'introduction': '以下为 高于 平均ACOS值 20% - 30% 的 sku广告',
                 'operate': '建议执行的操作：暂不操作'},
                {'title': '2.2.5',
                 'file_path': getfilepath(product_2_a5),
                 'introduction': '以下为 高于 平均ACOS值 10% - 20% 的 sku广告',
                 'operate': '建议执行的操作：暂不操作'},

                {'title': '3.1.3',
                 'file_path': getfilepath(campaign_1_a3),
                 'introduction': '以下为 低于 平均ACOS值30% 以上的 campaign 广告活动',
                 'operate': '建议执行的操作：预算提升30%'},
                {'title': '3.1.4',
                 'file_path': getfilepath(campaign_1_a4),
                 'introduction': '以下为 低于 平均ACOS值 20% - 30% 的 campaign 广告活动',
                 'operate': '建议执行的操作：预算提升20%'},
                {'title': '3.1.5',
                 'file_path': getfilepath(campaign_1_a5),
                 'introduction': '以下为 低于 平均ACOS值 10% - 20% 的 campaign 广告活动',
                 'operate': '建议执行的操作：预算提升10%'},
                {'title': '3.1.7',
                 'file_path': getfilepath(campaign_1_a7),
                 'introduction': '以下为 高于 平均ACOS值30% 以上的 campaign 广告活动',
                 'operate': '建议执行的操作：预算降低30%'},
                {'title': '3.1.8',
                 'file_path': getfilepath(campaign_1_a8),
                 'introduction': '以下为 高于 平均ACOS值 20% - 30% 的 campaign 广告活动',
                 'operate': '建议执行的操作：预算降低20%'},
                {'title': '3.1.9',
                 'file_path': getfilepath(campaign_1_a9),
                 'introduction': '以下为 高于 平均ACOS值 10% - 20% 的 campaign 广告活动',
                 'operate': '建议执行的操作：预算降低10%'},

                {'title': '3.2.3',
                 'file_path': getfilepath(campaign_2_a3),
                 'introduction': '以下为 低于 平均ACOS值30% 以上的 campaign 广告活动placement',
                 'operate': '建议执行的操作：溢价提升10%, （增加）'},
                {'title': '3.2.4',
                 'file_path': getfilepath(campaign_2_a4),
                 'introduction': '以下为 低于 平均ACOS值 20% - 30% 的 campaign 广告活动placement',
                 'operate': '建议执行的操作：溢价提升5%'},
                {'title': '3.2.5',
                 'file_path': getfilepath(campaign_2_a5),
                 'introduction': '以下为 低于 平均ACOS值 10% - 20% 的 campaign 广告活动placement',
                 'operate': '建议执行的操作：溢价提升3%'},
                {'title': '3.2.7',
                 'file_path': getfilepath(campaign_2_a7),
                 'introduction': '以下为 高于 平均ACOS值30% 以上的 campaign 广告活动placement',
                 'operate': '建议执行的操作：溢价清零，归到0%的溢价。'},
                {'title': '3.2.8',
                 'file_path': getfilepath(campaign_2_a8),
                 'introduction': '以下为 高于 平均ACOS值 20% - 30% 的 campaign 广告活动placement',
                 'operate': '建议执行的操作：溢价下调10%，直到0%'},
                {'title': '3.2.9',
                 'file_path': getfilepath(campaign_2_a9),
                 'introduction': '以下为 高于 平均ACOS值 10% - 20% 的 campaign 广告活动placement',
                 'operate': '建议执行的操作：溢价下调5%，直到0%'},

                {'title': '3.3.3',
                 'file_path': getfilepath(campaign_3_a3),
                 'introduction': '以下为 低于 平均ACOS值30% 以上的 campaign 广告 adgroup',
                 'operate': '建议执行的操作：基础出价提升10%， 自动广告组的基础出价'},
                {'title': '3.3.4',
                 'file_path': getfilepath(campaign_3_a4),
                 'introduction': '以下为 低于 平均ACOS值 20% - 30% 的 campaign 广告 adgroup',
                 'operate': '建议执行的操作：基础出价提升5%'},
                {'title': '3.3.5',
                 'file_path': getfilepath(campaign_3_a5),
                 'introduction': '以下为 低于 平均ACOS值 10% - 20% 的 campaign 广告 adgroup',
                 'operate': '建议执行的操作：基础出价提升3%'},
                {'title': '3.3.7',
                 'file_path': getfilepath(campaign_3_a7),
                 'introduction': '以下为 高于 平均ACOS值30% 以上的 campaign 广告 adgroup',
                 'operate': '建议执行的操作：基础出价降低15%'},
                {'title': '3.3.8',
                 'file_path': getfilepath(campaign_3_a8),
                 'introduction': '以下为 高于 平均ACOS值 20% - 30% 的 campaign 广告 adgroup',
                 'operate': '建议执行的操作：基础出价降低10%'},
                {'title': '3.3.9',
                 'file_path': getfilepath(campaign_3_a9),
                 'introduction': '以下为 高于 平均ACOS值 10% - 20% 的 campaign 广告 adgroup',
                 'operate': '建议执行的操作：基础出价降低5%'},

                ]

    last_answer["csvfiles"] = csvfiles



    print(last_answer)
    return last_answer



# res = auto_generepot('US','2024-04-01','2024-04-14')
def generate():
    last_answer = auto_generepot('FR', '2024-04-19', '2024-04-28')
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
