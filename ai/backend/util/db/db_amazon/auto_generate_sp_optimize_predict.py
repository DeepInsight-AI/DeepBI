import csv
import json

from flask import render_template, Flask

from amazon_mysql_rag_util_sp_optimize_predict import AmazonMysqlRagUitl
from auto_content import REPORT_SP_PREDICT_THOUGHT,REPORT_ANALYST
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
    report_name = "SP广告"+market1+"优化销售额期望（以"+market2+"在"+startdate+"至"+endate+"为例）"
    last_answer["report_name"]=report_name

    # 增加问题
    all_report_question=[]
    """构建"""

    # 构建1.法国所有商品与德国前70%销售重叠部分
    module_1_process_1 = []
    keyword_1_q1 = "1.1 在 {} 至 {} 这段时间内，{}_SP广告的前70%销售部分的商品sku，将这些sku信息生成csv文件。".format(startdate, endate, market2)
    keyword_1_a1 = amazon.get_sp_seles_top70_sku(market2, startdate, endate)
    module_1_process_1.append({"question": keyword_1_q1, "answer": keyword_1_a1})
    keyword_1_q2 = "1.2 在 {} 至 {} 这段时间内，{}_SP广告的前70%销售部分的商品sku与{}投放商品重复的sku是什么，将这些sku信息生成csv文件。".format(startdate, endate, market2, market1)
    keyword_1_a2 = amazon.get_repeat_sp_seles_top70_sku(market1, market2, startdate, endate)
    module_1_process_1.append({"question": keyword_1_q2, "answer": keyword_1_a2})
    keyword_1_q3 = "1.3 在 {} 至 {} 这段时间内，{}_SP广告重复的sku的总销售额 ，总广告消耗和平均ACOS是多少？".format(startdate, endate, market2)
    keyword_1_a3 = amazon.get_repeat_sp_seles_top70_sku_info_market2(market1, market2, startdate, endate)
    module_1_process_1.append({"question": keyword_1_q3, "answer": keyword_1_a3})
    keyword_1_q4 = "1.4 在 {} 至 {} 这段时间内，{}_SP广告重复的sku的总销售额 ，总广告消耗和平均ACOS是多少？".format(startdate, endate, market1)
    keyword_1_a4 = amazon.get_repeat_sp_seles_top70_sku_info_market1(market1, market2, startdate, endate)
    module_1_process_1.append({"question": keyword_1_q4, "answer": keyword_1_a4})

    keyword_answer_1 = {}
    report_name_1 = f'1.找出{market1}所有商品与{market2}前70%销售重叠部分'
    keyword_answer_1["analysis_item"] = report_name_1
    keyword_answer_1["description"] = '分四步执行'
    keyword_answer_1["analysis_process"] = module_1_process_1

    keyword_answer = []
    keyword_answer.append(keyword_answer_1)

    # 2.找出 定向投放关键词 中的 优质关键词，提价操作
    module_1_process_2 = []
    keyword_2_q1 = "2.1，在 {} 至 {} 这段时间内，{}_SP广告中重复的sku中低于 平均ACOS 30%以上的sku的旧总销售额 ，总广告消耗和提价后的期望是多少?".format(startdate,endate,market1)
    keyword_2_a1 = amazon.predict_repeat_market1_sp_seles_sku_info1(market1, market2, startdate, endate)
    module_1_process_2.append({"question": keyword_2_q1, "answer": keyword_2_a1})
    keyword_2_q2 = "2.2，在 {} 至 {} 这段时间内，{}_SP广告中重复的sku中低于 平均ACOS 20%，高于 30%的sku的旧总销售额 ，总广告消耗和提价后的期望是多少".format(startdate, endate, market1)
    keyword_2_a2 = amazon.predict_repeat_market1_sp_seles_sku_info2(market1, market2, startdate, endate)
    module_1_process_2.append({"question": keyword_2_q2, "answer": keyword_2_a2})
    keyword_2_q3 = "2.3 在 {} 至 {} 这段时间内，{}_SP广告中重复的sku中低于 平均ACOS 10%，高于 20%的sku的旧总销售额 ，总广告消耗和提价后的期望是多少".format(startdate, endate, market1)
    keyword_2_a3 = amazon.predict_repeat_market1_sp_seles_sku_info3(market1, market2, startdate, endate)
    module_1_process_2.append({"question": keyword_2_q3, "answer": keyword_2_a3})
    keyword_2_q4 = "2.4 在 {} 至 {} 这段时间内，{}_SP广告中重复的sku中高于 平均ACOS 20%，低于 30%的sku的旧总销售额 ，总广告消耗和降价后的期望是多少".format(startdate, endate, market1)
    keyword_2_a4 = amazon.predict_repeat_market1_sp_seles_sku_info4(market1, market2, startdate, endate)
    module_1_process_2.append({"question": keyword_2_q4, "answer": keyword_2_a4})
    keyword_2_q5 = "2.5 在 {} 至 {} 这段时间内，{}_SP广告中重复的sku中高于 平均ACOS 10%，低于 20%的sku的旧总销售额 ，总广告消耗和降价后的期望是多少".format(startdate, endate, market1)
    keyword_2_a5 = amazon.predict_repeat_market1_sp_seles_sku_info5(market1, market2, startdate, endate)
    module_1_process_2.append({"question": keyword_2_q5, "answer": keyword_2_a5})
    keyword_2_q6 = "2.6 在 {} 至 {} 这段时间内，{}_SP广告重复的介于 平均ACOS 10%的sku的总销售额 ，总广告消耗是多少？".format(startdate, endate, market1)
    keyword_2_a6 = amazon.get_repeat_sp_seles_sku_info_market1_1(market1, market2, startdate, endate)
    module_1_process_2.append({"question": keyword_2_q6, "answer": keyword_2_a6})
    keyword_2_q7 = "2.7 在 {} 至 {} 这段时间内，{}_SP广告重复的的sku其中销售额为0的，总销售额 ，总广告消耗是多少？".format(startdate, endate, market1)
    keyword_2_a7 = amazon.get_repeat_sp_seles_sku_info_market1_2(market1, market2, startdate, endate)
    module_1_process_2.append({"question": keyword_2_q7, "answer": keyword_2_a7})
    keyword_2_q8 = "2.8 在 {} 至 {} 这段时间内，{}_SP广告中重复的sku中高于 平均ACOS 30%以上的sku的总销售额 ，总广告消耗".format(startdate, endate, market1)
    keyword_2_a8 = amazon.get_repeat_sp_seles_sku_info_market1_3(market1, market2, startdate, endate)
    module_1_process_2.append({"question": keyword_2_q8, "answer": keyword_2_a8})




    keyword_answer_2 = {}
    report_name_2 = f'2.{market1}所有商品与{market2}前70%销售重叠部分,进行线性优化'
    keyword_answer_2["analysis_item"] = report_name_2
    keyword_answer_2["description"] = '分八步执行'
    keyword_answer_2["analysis_process"] = module_1_process_2
    keyword_answer.append(keyword_answer_2)


    # 关键词question
    keyword_question={}
    keyword_question["question"]={'report_name': '1.前70%商品重叠部分',
          'description': ''}
    keyword_question["answer"]=keyword_answer
    # 关键词部分完成
    all_report_question.append(keyword_question)
    #
    #
    # 商品优化部分
    # 1，未重叠部分
    module_2_process_1 = []
    product_1_q1 = "1.1 在 {} 至 {} 这段时间内，{}_SP广告的前70%销售部分的商品sku中{}_SP广告未投放的sku是什么，将这些sku信息生成csv文件。".format(startdate, endate, market2, market1)
    product_1_a1 = amazon.get_unrepeat_sp_seles_top70_sku(market1, market2, startdate, endate)
    module_2_process_1.append({"question": product_1_q1, "answer": product_1_a1})
    product_1_q2 = "1.2 在 {} 至 {} 这段时间内，{}_SP广告中{}_SP广告未投放的sku的总销售额 ，总广告消耗和平均ACOS是多少？".format(startdate, endate, market2, market1)
    product_1_a2 = amazon.get_unrepeat_sp_seles_top70_sku_info_market2(market1, market2, startdate, endate)
    module_2_process_1.append({"question": product_1_q2, "answer": product_1_a2})
    product_1_q3 = "1.3，根据在 {} 至 {} 这段时间内，{}_SP广告和{}_SP广告重叠sku的数据，预测{}_SP广告未投放sku的总销售额 ，总广告消耗是多少？".format(startdate,endate,market1,market2,market1)
    product_1_a3 = amazon.predict_unrepeat_sp_seles_top70_sku_info_market1(market1, market2, startdate, endate)
    module_2_process_1.append({"question": product_1_q3, "answer": product_1_a3})

    product_answer_1 = {}
    product_answer_1["analysis_item"] = ''
    product_answer_1["description"] = '分三步执行'
    product_answer_1["analysis_process"] = module_2_process_1

    product_answer = []
    product_answer.append(product_answer_1)

    # 产品question
    product_question = {}
    product_question["question"] = {'report_name': '2.预测前70%商品未投放部分，上架后的数据',
                                    'description': ''}
    product_question["answer"] = product_answer
    # 产品部分完成
    all_report_question.append(product_question)
    #
    # 广告计划优化分析
    # 1，找出 campaign 广告活动 中的  优质 与 劣质广告活动
    campaign_analysis_process_1 = []
    campaign_1_q1 = "1.1 在 {} 至 {} 这段时间内，{}_SP广告的前70%销售部分的商品sku与{}在投放商品未重复的sku是什么，将这些sku信息生成csv文件。".format(startdate, endate, market2, market1)
    campaign_1_a1 = amazon.get_unrepeat_market1_sp_seles_sku(market1, market2, startdate, endate)
    campaign_analysis_process_1.append({"question":campaign_1_q1,"answer":campaign_1_a1})
    campaign_1_q2 = "1.2 在 {} 至 {} 这段时间内，{}_SP广告未重复的sku的总销售额 ，总广告消耗和平均ACOS是多少？".format(startdate, endate, market1)
    campaign_1_a2 = amazon.get_unrepeat_sp_seles_sku_info_market1(market1, market2, startdate, endate)
    campaign_analysis_process_1.append({"question": campaign_1_q2, "answer": campaign_1_a2})
    campaign_1_q3 = "1.3 在 {} 至 {} 这段时间内，{}_SP广告中未重复的sku中低于 平均ACOS 30%以上的sku的旧总销售额 ，总广告消耗和提价后的期望是多少".format(startdate, endate, market1)
    campaign_1_a3 = amazon.predict_unrepeat_market1_sp_seles_sku_info1(market1, market2, startdate, endate)
    campaign_analysis_process_1.append({"question":campaign_1_q3,"answer":campaign_1_a3})
    campaign_1_q4 = "1.4 在 {} 至 {} 这段时间内，{}_SP广告中未重复的sku中低于 平均ACOS 20%，高于 30%的sku的旧总销售额 ，总广告消耗和提价后的期望是多少".format(startdate, endate, market1)
    campaign_1_a4 = amazon.predict_unrepeat_market1_sp_seles_sku_info2(market1, market2, startdate, endate)
    campaign_analysis_process_1.append({"question": campaign_1_q4, "answer": campaign_1_a4})
    campaign_1_q5 = "1.5 在 {} 至 {} 这段时间内，{}_SP广告中未重复的sku中低于 平均ACOS 10%，高于 20%的sku的旧总销售额 ，总广告消耗和提价后的期望是多少".format(startdate, endate, market1)
    campaign_1_a5 = amazon.predict_unrepeat_market1_sp_seles_sku_info3(market1, market2, startdate, endate)
    campaign_analysis_process_1.append({"question": campaign_1_q5, "answer": campaign_1_a5})
    campaign_1_q6 = "1.6 在 {} 至 {} 这段时间内，{}_SP广告中未重复的sku中高于 平均ACOS 20%，低于 30%的sku的旧总销售额 ，总广告消耗和降价后的期望是多少".format(startdate, endate, market1)
    campaign_1_a6 = amazon.predict_unrepeat_market1_sp_seles_sku_info4(market1, market2, startdate, endate)
    campaign_analysis_process_1.append({"question": campaign_1_q6, "answer": campaign_1_a6})
    campaign_1_q7 = "1.7 在 {} 至 {} 这段时间内，{}_SP广告中未重复的sku中高于 平均ACOS 10%，低于 20%的sku的旧总销售额 ，总广告消耗和降价后的期望是多少".format(startdate, endate, market1)
    campaign_1_a7 = amazon.predict_unrepeat_market1_sp_seles_sku_info5(market1, market2, startdate, endate)
    campaign_analysis_process_1.append({"question": campaign_1_q7, "answer": campaign_1_a7})


    campaign_answer_1 = {}
    campaign_answer_1["analysis_item"] = '1，调价部分'
    campaign_answer_1["description"] = '分七步执行'
    campaign_answer_1["analysis_process"] = campaign_analysis_process_1

    campaign_answer = []
    campaign_answer.append(campaign_answer_1)
    #
    # # 2，找出 campaign 广告活动 中的  placement优质位置 与 劣质位置
    campaign_analysis_process_2 = []
    campaign_2_q1 = "2.1 在 {} 至 {} 这段时间内，{}_SP广告未重复的介于 平均ACOS 10%的sku的总销售额 ，总广告消耗是多少？".format(startdate, endate, market1)
    campaign_2_a1 = amazon.get_unrepeat_sp_seles_sku_info_market1_1(market1, market2, startdate, endate)
    campaign_analysis_process_2.append({"question": campaign_2_q1, "answer": campaign_2_a1})
    campaign_2_q2 = "2.2 在 {} 至 {} 这段时间内，{}_SP广告未重复的的sku其中销售额为0的，总销售额 ，总广告消耗是多少？".format(startdate, endate, market1)
    campaign_2_a2 = amazon.get_unrepeat_sp_seles_sku_info_market1_2(market1, market2, startdate, endate)
    campaign_analysis_process_2.append({"question": campaign_2_q2, "answer": campaign_2_a2})
    campaign_2_q3 = "2.3 在 {} 至 {} 这段时间内，{}_SP广告中未重复的sku中高于 平均ACOS 30%以上的sku的总销售额 ，总广告消耗".format(startdate, endate, market1)
    campaign_2_a3 = amazon.get_unrepeat_sp_seles_sku_info_market1_3(market1, market2, startdate, endate)
    campaign_analysis_process_2.append({"question": campaign_2_q3, "answer": campaign_2_a3})

    campaign_answer_2 = {}
    campaign_answer_2["analysis_item"] = '2，未调价部分'
    campaign_answer_2["description"] = '分三步执行'
    campaign_answer_2["analysis_process"] = campaign_analysis_process_2
    campaign_answer.append(campaign_answer_2)
    #
    #
    #
    #
    # 广告计划question
    campaign_question={}
    report_name_3 = f'3.{market1}与{market2}前70%销售未重叠部分的线性优化'
    campaign_question["question"]={'report_name': report_name_3,
          'description': ''}
    campaign_question["answer"]=campaign_answer
    # 关键词部分完成
    all_report_question.append(campaign_question)
    # 第五六部分  总结
    # 6. 把新算出的cost之和除以新算出的sales之和，求出ACOS的期望
    # 总结1-5数据做对比
    part_one1 = [keyword_2_a1, keyword_2_a2, keyword_2_a3, keyword_2_a4, keyword_2_a5, ]
    part_two1 = [keyword_2_a6, keyword_2_a7]
    new_allsales1 = 0
    new_allcost1 = 0
    old_allsales1 = 0
    old_allcost1 = 0

    for i in part_one1:
        if "total_cost_new" in i[2]:
            new_allcost1 += i[2]["total_cost_new"]
        if "total_cost_old" in i[0]:
            old_allcost1 += i[0]["total_cost_old"]
        if "total_sales_new" in i[3]:
            new_allsales1 += i[3]["total_sales_new"]
        if "total_sales_old" in i[1]:
            old_allsales1 += i[1]["total_sales_old"]
    for j in part_two1:
        old_allcost1 += j[0]["total_cost_old"]
        new_allcost1 += j[0]["total_cost_old"]
        old_allsales1 += j[1]["total_sales_old"]
        new_allsales1 += j[1]["total_sales_old"]

    old_allcost1 += keyword_2_a8[0]["total_cost_old"]
    old_allsales1 += keyword_2_a8[1]["total_sales_old"]


    part6_1 = []
    part6_1_q1 = "1.1 在 {} 至 {} 这段时间内，{}_SP广告中重复的sku总结".format(startdate, endate, market1)
    part6_1_a1 = "新的总销售额:{},旧的总销售额:{},新的总花费额:{},旧的总花费额:{},新的ACOS:{}%,旧的ACOS:{}%,销售额增长:{}%,花费增长:{}%,ACOS变化:{}%". \
        format(round(new_allsales1, 2), round(old_allsales1, 2), round(new_allcost1, 2), round(old_allcost1, 2),round(new_allcost1 / new_allsales1*100, 2), round(old_allcost1 / old_allsales1*100,2),
               round((new_allsales1 - old_allsales1) / old_allsales1*100, 2),
               round((new_allcost1 - old_allcost1) / old_allcost1*100, 2),
               round((new_allcost1 / new_allsales1 - old_allcost1 / old_allsales1) / (old_allcost1 / old_allsales1)*100, 2))
    part6_1.append({"question": part6_1_q1, "answer": part6_1_a1})

    old_allcost2 = product_1_a3[0]["total_cost_old"]
    old_allsales2 = product_1_a3[1]["total_sales_old"]
    new_allcost2 = product_1_a3[2]["total_cost_new"]
    new_allsales2 = product_1_a3[3]["total_sales_new"]
    part6_1_q2 = "1.2 在 {} 至 {} 这段时间内，{}_SP广告中前70%商品未投放的sku总结".format(startdate, endate, market1)
    part6_1_a2 = "新的总销售额:{},旧的总销售额:{},新的总花费额:{},旧的总花费额:{}". \
        format(round(new_allsales2, 2), round(old_allsales2, 2), round(new_allcost2, 2), round(old_allcost2, 2))
    part6_1.append({"question": part6_1_q2, "answer": part6_1_a2})

    part_one2 = [campaign_1_a3, campaign_1_a4,campaign_1_a5, campaign_1_a6, campaign_1_a7]
    part_two2 = [campaign_2_a1, campaign_2_a2]
    new_allsales3 = 0
    new_allcost3 = 0
    old_allsales3 = 0
    old_allcost3 = 0

    for i in part_one2:
        if "total_cost_new" in i[2]:
            new_allcost3 += i[2]["total_cost_new"]
        if "total_cost_old" in i[0]:
            old_allcost3 += i[0]["total_cost_old"]
        if "total_sales_new" in i[3]:
            new_allsales3 += i[3]["total_sales_new"]
        if "total_sales_old" in i[1]:
            old_allsales3 += i[1]["total_sales_old"]
    for j in part_two2:
        old_allcost3 += j[0]["total_cost_old"]
        new_allcost3 += j[0]["total_cost_old"]
        old_allsales3 += j[1]["total_sales_old"]
        new_allsales3 += j[1]["total_sales_old"]

    old_allcost3 += campaign_2_a3[0]["total_cost_old"]
    old_allsales3 += campaign_2_a3[1]["total_sales_old"]

    part6_1_q3 = "1.3 在 {} 至 {} 这段时间内，{}_SP广告中未重复的sku总结".format(startdate, endate, market1)
    part6_1_a3 = "新的总销售额:{},旧的总销售额:{},新的总花费额:{},旧的总花费额:{},新的ACOS:{}%,旧的ACOS:{}%,销售额增长:{}%,花费增长:{}%,ACOS变化:{}%". \
        format(round(new_allsales3, 2), round(old_allsales3, 2), round(new_allcost3, 2), round(old_allcost3, 2),round(new_allcost3 / new_allsales3*100, 2), round(old_allcost3 / old_allsales3*100,2),
               round((new_allsales3 - old_allsales3) / old_allsales3*100, 2) ,
               round((new_allcost3 - old_allcost3) / old_allcost3*100, 2) ,
               round((new_allcost3 / new_allsales3 - old_allcost3 / old_allsales3) / (old_allcost3 / old_allsales3)*100, 2))
    part6_1.append({"question": part6_1_q3, "answer": part6_1_a3})

    part_one = [keyword_2_a1, keyword_2_a2, keyword_2_a3, keyword_2_a4, keyword_2_a5, product_1_a3, campaign_1_a3, campaign_1_a4,
                campaign_1_a5, campaign_1_a6, campaign_1_a7]
    part_two = [keyword_2_a6, keyword_2_a7, campaign_2_a1, campaign_2_a2]
    new_allsales = 0
    new_allcost = 0
    old_allsales = 0
    old_allcost = 0

    for i in part_one:
        if "total_cost_new" in i[2]:
            new_allcost += i[2]["total_cost_new"]
        if "total_cost_old" in i[0]:
            old_allcost += i[0]["total_cost_old"]
        if "total_sales_new" in i[3]:
            new_allsales += i[3]["total_sales_new"]
        if "total_sales_old" in i[1]:
            old_allsales += i[1]["total_sales_old"]
    for j in part_two:
        old_allcost += j[0]["total_cost_old"]
        new_allcost += j[0]["total_cost_old"]
        old_allsales += j[1]["total_sales_old"]
        new_allsales += j[1]["total_sales_old"]

    old_allcost += keyword_2_a8[0]["total_cost_old"]
    old_allcost += campaign_2_a3[0]["total_cost_old"]
    old_allsales += keyword_2_a8[1]["total_sales_old"]
    old_allsales += campaign_2_a3[1]["total_sales_old"]


    part6_1_q2 = "1.4 总结新旧对比"
    part6_1_a2 = "新的总销售额:{},旧的总销售额:{},新的总花费额:{},旧的总花费额:{},新的ACOS:{}%,旧的ACOS:{}%,销售额增长:{}%,花费增长:{}%,ACOS变化:{}%". \
        format(round(new_allsales, 2), round(old_allsales, 2), round(new_allcost, 2), round(old_allcost, 2),round(new_allcost / new_allsales*100, 2), round(old_allcost / old_allsales*100,2),
               round((new_allsales - old_allsales) / old_allsales*100, 2),
               round((new_allcost - old_allcost) / old_allcost*100, 2),
               round((new_allcost / new_allsales - old_allcost / old_allsales) / (old_allcost / old_allsales)*100, 2))
    part6_1.append({"question": part6_1_q2, "answer": part6_1_a2})
    part6_1_answer_1 = {}
    part6_1_answer_1["analysis_item"] = ''
    part6_1_answer_1["description"] = ''
    part6_1_answer_1["analysis_process"] = part6_1

    part6_answer = []
    part6_answer.append(part6_1_answer_1)
    part6_question = {}
    part6_question["question"] = {'report_name': '5.总结优化前后对比',
                                  'description': ''}
    part6_question["answer"] = part6_answer
    all_report_question.append(part6_question)




    """执行上述构建"""
    last_answer["report_question"] = all_report_question
    # 增加report_thought
    last_answer["report_thought"]= REPORT_SP_PREDICT_THOUGHT
    # 增加report_analyst
    last_answer["report_analyst"] = REPORT_ANALYST

    return last_answer




def generate(market1= 'DE', market2= 'US', startdate = '2024-05-01', endate= '2024-05-15'):
    last_answer = auto_generepot(market1, market2, startdate, endate)
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
    filename = f"output_{market1}_{market2}_{startdate}_{endate}.html"
    with open(filename, 'w', encoding='utf-8') as output_file:
        output_file.write(rendered_html)

    print(f"HTML文件已生成：{filename}")

# generate()
def main():
    # 在这里调用 generate() 函数
    generate()

if __name__ == "__main__":
    main()
