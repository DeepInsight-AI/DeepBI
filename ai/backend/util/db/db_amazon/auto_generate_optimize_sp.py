import csv
import json

from flask import render_template, Flask
#from ai.backend.util.db.db_dwx.dwx_generate_tools import generate_ecode
from amazon_mysql_util_genarate_optimizesp import AmazonMysqlOptimizeSP
from auto_content import REPORT_THOUGHT_SD, REPORT_ANALYST, REPORT_SP_OPTIMIZER_THOUGHT
# -*- coding: utf-8 -*-
from jinja2 import Template
import time
import pandas as pd
import os



def auto_generate_optimize_sp(market,startdate,endate):
    """该报告针对指定market 指定时间段内的 ：SP线性优化ACOS期望和销售额期望"""

    db_info = {'host': '192.168.5.114', 'user': 'test_deepdata', 'passwd': 'test123!@#', 'port': 3308,
               'db': 'amazon_ads',
               'charset': 'utf8mb4', 'use_unicode': True, }

    dwx = AmazonMysqlOptimizeSP(db_info)

    last_answer = {}

    # 设置标题
    report_name = market+"_SP线性优化ACOS期望和销售额期望["+startdate+"_"+endate+"]"
    last_answer["report_name"]=report_name

    # 增加问题
    all_report_question=[]
    """构建"""

    # 计算平均ACOS作为以下内容标准
    avgacos = dwx.get_avgacos(market,startdate,endate)
    # 构建1. 按照四舍五入的ACOS求出所有对应关键词的调价后clicks、CPC，然后用cost= clicks*CPC求出关键词的花费
    # 1，找出 sku广告中的 优质广告
    keyword_analysis_process_1 = []
    keyword_1_q1="1.1 低于 平均ACOS 30%以上——关键词 提高出价 10%后期望值"
    keyword_1_a1=dwx.get_sd_product_111(market,startdate,endate,avgacos)
    keyword_analysis_process_1.append({"question":keyword_1_q1,"answer":keyword_1_a1})
    keyword_1_q2 = "1.2 低于 平均ACOS 30%以上——关键词 的旧总值"
    keyword_1_a2 = dwx.get_sd_product_112(market,startdate,endate,avgacos)
    keyword_analysis_process_1.append({"question":keyword_1_q2,"answer":keyword_1_a2})
    keyword_1_q3 = "1.3 低于 平均ACOS 20%，高于 30%的——关键词 提高出价 5%后期望值"
    keyword_1_a3 = dwx.get_sd_product_113(market,startdate,endate,avgacos)
    keyword_analysis_process_1.append({"question":keyword_1_q3,"answer":keyword_1_a3})
    keyword_1_q4 = "1.4 低于 平均ACOS 20%，高于 30%的——关键词 的旧总值"
    keyword_1_a4 = dwx.get_sd_product_114(market,startdate,endate,avgacos)
    keyword_analysis_process_1.append({"question":keyword_1_q4,"answer":keyword_1_a4})
    keyword_1_q5 = "1.5 低于 平均ACOS 10%，高于 20%的——关键词 提高出价 3%后期望值"
    keyword_1_a5 = dwx.get_sd_product_115(market, startdate, endate,avgacos)
    keyword_analysis_process_1.append({"question": keyword_1_q5, "answer": keyword_1_a5})
    keyword_1_q6 = "1.6 低于 平均ACOS 10%，高于 20%的——关键词 的旧总值"
    keyword_1_a6 = dwx.get_sd_product_116(market, startdate, endate, avgacos)
    keyword_analysis_process_1.append({"question": keyword_1_q6, "answer": keyword_1_a6})
    keyword_1_q7 = "1.7 这些已投放关键词中，高于 平均ACOS 20%，低于 30%的有哪些——调低出价 20% 后期望值"
    keyword_1_a7 = dwx.get_sd_product_117(market, startdate, endate, avgacos)
    keyword_analysis_process_1.append({"question": keyword_1_q7, "answer": keyword_1_a7})
    keyword_1_q8 = "1.8 这些已投放关键词中，高于 平均ACOS 20%，低于 30%的——关键词 的旧总值"
    keyword_1_a8 = dwx.get_sd_product_118(market, startdate, endate, avgacos)
    keyword_analysis_process_1.append({"question": keyword_1_q8, "answer": keyword_1_a8})
    keyword_1_q9 = "1.9 这些已投放关键词中，高于 平均ACOS 10%，低于 20%的——调低出价 10% 后期望值"
    keyword_1_a9 = dwx.get_sd_product_119(market, startdate, endate, avgacos)
    keyword_analysis_process_1.append({"question": keyword_1_q9, "answer": keyword_1_a9})
    keyword_1_q10 = "1.10 这些已投放关键词中，高于 平均ACOS 10%，低于 20%的——关键词 的旧总值"
    keyword_1_a10 = dwx.get_sd_product_110(market, startdate, endate, avgacos)
    keyword_analysis_process_1.append({"question": keyword_1_q10, "answer": keyword_1_a10})

    keyword_answer_1 = {}
    keyword_answer_1["analysis_item"] = '1. 按照四舍五入的ACOS求出所有对应关键词的调价后clicks、CPC，然后用cost= clicks*CPC求出关键词的花费'
    keyword_answer_1["description"] = '按如下过程进行分析:<br>1. 按照四舍五入的ACOS求出所有对应关键词的调价后clicks、CPC，然后用cost= clicks*CPC求出关键词的花费'
    keyword_answer_1["analysis_process"] = keyword_analysis_process_1

    keyword_answer = []
    keyword_answer.append(keyword_answer_1)

    # 关键词question
    keyword_question={}
    keyword_question["question"]={'report_name': '1.关键词的调价后期望分析',
          'description': ''}
    keyword_question["answer"]=keyword_answer
    # 关键词部分完成
    all_report_question.append(keyword_question)

    # product_answer = []
    # # product_answer.append(product_answer_1)
    # # 产品question
    # product_question = {}
    # product_question["question"] = {'report_name': '2. 假设每个关键词调价后的ACOS（四舍五入）不变，用新算出的cost除以ACOS得到每个关键词对应的新sales',
    #                                 'description': '2. 假设每个关键词调价后的ACOS（四舍五入）不变，用新算出的cost除以ACOS得到每个关键词对应的新sales'}
    # product_question["answer"] = product_answer
    # # 产品部分完成
    # all_report_question.append(product_question)

    # 3. 没有调整的sp广告区间（相对平均ACOS的±10%）
    campaign_analysis_process_1 = []
    campaign_1_q1 = "1.1，大于平均ACOS的0-10%区间"
    campaign_1_a1 = dwx.get_sd_targeting_311(market,startdate,endate,avgacos)
    campaign_analysis_process_1.append({"question":campaign_1_q1,"answer":campaign_1_a1})
    campaign_1_q2 = "1.2 小于平均ACOS的0-10%区间"
    campaign_1_a2 = dwx.get_sd_targeting_312(market,startdate,endate,avgacos)
    campaign_analysis_process_1.append({"question":campaign_1_q2,"answer":campaign_1_a2})
    campaign_answer_1 = {}
    campaign_answer_1["analysis_item"] = '没有调整的sp广告区间（相对平均ACOS的±10%）'
    campaign_answer_1["description"] = ''
    campaign_answer_1["analysis_process"] = campaign_analysis_process_1

    campaign_answer = []
    campaign_answer.append(campaign_answer_1)

    # 广告计划question
    campaign_question={}
    campaign_question["question"]={'report_name': '2. 没有调整的sp广告区间（相对平均ACOS的±10%）',
          'description': ''}
    campaign_question["answer"]=campaign_answer
    # 关键词部分完成
    all_report_question.append(campaign_question)


    # 第四部分
    # 4.4. Sales = 0的数据
    sales0_1 = []
    sales0_1_q1 = "1.1，Sales = 0的数据 的新期望"
    sales0_1_a1 = dwx.get_sd_product_411(market, startdate, endate, avgacos)
    sales0_1.append({"question": sales0_1_q1, "answer": sales0_1_a1})
    sales0_answer_1 = {}
    sales0_answer_1["analysis_item"] = 'Sales = 0的数据 的新期望'
    sales0_answer_1["description"] = ''
    sales0_answer_1["analysis_process"] = sales0_1

    sales0_answer = []
    sales0_answer.append(sales0_answer_1)
    sales0_question = {}
    sales0_question["question"] = {'report_name': '3.Sales = 0的数据',
                                   'description': ''}
    sales0_question["answer"] = sales0_answer
    all_report_question.append(sales0_question)

    # 第五部分
    # 5. ACOS大于平均ACOS的30%的数据
    part5_1 = []
    part5_1_q1 = "1.1，ACOS大于平均ACOS的30%的数据"
    part5_1_a1 = dwx.get_sp_optimize_511(market, startdate, endate, avgacos)
    part5_1.append({"question": part5_1_q1, "answer": part5_1_a1})
    part5_1_answer_1 = {}
    part5_1_answer_1["analysis_item"] = 'ACOS大于平均ACOS的30%的数据'
    part5_1_answer_1["description"] = ''
    part5_1_answer_1["analysis_process"] = part5_1

    part5_answer = []
    part5_answer.append(part5_1_answer_1)
    part5_question = {}
    part5_question["question"] = {'report_name': '4.ACOS大于平均ACOS的30%的数据',
                                   'description': ''}
    part5_question["answer"] = part5_answer
    all_report_question.append(part5_question)

    # 第五六部分  总结
    # 6. 把新算出的cost之和除以新算出的sales之和，求出ACOS的期望
    # 总结1-5数据做对比
    part_one = [keyword_1_a1,keyword_1_a2,keyword_1_a3,keyword_1_a4,keyword_1_a5,keyword_1_a6,keyword_1_a7,keyword_1_a8,keyword_1_a9,keyword_1_a10]
    part_two = [campaign_1_a1,campaign_1_a2,sales0_1_a1]
    new_allsales = 0
    new_allcost = 0
    old_allsales = 0
    old_allcost = 0

    for i in part_one:
        if "total_cost_new" in i[0]:
            new_allcost+=i[0]["total_cost_new"]
        if "total_cost_before" in i[0]:
            old_allcost+=i[0]["total_cost_before"]
        if "total_sales_new" in i[1]:
            new_allsales+=i[1]["total_sales_new"]
        if "total_sales_before" in i[1]:
            old_allsales+=i[1]["total_sales_before"]
    for j in part_two:
        old_allcost+=j[0]["total_cost_before"]
        new_allcost+=j[0]["total_cost_before"]
        old_allsales+=j[1]["total_sales_before"]
        new_allsales+=j[1]["total_sales_before"]

    old_allcost+=part5_1_a1[0]["total_cost_before"]
    # new_allcost+=part5_1_a1[0]["total_cost_before"]
    old_allsales+=part5_1_a1[1]["total_sales_before"]

    part6_1 = []
    part6_1_q1 = "总结新旧对比"
    part6_1_a1 = "<br>新的总销售额:{},<br>旧的总销售额:{},<br>新的总花费额:{},<br>旧的总花费额:{},<br>销售额增长:{},<br>花费增长:{},<br>ACOS变化:{}".\
        format(round(new_allsales,2),round(old_allsales,2),round(new_allcost,2),round(old_allcost,2),
               round((new_allsales-old_allsales)/old_allsales,2),
               round((new_allcost-old_allcost)/old_allcost,2),
               round((new_allcost/new_allsales-old_allcost/old_allsales)/(old_allcost/old_allsales),2))
    part6_1.append({"question": part6_1_q1, "answer": part6_1_a1})
    part6_1_answer_1 = {}
    part6_1_answer_1["analysis_item"] = 'ACOS大于平均ACOS的30%的数据'
    part6_1_answer_1["description"] = ''
    part6_1_answer_1["analysis_process"] = part6_1

    part6_answer = []
    part6_answer.append(part6_1_answer_1)
    part6_question = {}
    part6_question["question"] = {'report_name': '5.总结新旧对比',
                                  'description': ''}
    part6_question["answer"] = part6_answer
    all_report_question.append(part6_question)


    """执行上述构建"""
    last_answer["report_question"] = all_report_question
    # 增加report_thought
    last_answer["report_thought"]=REPORT_SP_OPTIMIZER_THOUGHT
    # 增加report_analyst
    last_answer["report_analyst"] = REPORT_ANALYST


    print(last_answer)
    return last_answer



# res = auto_generepot('US','2024-04-01','2024-04-14')
def generate():
    last_answer = auto_generate_optimize_sp('US', '2024-03-01', '2024-03-31')
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
    with open('output_sp_optimize.html', 'w', encoding='utf-8') as output_file:
        output_file.write(rendered_html)

    print("HTML文件已生成：output_sp_optimize.html")

generate()
# def main():
#     # 在这里调用 generate() 函数
#     generate()
#
# if __name__ == "__main__":
#     main()
