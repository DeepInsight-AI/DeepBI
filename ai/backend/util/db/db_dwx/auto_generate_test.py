
# from db_amazon.amazon_mysql_rag_util import AmazonMysqlRagUitl
from datetime import datetime

from auto_content_test import REPORT_THOUGHT,REPORT_ANALYST
# -*- coding: utf-8 -*-
from jinja2 import Template
from jinja2 import Environment
env = Environment()
env.globals['now'] = datetime.now
import time
from dwx_generate_tools import generate_ecode,fun_Wenxin
from dwx_all_mysql_rag_util import DwxMysqlRagUitl
import json



def auto_generepot(BusinessName, startdate, endate,BusinessName2,last_start,last_end):
    """该报告针对电玩猩江苏南通店财务数据进行生成"""
    db_info = {'host': '192.168.5.114', 'user': 'test_deepdata', 'passwd': 'test123!@#', 'port': 3308,
               'db': 'test_dwx_all',
               'charset': 'utf8mb4', 'use_unicode': True, }
    dwx = DwxMysqlRagUitl(db_info)

    last_answer = {}
    chat_question=[]
    #存放主观问题

    # 设置标题
    report_name = "电玩猩_" + BusinessName + "体检报告_" + startdate + "_" + endate
    last_answer["report_name"]=report_name

    # 增加问题
    all_report_question=[]
    """构建"""

    # 构建1.套餐营收优化分析

    # 1，分析南通店可以优化的代币套餐渠道
    channel_analysis_process_1 = []
    channel_1_q1="1.1 计算 {} -{}这段时间，{}的各个渠道套餐销售额分别是多少？".format(startdate, endate, BusinessName)
    channel_1_a1=dwx.get_nt_total_sellMoney(startdate, endate, BusinessName)
    channel_1_a1_ecode = generate_ecode(channel_1_a1)
    channel_analysis_process_1.append({"question":channel_1_q1,"answer":channel_1_a1,"echart_id":"channel_1_1","echart_code":channel_1_a1_ecode})

    channel_1_q2 = "1.2 计算{}-{}这段时间，{}的各个渠道套餐销售额占比".format(startdate, endate, BusinessName)
    channel_1_a2 = dwx.calculate_channel_sellMoney_proportion(startdate, endate, BusinessName)
    channel_1_a2_ecode = generate_ecode(channel_1_a2)
    channel_analysis_process_1.append({"question": channel_1_q2, "answer": channel_1_a2,"echart_id":"channel_1_2","echart_code":channel_1_a2_ecode})

    channel_1_q3 = "1.3 计算{}-{}这段时间，各个店铺的各渠道套餐销售额占比".format(startdate, endate)
    channel_1_a3 = dwx.calculate_store_channel_sellMoney_proportion(startdate, endate)
    channel_1_a3_ecode = generate_ecode(channel_1_a3)
    channel_analysis_process_1.append({"question": channel_1_q3, "answer": channel_1_a3,"echart_id":"channel_1_3","echart_code":channel_1_a3_ecode})

    channel_1_q4 = "1.4 找出{}-{}这段时间，{}销售额占比（1.2结论）低于总占比（1.3结论）的80%的销售渠道".format(startdate,endate, BusinessName)
    channel_1_a4 = dwx.find_low_sellMoney_channel(startdate, endate, BusinessName)
    channel_1_a4_ecode = ""
    channel_analysis_process_1.append({"question": channel_1_q4, "answer": channel_1_a4,"echart_code":channel_1_a4_ecode})

    channel_answer_1 = {}
    channel_answer_1["analysis_item"] = '1. 分析南通店可以优化的代币套餐渠道'
    channel_answer_1["description"] = '分四步执行'
    channel_answer_1["analysis_process"] = channel_analysis_process_1

    channel_answer = []
    channel_answer.append(channel_answer_1)

    # 2，分析南通店可优化渠道的滞销套餐
    channel_analysis_process_2 = []
    channel_2_q1 = "2.1 计算{}-{}这段时间内{}可优化渠道（1.4结论）的各套餐销量占比".format(startdate, endate, BusinessName)
    channel_2_a1 = dwx.calculate_optimizable_channel_sellProportion(startdate, endate, BusinessName)
    channel_2_a1_ecode = generate_ecode(channel_2_a1)
    channel_analysis_process_2.append({"question": channel_2_q1, "answer": channel_2_a1,"echart_id":"channel_2_1","echart_code":channel_2_a1_ecode})

    channel_2_q2 = "2.2 分析{}-{}这段时间内{}在可优化渠道（1.4结论）中各套餐销量平均占比".format(startdate, endate, BusinessName)
    channel_2_a2 = dwx.calculate_average_sellProportion(startdate, endate)
    channel_2_a2_ecode = generate_ecode(channel_2_a2)
    channel_analysis_process_2.append({"question": channel_2_q2, "answer": channel_2_a2,"echart_id":"channel_2_2","echart_code":channel_2_a2_ecode})

    channel_2_q3 = "2.3 分析{}-{}这段时间内{}在可优化渠道（1.4结论）滞销套餐名（销量占比低于均值的80%）".format(startdate, endate, BusinessName)
    channel_2_a3 = dwx.analyze_underperforming_packages(startdate, endate, BusinessName)
    channel_2_a3_ecode = ""
    channel_analysis_process_2.append({"question": channel_2_q3, "answer": channel_2_a3,"echart_id":"channel_2_3","echart_code":channel_2_a3_ecode})

# 2.4 主观问题
    channel_2_q4 = "2.4根据1.4和2.3的问题和结论 给出建议如何优化滞销套餐"
    # channel_analysis_process_2.append({"question": channel_2_q4, "answer": channel_2_a4})

#调用文心一言生成结论，test
    channel_2_q4_new=channel_2_q4 + channel_1_q4 + channel_1_a4 + channel_2_q3 + channel_2_a3
    channel_2_a4={}
    channel_2_a4 = fun_Wenxin(channel_2_q4_new)
    channel_analysis_process_2.append({"question": channel_2_q4, "answer": channel_2_a4})
    if channel_2_a4:
        print("1 2.4 data succeed")
# 调用文心一言生成结论, 成功

    chat_question.append(channel_2_q4_new)


    channel_answer_2 = {}
    channel_answer_2["analysis_item"] = '2. 分析可优化渠道的滞销套餐'
    channel_answer_2["description"] = '分四步执行'
    channel_answer_2["analysis_process"] = channel_analysis_process_2

    channel_answer.append(channel_answer_2)

    # 1套餐营收分析
    channel_question={}
    channel_question["question"]={'report_name': '1.套餐营收优化分析',
          'description': '目标：找出各个代币销售渠道中江苏南通店可优化的渠道，对于可优化渠道找出滞销套餐，进而对滞销套餐加大营销力度，提高销售额'}
    channel_question["answer"]=channel_answer

    # 套餐部分完成
    all_report_question.append(channel_question)



    # 2日营收优化分析
    daily_revenue_analysis_process_1 = []

    daily_revenue_1_1_q = "1.1 分析{}-{}这段时间内{}每日营收金额波动情况".format(startdate, endate, BusinessName)
    daily_revenue_1_1_a = dwx.analyze_daily_revenue_fluctuation(startdate, endate, BusinessName)
    daily_revenue_1_1_a_ecode = generate_ecode(daily_revenue_1_1_a)
    daily_revenue_analysis_process_1.append({"question": daily_revenue_1_1_q, "answer": daily_revenue_1_1_a,"echart_id":"daily_revenue_1_1_a_ecode","echart_code":daily_revenue_1_1_a_ecode})

    daily_revenue_1_2_q = "1.2 分析{}-{},{}这一周营收金额是否有异常情况,分析这周最低营收是否低于上周最低值的80%".format(startdate,endate, BusinessName)
    daily_revenue_1_2_a = dwx.analyze_weekly_revenue_anomaly(last_start,last_end,startdate,endate, BusinessName)
    daily_revenue_1_2_a_ecode = generate_ecode(daily_revenue_1_2_a)
    daily_revenue_analysis_process_1.append({"question": daily_revenue_1_2_q, "answer": daily_revenue_1_2_a,"echart_id":"daily_revenue_1_2_a_ecode","echart_code":daily_revenue_1_2_a_ecode})


# 2 1.3 主观问题
    daily_revenue_1_3_q = "1.3 根据1.1和1.2的结论分析可能的营收异常原因，给出优化建议 "
    # daily_revenue_1_3_a = dwx.analyze_weekly_revenue_anomaly(last_start,last_end,startdate,endate, BusinessName)
    daily_revenue_1_3_q_new=daily_revenue_1_3_q +  daily_revenue_1_1_q + "\n 结论："+ daily_revenue_1_1_a + daily_revenue_1_2_q +"\n 结论："+ daily_revenue_1_2_a
    daily_revenue_1_3_a={}
    daily_revenue_1_3_a = fun_Wenxin(daily_revenue_1_3_q_new)

    # print(daily_revenue_1_3_q_new )
    if daily_revenue_1_3_a:
        print("2 1.3 data succeed")
    daily_revenue_analysis_process_1.append({"question": daily_revenue_1_3_q, "answer": daily_revenue_1_3_a})
    chat_question.append(daily_revenue_1_3_q_new)


    daily_revenue_answer_1 = {}
    daily_revenue_answer_1["analysis_item"] = '1. 日营收优化分析'
    daily_revenue_answer_1["description"] = '三个子问题'
    daily_revenue_answer_1["analysis_process"] = daily_revenue_analysis_process_1

    daily_revenue_answer=[]
    daily_revenue_answer.append(daily_revenue_answer_1)

    daily_revenue_question = {}
    daily_revenue_question["question"] = {'report_name': '2.日营收优化分析',
                                          'description': '目标：分析营收金额是否有异常情况'}
    daily_revenue_question["answer"] = daily_revenue_answer

    all_report_question.append(daily_revenue_question)


    # 3日成本优化分析

    # 1. 分析南通店第14周日成本波动情况
    cost_analysis_process_1 = []

    # 1.1 计算南通店第14周各日成本波动情况
    cost_1_q1 = "1.1 计算南通店第14周每日成本波动情况"
    cost_1_a1 = dwx.calculate_cost_fluctuation_weekly(startdate, endate, BusinessName)
    cost_1_a1_ecode = generate_ecode(cost_1_a1)
    cost_analysis_process_1.append({"question": cost_1_q1, "answer": cost_1_a1,"echart_id":"cost_1_a1_ecode","echart_code":cost_1_a1_ecode})

    # 1.2 分析南通店第14周日消费波动情况
    cost_1_q2 = "1.2 分析南通店第14周每日消费波动情况"
    cost_1_a2 = dwx.analyze_spending_fluctuation_weekly(startdate, endate, BusinessName)
    cost_1_a2_ecode = generate_ecode(cost_1_a2)
    cost_analysis_process_1.append({"question": cost_1_q2, "answer": cost_1_a2,"echart_id":"cost_1_a2_ecode","echart_code":cost_1_a2_ecode})

    # 1.3 比较南通店第13周和14周是否有当日成本超出当日营收50%的情况
    cost_1_q3 = "1.3 分析南通店第13周和14周是否有当日成本超出当日营收50%的情况"
    cost_1_a3 = dwx.compare_cost_revenue_ratio(last_start, endate, BusinessName)
    cost_1_a3_ecode = generate_ecode(cost_1_a3)
    cost_analysis_process_1.append({"question": cost_1_q3, "answer": cost_1_a3,"echart_id":"cost_1_a3_ecode","echart_code":cost_1_a3_ecode})

    cost_answer_1 = {}
    cost_answer_1["analysis_item"] = '1.分析本周成本是否有异常情况'
    cost_answer_1["description"] = '分三步执行'
    cost_answer_1["analysis_process"] = cost_analysis_process_1


    cost_answer = []
    cost_answer.append(cost_answer_1)

    # 2. 分析异常情况原因
    cost_analysis_process_2 = []

    # 2.1 分析南通店第13周一周成本均值
    cost_2_q1 = "2.1 分析南通店第13周一周成本均值"
    cost_2_a1 = dwx.analyze_weekly_average_cost(startdate, endate, BusinessName)
    cost_analysis_process_2.append({"question": cost_2_q1, "answer": cost_2_a1})

    # 2.2 分析南通店出现异常当日（1.3结论）的采购礼品价格是否有超过成本均值（2.1结论）40%的商品
    cost_2_q2 = "2.2 分析南通店出现异常当日的采购礼品价格是否有超过成本均值40%的商品"
    cost_2_a2 = dwx.analyze_abnormal_purchase(startdate, endate, BusinessName)
    cost_analysis_process_2.append({"question": cost_2_q2, "answer": cost_2_a2})

    # 2.3 给出成本异常原因  （主观问题）
    cost_2_q3 = "2.3 根据1.3，2.1和2.2的结论，分析可能的成本异常原因"
    # cost_2_a3 = "成本异常可能由于供应链问题、人工成本增加或者营销策略调整等原因引起。"
    # cost_analysis_process_2.append({"question": cost_2_q3, "answer": cost_2_a3})

#把分析需要的问题和结论加到问题中
    cost_2_q3_new= cost_2_q3+ cost_1_q3 + "\n 结论："+cost_1_a3 +cost_2_q1 +"\n 结论："+cost_2_a1+ cost_2_q2 + "\n 结论："+cost_2_a2
    # print(cost_2_q3_new)
    cost_2_a3 = {}
    cost_2_a3 = fun_Wenxin(cost_2_q3_new)
    # print(cost_2_a3)
    cost_analysis_process_2.append({"question": cost_2_q3, "answer": cost_2_a3})
    if cost_2_a3:
        print("3 2.3 data succeed")

    chat_question.append(cost_2_q3_new)


    cost_answer_2 = {}
    cost_answer_2["analysis_item"] = '2. 分析异常情况原因'
    cost_answer_2["description"] = '分三步执行'
    cost_answer_2["analysis_process"] = cost_analysis_process_2


    cost_answer.append(cost_answer_2)

#成本question
    cost_question = {}
    cost_question["question"] = {'report_name': '3.日成本优化分析',
                                    'description': '目标：根据每日的日消费与成本变化趋势，分析当日利润情况影响'}
    cost_question["answer"] = cost_answer
    # 日成本部分完成
    all_report_question.append(cost_question)

    # 4游戏币优化分析
    game_coins_analysis_process_1 = []

    game_coins_1_1_q = "1.1 计算{}-{},{}这一周周币单价波动".format(startdate,endate, BusinessName)
    game_coins_1_1_a = dwx.calculate_coin_price_fluctuation(startdate,endate, BusinessName)
    game_coins_analysis_process_1.append({"question": game_coins_1_1_q, "answer": game_coins_1_1_a})

    game_coins_1_2_q = "1.2 分析{}-{},{}这一周周日售币波动".format(startdate,endate, BusinessName)
    game_coins_1_2_a = dwx.analyze_daily_coin_sales(startdate,endate, BusinessName)
    game_coins_1_2_a_ecode = generate_ecode(game_coins_1_2_a)
    game_coins_analysis_process_1.append({"question": game_coins_1_2_q, "answer": game_coins_1_2_a, "echart_id": "game_coins_1_2",
                                       "echart_code": game_coins_1_2_a_ecode})


    # 4 1.3 主观问题
    game_coins_1_3_q = "1.3 根据1.1和1.2结论，分析这周游戏币波动是否存在异常 ，给出优化建议 "

    game_coins_1_3_q_new = game_coins_1_3_q + game_coins_1_1_q + "\n 结论：" + game_coins_1_1_a + game_coins_1_2_q + "\n 结论：" + game_coins_1_2_a
    game_coins_1_3_a = {}
    game_coins_1_3_a = fun_Wenxin(game_coins_1_3_q_new)

    # print(game_coins_1_3_q_new)
    if game_coins_1_3_a:
        print("4 1.3 data succeed")

    game_coins_analysis_process_1.append({"question": game_coins_1_3_q, "answer": game_coins_1_3_a})

    chat_question.append(game_coins_1_3_q_new)


    game_coins_answer_1 = {}
    game_coins_answer_1["analysis_item"] = '1. 分析游戏币波动是否有异常'
    game_coins_answer_1["description"] = '三个子问题'
    game_coins_answer_1["analysis_process"] = game_coins_analysis_process_1

    game_coins_answer = []
    game_coins_answer.append(game_coins_answer_1)

    game_coins_question = {}
    game_coins_question["question"] = {'report_name': '4.游戏币优化分析', 'description': '目标：分析游戏币相关数据波动是否有异常'}
    game_coins_question["answer"] = game_coins_answer

    all_report_question.append(game_coins_question)

    # 5会员优化分析
    member_analysis_process_1 = []

    member_1_1_q = "1.1 分析{}-{}一周，{}到店会员数波动情况".format(startdate,endate, BusinessName)
    member_1_1_a = dwx.analyze_member_visits(startdate,endate, BusinessName)
    member_analysis_process_1.append({"question": member_1_1_q, "answer": member_1_1_a})

    member_1_2_q = "1.2 分析{}的RFM会员占比情况".format( BusinessName)
    member_1_2_a = dwx.analyze_RFM_member_proportion(BusinessName)
    member_analysis_process_1.append({"question": member_1_2_q, "answer": member_1_2_a})

    member_1_3_q = "1.3 分析其他店铺的RFM会员总会员占比".format()
    member_1_3_a = dwx.analyze_RFM_all_member_proportion()
    member_analysis_process_1.append({"question": member_1_3_q, "answer": member_1_3_a})

# 5 1.4 (主观问题)
    member_1_4_q = "1.4.根据1.2和1.3问题结论，对比分析各类会员占比与总会员占比（第3问结论），给出优化建议".format(startdate,endate, BusinessName)
    # member_1_4_a = dwx.analyze_RFM(startdate, endate)
    # member_analysis_process.append({"question": member_1_4_q, "answer": member_1_4_a})

    member_1_4_q_new = member_1_2_q + "\n 结论：" + member_1_2_a + member_1_3_q + "\n 结论：" + member_1_3_a  + member_1_4_q
    # print(member_1_4_q_new)
    member_1_4_a = {}
    member_1_4_a = fun_Wenxin(member_1_4_q_new)
    # print(member_1_4_a)
    member_analysis_process_1.append({"question": member_1_4_q, "answer": member_1_4_a})
    if member_1_4_a:
        print("5 1.4 data succeed")

    chat_question.append(member_1_4_q_new)


    member_answer_1 = {}
    member_answer_1["analysis_item"] = '1. 会员优化分析'
    member_answer_1["description"] = '四个子问题'
    member_answer_1["analysis_process"] = member_analysis_process_1


    member_answer = []
    member_answer.append(member_answer_1)

    member_question = {}
    member_question["question"] = {'report_name': '5. 会员优化分析', 'description': '目标：分析会员情况和结构占比'}
    member_question["answer"] = member_answer

    all_report_question.append(member_question)

    # 6机台投币量优化分析
    # 机台投币量优化分析
    coin_analysis_process_1 = []

    # 1.1 分析南通店第14周的日机台投币量波动
    analysis_1_1_question = "1.1 分析{}-{}一周，{}的日机台投币量波动".format(startdate,endate, BusinessName)
    analysis_1_1_answer = dwx.calculate_coin_fluctuation(startdate,endate, BusinessName)
    coin_analysis_process_1.append({"question": analysis_1_1_question, "answer": analysis_1_1_answer})

    # 1.2 分析南通店第14周的各类机器机台投币量占比
    analysis_1_2_question = "1.2 分析{}-{}一周，{}的各类机器机台投币量占比".format(startdate,endate, BusinessName)
    analysis_1_2_answer = dwx.calculate_coin_proportion(startdate,endate, BusinessName)
    coin_analysis_process_1.append({"question": analysis_1_2_question, "answer": analysis_1_2_answer})

    # 1.3 分析南通店第14周的各类机器机台启动次数占比
    analysis_1_3_question = "1.3 分析{}-{}一周，{}的各类机器机台启动次数占比".format(startdate,endate, BusinessName)
    analysis_1_3_answer = dwx.calculate_startup_proportion(startdate,endate, BusinessName)
    coin_analysis_process_1.append({"question": analysis_1_3_question, "answer": analysis_1_3_answer})


    coin_answer_1 = {}
    coin_answer_1["analysis_item"] = '机台投币量优化分析'
    coin_answer_1["description"] = '分三步执行'
    coin_answer_1["analysis_process"] = coin_analysis_process_1

    coin_answer = []
    coin_answer.append(coin_answer_1)


    # 2 对比分析热门机器，分析是否有可优化的机器

    # 热门机器对比分析

    coin_analysis_process_2= []

    # 2.1 分析南通店和合肥店的第14周的热门机器类型和投币量
    analysis_2_1_question = "2.1 分析{}-{}一周，{}周的热门机器类型和投币量".format(startdate,endate, BusinessName)
    analysis_2_1_answer = dwx.compare_hot_machines(startdate,endate, BusinessName)
    coin_analysis_process_2.append({"question": analysis_2_1_question, "answer": analysis_2_1_answer})

    # 2.2 分析合肥店的第14周的热门机器类型和投币量
    analysis_2_2_question = "2.2 分析{}-{}一周，{}的热门机器类型和投币量".format(startdate,endate, BusinessName2)
    analysis_2_2_answer = dwx.analyze_hot_machines(startdate,endate, BusinessName2)
    coin_analysis_process_2.append({"question": analysis_2_2_question, "answer": analysis_2_2_answer})

    # 2.3 分析第14周的两家店铺的最热门机器销售额和机器类型，给出优化意见  (主观问题)
    analysis_2_3_question = "2.3 根据2.1和2.2的问题结论，对比分析两家店铺的最热门机器销售额和机器类型，给出优化意见"
    # analysis_2_3_answer = dwx.analyze_top_machines(startdate,endate, BusinessName)
    # hot_machine_analysis_process.append({"question": analysis_2_3_question, "answer": analysis_2_3_answer})

    analysis_2_3_question_new = analysis_2_1_question + "\n 结论：" + analysis_2_1_answer + analysis_2_2_question + "\n 结论：" + analysis_2_2_answer  + analysis_2_3_question
    # print(analysis_2_3_question_new)
    analysis_2_3_answer = {}
    analysis_2_3_answer = fun_Wenxin(analysis_2_3_question_new)
    # print(analysis_2_3_answer)
    coin_analysis_process_2.append({"question": analysis_2_3_question, "answer": analysis_2_3_answer})
    if analysis_2_3_answer:
        print("6 2.3 data succeed")

    chat_question.append(analysis_2_3_question_new )

    # 2.4 计算第14周的南通店热门彩票机（2.1结论）的出奖率
    analysis_2_4_question = "2.4 计算{}-{}一周，{}热门彩票机的出奖率".format(startdate,endate, BusinessName)
    analysis_2_4_answer = dwx.calculate_lottery_win_rate(startdate,endate, BusinessName)
    coin_analysis_process_2.append({"question": analysis_2_4_question, "answer": analysis_2_4_answer})

    # 2.5 计算第14周的南通店娃娃机启动次数，找出前十热门机器
    analysis_2_5_question = "2.5 计算{}-{}一周，{}的娃娃机启动次数，找出前十热门机器".format(startdate,endate, BusinessName)
    analysis_2_5_answer = dwx.calculate_claw_machine_startup(startdate,endate, BusinessName)
    coin_analysis_process_2.append({"question": analysis_2_5_question, "answer": analysis_2_5_answer})


    coin_answer_2 = {}
    coin_answer_2["analysis_item"] = '热门机器对比分析'
    coin_answer_2["description"] = '分五步执行'
    coin_answer_2["analysis_process"] = coin_analysis_process_2

    coin_answer.append(coin_answer_2)

    coin_question = {}
    coin_question["question"] = {'report_name': '6. 机台投币量优化分析', 'description': '目标：分析店铺一周的机台投币量波动和机器投币量占比，分析是否存在异常'}
    coin_question["answer"] = coin_answer

    all_report_question.append(coin_question)

    # 7机台异常分析
    machine_issue_analysis_process_1 = []

    machine_issue_1_1_q = "1.1 分析{}周南通店各类机器的投币产出异常(即投币为0但产出不为0)".format(startdate,endate, BusinessName)
    machine_issue_1_1_a = dwx.analyze_machine_coin_output_issue(startdate,endate, BusinessName)
    machine_issue_analysis_process_1.append({"question": machine_issue_1_1_q, "answer": machine_issue_1_1_a})

    machine_issue_1_2_q = "1.2 计算{}周南通店各类机器出奖异常次数".format(startdate,endate, BusinessName)
    machine_issue_1_2_a = dwx.calculate_machine_prize_issue(startdate,endate, BusinessName)
    machine_issue_analysis_process_1.append({"question": machine_issue_1_2_q, "answer": machine_issue_1_2_a})

    machine_issue_1_3_q = "1.3 分析{}周南通店故障次数异常的机器".format(startdate,endate, BusinessName)
    machine_issue_1_3_a = dwx.analyze_machine_fault_issue(startdate,endate, BusinessName)
    machine_issue_analysis_process_1.append({"question": machine_issue_1_3_q, "answer": machine_issue_1_3_a})

    machine_issue_1_4_q = "1.4 根据1.1 1.2 和1.3的结论，分析机台异常情况，给出优化建议"
    # machine_issue_1_4_a = dwx.analyze_machine_fault_issue(startdate,endate, BusinessName)

    machine_issue_1_4_q_new = machine_issue_1_4_q + machine_issue_1_1_q + "\n 结论：" + machine_issue_1_1_a + machine_issue_1_2_q + "\n 结论：" + machine_issue_1_2_a+ machine_issue_1_3_q+ "\n 结论："+machine_issue_1_3_a
    machine_issue_1_4_a = {}
    machine_issue_1_4_a = fun_Wenxin(machine_issue_1_4_q_new)

    # print(machine_issue_1_4_q_new)
    if machine_issue_1_4_a:
        print("7 1.4 data succeed")


    machine_issue_analysis_process_1.append({"question": machine_issue_1_4_q, "answer": machine_issue_1_4_a})

    chat_question.append(machine_issue_1_4_q_new )


    machine_issue_answer_1 = {}
    machine_issue_answer_1["analysis_item"] = '1. 机台异常分析'
    machine_issue_answer_1["description"] = '四个子问题'
    machine_issue_answer_1["analysis_process"] = machine_issue_analysis_process_1

#按道理不加也行，但是不加会渲染不出结果，，，，，
    machine_issue_answer = []
    machine_issue_answer.append(machine_issue_answer_1)

    machine_issue_question = {}
    machine_issue_question["question"] = {'report_name': '7.机台异常分析',
                                          'description': '目标：分析南通店第14周投币产出异常机台，出奖异常机台，故障次数异常的机台'}
    machine_issue_question["answer"] = machine_issue_answer

    all_report_question.append(machine_issue_question)



#-------------------------以上是子问题分析---------------------


    # TODO 增加 优化策略模块——总结几个模块的主观问题，调用gpt
    # chat_question.append(("请根据以上问题和结论，分点给出对应电玩猩{}，在{}-{}这一周的优化建议").format(BusinessName,startdate,endate))
    # chat_answer={}
    #
    # print(chat_question)
    #
    # chat_answer=fun_Wenxin(chat_question)
    # print(chat_answer)
    #
    # if chat_answer:
    #     print("Analysis succeed")
    #
    # REPORT_ANALYST["analysis_item"] = chat_question
    # REPORT_ANALYST["analysis_item"] = chat_answer
# -------------------------以上是chat生成优化策略---------------------


    """执行上述构建"""
    last_answer["report_question"] = all_report_question
    # 增加report_thought
    last_answer["report_thought"]=REPORT_THOUGHT
    # 增加report_analyst
    last_answer["report_analyst"] = REPORT_ANALYST
    # last_answer["report_analyst"].append(chat_answer)

    # 增加前端展示图形测试 --auserwn
    # last_answer["autogen_echart"]=generate_ecode(analysis_2_5_answer)

    print(last_answer)
    return last_answer


# res = auto_generepot('US','2024-04-01','2024-04-14')
def generate():
    last_answer = auto_generepot('江苏南通电玩猩-南通店', '2024-04-01', '2024-04-07','安徽合肥电玩猩-合肥店','2024-03-25','2024-03-31')
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
    with open('output_dwx_test.html', 'w', encoding='utf-8') as output_file:
        output_file.write(rendered_html)

    print("HTML文件已生成：output_dwx_test.html")

generate()

