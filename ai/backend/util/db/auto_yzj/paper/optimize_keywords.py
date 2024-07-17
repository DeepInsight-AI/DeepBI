# filename: optimize_keywords.py
import os
import pandas as pd
import numpy as np
from ai.backend.util.db.auto_process.tools_db_new_sp import DbNewSpTools
from datetime import datetime


def main(path, brand, cur_time, country, version: int = 1):
    # 读取数据
    file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv'
    file_name = "手动_ASIN_劣质商品投放" + '_' + brand + '_' + country + '_' + cur_time + '.csv'
    output_file_path = os.path.join(path, file_name)
    data = pd.read_csv(file_path)

    # 定义函数来调整关键词竞价
    def adjust_bid(row, cause):
        avg_ACOS_7d = row['ACOS_7d']
        keywordBid = row['keywordBid']
        new_bid = keywordBid / ((avg_ACOS_7d - 0.24) / 0.24 + 1)

        if new_bid < 0.05:
            new_bid = max(keywordBid, 0.05)

        return round(new_bid, 2), cause

    # 遍历数据并进行判断
    results = []

    for idx, row in data.iterrows():
        keyword_bid = row['keywordBid']
        avg_ACOS_7d = row['ACOS_7d']
        avg_ACOS_30d = row['ACOS_30d']
        avg_ACOS_3d = row['ACOS_3d']
        total_clicks_7d = row['total_clicks_7d']
        total_sales14d_7d = row['total_sales14d_7d']
        total_cost_7d = row['total_cost_7d']
        total_cost_30d = row['total_cost_30d']
        order_1m = row['ORDER_1m']
        total_clicks_30d = row['total_clicks_30d']
        total_sales14d_3d = row['total_sales14d_3d']
        total_sales14d_30d = row['total_sales14d_30d']
        total_cost_3d = row['total_cost_3d']

        new_bid = keyword_bid
        cause = ""

        if 0.24 < avg_ACOS_7d <= 0.5 and 0 < avg_ACOS_30d <= 0.5 and order_1m < 5 and avg_ACOS_3d >= 0.24:
            new_bid, cause = adjust_bid(row, "定义一")
        elif avg_ACOS_7d > 0.5 and avg_ACOS_30d <= 0.36 and avg_ACOS_3d > 0.24:
            new_bid, cause = adjust_bid(row, "定义二")
        elif total_clicks_7d >= 10 and total_sales14d_7d == 0 and total_cost_7d <= 5 and avg_ACOS_30d <= 0.36:
            new_bid = max(keyword_bid - 0.03, 0.05)
            cause = "定义三"
        elif total_clicks_7d > 10 and total_sales14d_7d == 0 and total_cost_7d > 7 and avg_ACOS_30d > 0.5:
            new_bid = 0.05
            cause = "定义四"
        elif avg_ACOS_7d > 0.5 and avg_ACOS_3d > 0.24 and avg_ACOS_30d > 0.36:
            new_bid = 0.05
            cause = "定义五"
        elif total_sales14d_30d == 0 and total_cost_30d >= 10 and total_clicks_30d >= 15:
            new_bid = 0.05
            cause = "定义六"
        elif 0.24 < avg_ACOS_7d <= 0.5 and 0 < avg_ACOS_30d <= 0.5 and order_1m < 5 and total_sales14d_3d == 0:
            new_bid, cause = adjust_bid(row, "定义七")
        elif avg_ACOS_7d > 0.5 and avg_ACOS_30d <= 0.36 and total_sales14d_3d == 0:
            new_bid, cause = adjust_bid(row, "定义八")
        elif avg_ACOS_7d > 0.5 and total_sales14d_3d == 0 and avg_ACOS_30d > 0.36:
            new_bid = 0.05
            cause = "定义九"
        elif 0.24 < avg_ACOS_7d <= 0.5 and avg_ACOS_30d > 0.5 and order_1m < 5 and avg_ACOS_3d >= 0.24:
            new_bid, cause = adjust_bid(row, "定义十")
        elif 0.24 < avg_ACOS_7d <= 0.5 and total_sales14d_3d == 0 and avg_ACOS_30d > 0.5:
            new_bid, cause = adjust_bid(row, "定义十一")
        elif avg_ACOS_7d <= 0.24 and total_sales14d_3d == 0 and 3 < total_cost_3d < 5:
            new_bid = max(keyword_bid - 0.01, 0.05)
            cause = "定义十二"
        elif avg_ACOS_7d <= 0.24 and 0.24 < avg_ACOS_3d < 0.36:
            new_bid = max(keyword_bid - 0.02, 0.05)
            cause = "定义十三"
        elif avg_ACOS_7d <= 0.24 and avg_ACOS_3d > 0.36:
            new_bid = max(keyword_bid - 0.03, 0.05)
            cause = "定义十四"
        elif total_clicks_7d >= 10 and total_sales14d_7d == 0 and total_cost_7d >= 10 and avg_ACOS_30d <= 0.36:
            new_bid = 0.05
            cause = "定义十五"
        elif total_clicks_7d >= 10 and total_sales14d_7d == 0 and 5 < total_cost_7d < 10 and avg_ACOS_30d <= 0.36:
            new_bid = max(keyword_bid - 0.07, 0.05)
            cause = "定义十六"

        if cause:
            results.append([
                row['keyword'], row['keywordId'], row['campaignName'], row['adGroupName'], row['matchType'], keyword_bid,
                new_bid, row['targeting'], avg_ACOS_30d,order_1m,total_clicks_30d,total_sales14d_30d,total_cost_30d,avg_ACOS_7d,
                total_clicks_7d,total_sales14d_7d,total_cost_7d,avg_ACOS_3d,total_sales14d_3d,total_cost_3d, cause
            ])

    # 生成结果CSV文件
    results_df = pd.DataFrame(results, columns=[
        'keyword', 'keywordId', 'campaignName', 'adGroupName', 'matchType', 'keywordBid', 'new_keywordBid', 'targeting',
        'ACOS_30d', 'ORDER_1m', 'total_clicks_30d', 'total_sales14d_30d', 'total_cost_30d', 'ACOS_7d', 'total_clicks_7d',
        'total_sales14d_7d', 'total_cost_7d', 'ACOS_3d', 'total_sales14d_3d', 'total_cost_3d', 'cause'
    ])


    results_df.to_csv(output_file_path, index=False)

    print("CSV 文件已经生成并保存在: ", output_file_path)
