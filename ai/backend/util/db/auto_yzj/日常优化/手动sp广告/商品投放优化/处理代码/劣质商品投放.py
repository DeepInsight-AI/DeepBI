# filename: optimize_keywords.py
import os
import pandas as pd
import numpy as np
from ai.backend.util.db.auto_process.tools_db_new_sp import DbNewSpTools
from datetime import datetime


def main(path, brand, cur_time, country, version=2):
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
    if version == 1:
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
            bid_adjust = ""
            cause = ""

            if 0.24 < avg_ACOS_7d <= 0.5 and 0 < avg_ACOS_30d <= 0.5 and order_1m < 5 and avg_ACOS_3d >= 0.24:
                new_bid, cause = adjust_bid(row, "定义一")
                bid_adjust = 0
            elif avg_ACOS_7d > 0.5 and avg_ACOS_30d <= 0.36 and avg_ACOS_3d > 0.24:
                new_bid, cause = adjust_bid(row, "定义二")
                bid_adjust = 0
            elif total_clicks_7d >= 10 and total_sales14d_7d == 0 and total_cost_7d <= 5 and avg_ACOS_30d <= 0.36:
                new_bid = max(keyword_bid - 0.03, 0.05)
                cause = "定义三"
                bid_adjust = -0.03
            elif total_clicks_7d > 10 and total_sales14d_7d == 0 and total_cost_7d > 7 and avg_ACOS_30d > 0.5:
                new_bid = 0.05
                cause = "定义四"
                bid_adjust = -1
            elif avg_ACOS_7d > 0.5 and avg_ACOS_3d > 0.24 and avg_ACOS_30d > 0.36:
                new_bid = 0.05
                cause = "定义五"
                bid_adjust = -1
            elif total_sales14d_30d == 0 and total_cost_30d >= 10 and total_clicks_30d >= 15:
                new_bid = 0.05
                cause = "定义六"
                bid_adjust = -1
            elif 0.24 < avg_ACOS_7d <= 0.5 and 0 < avg_ACOS_30d <= 0.5 and order_1m < 5 and total_sales14d_3d == 0:
                new_bid, cause = adjust_bid(row, "定义七")
                bid_adjust = 0
            elif avg_ACOS_7d > 0.5 and avg_ACOS_30d <= 0.36 and total_sales14d_3d == 0:
                new_bid, cause = adjust_bid(row, "定义八")
                bid_adjust = 0
            elif avg_ACOS_7d > 0.5 and total_sales14d_3d == 0 and avg_ACOS_30d > 0.36:
                new_bid = 0.05
                cause = "定义九"
                bid_adjust = -1
            elif 0.24 < avg_ACOS_7d <= 0.5 and avg_ACOS_30d > 0.5 and order_1m < 5 and avg_ACOS_3d >= 0.24:
                new_bid, cause = adjust_bid(row, "定义十")
                bid_adjust = 0
            elif 0.24 < avg_ACOS_7d <= 0.5 and total_sales14d_3d == 0 and avg_ACOS_30d > 0.5:
                new_bid, cause = adjust_bid(row, "定义十一")
                bid_adjust = 0
            elif avg_ACOS_7d <= 0.24 and total_sales14d_3d == 0 and 3 < total_cost_3d < 5:
                new_bid = max(keyword_bid - 0.01, 0.05)
                cause = "定义十二"
                bid_adjust = -0.01
            elif avg_ACOS_7d <= 0.24 and 0.24 < avg_ACOS_3d < 0.36:
                new_bid = max(keyword_bid - 0.02, 0.05)
                cause = "定义十三"
                bid_adjust = -0.02
            elif avg_ACOS_7d <= 0.24 and avg_ACOS_3d > 0.36:
                new_bid = max(keyword_bid - 0.03, 0.05)
                cause = "定义十四"
                bid_adjust = -0.03
            elif total_clicks_7d >= 10 and total_sales14d_7d == 0 and total_cost_7d >= 10 and avg_ACOS_30d <= 0.36:
                new_bid = 0.05
                cause = "定义十五"
                bid_adjust = -1
            elif total_clicks_7d >= 10 and total_sales14d_7d == 0 and 5 < total_cost_7d < 10 and avg_ACOS_30d <= 0.36:
                new_bid = max(keyword_bid - 0.07, 0.05)
                cause = "定义十六"
                bid_adjust = -0.07

            if cause:
                results.append([
                    row['keyword'], row['keywordId'], row['campaignName'], row['adGroupName'], row['matchType'], keyword_bid,
                    new_bid, row['targeting'], avg_ACOS_30d,order_1m,total_clicks_30d,total_sales14d_30d,total_cost_30d,avg_ACOS_7d,
                    total_clicks_7d,total_sales14d_7d,total_cost_7d,avg_ACOS_3d,total_sales14d_3d,total_cost_3d, cause, bid_adjust
                ])
    elif version == 2:
        for idx, row in data.iterrows():
            keyword_bid = row['bid']
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
            bid_adjust = ""
            cause = ""

            if avg_ACOS_7d > 0.27 and avg_ACOS_30d > 0.27 and avg_ACOS_3d > 0.27:
                new_bid = max(keyword_bid - 0.02, 0.05)
                cause = "定义一"
                bid_adjust = -0.02
            elif total_sales14d_30d == 0 and total_clicks_30d > 15:
                new_bid = max(keyword_bid - 0.02, 0.05)
                cause = "定义二"
                bid_adjust = -0.02

            if cause:
                results.append([
                    row['keyword'], row['keywordId'], row['campaignName'], row['adGroupName'], row['matchType'],
                    keyword_bid,
                    new_bid, row['targeting'], avg_ACOS_30d, order_1m, total_clicks_30d, total_sales14d_30d,
                    total_cost_30d, avg_ACOS_7d,
                    total_clicks_7d, total_sales14d_7d, total_cost_7d, avg_ACOS_3d, total_sales14d_3d, total_cost_3d,
                    cause, bid_adjust
                ])
    # 生成结果CSV文件
    results_df = pd.DataFrame(results, columns=[
        'keyword', 'keywordId', 'campaignName', 'adGroupName', 'matchType', 'keywordBid', 'New_keywordBid', 'targeting',
        'ACOS_30d', 'ORDER_1m', 'total_clicks_30d', 'total_sales14d_30d', 'total_cost_30d', 'ACOS_7d', 'total_clicks_7d',
        'total_sales14d_7d', 'total_cost_7d', 'ACOS_3d', 'total_sales14d_3d', 'total_cost_3d', 'cause', 'bid_adjust'
    ])
    results_df.replace({np.nan: None}, inplace=True)
    api = DbNewSpTools(brand,country)
    for index, row in results_df.iterrows():
        api.create_product_targets_info(country, brand, '日常优化', '手动_劣质', row['keyword'], row['keywordId'],
                                row['campaignName'], row['adGroupName'], row['matchType'], row['keywordBid'],
                                row['New_keywordBid'], row['ACOS_30d'], row['ORDER_1m'], row['total_clicks_30d'],
                                row['total_sales14d_30d'], row['total_cost_30d'],None,
                                row['ACOS_7d'], row['total_clicks_7d'], row['total_sales14d_7d'], row['total_cost_7d'],
                                row['ACOS_3d'], row['total_sales14d_3d'], row['total_cost_3d'], row['cause'],row['bid_adjust'],
                                cur_time, datetime.now(), 0)

    results_df.to_csv(output_file_path, index=False)

    print("CSV 文件已经生成并保存在: ", output_file_path)

#main('C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/输出结果/LAPASA_US_2024-07-15','LAPASA','2024-07-15','US')
