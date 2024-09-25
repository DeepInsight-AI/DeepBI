# filename: handle_ad_performance.py
import os
import pandas as pd
import numpy as np
from ai.backend.util.db.auto_process.tools_db_new_sp import DbNewSpTools
from ai.backend.util.db.auto_process.summary.db_tool.tools_db import AmazonMysqlRagUitl
from datetime import datetime

def main(path, brand, cur_time, country, version=3):
    # 读取CSV文件
    file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\预算优化\预处理.csv'
    file_name = "自动_劣质广告活动" + '_' + brand + '_' + country + '_' + cur_time + '.csv'
    output_file_path = os.path.join(path, file_name)
    data = pd.read_csv(file_path)

    # 定义结果列表
    results = []

    # 遍历广告活动数据并检查每条数据是否满足条件
    for index, row in data.iterrows():
        campaignId = row['campaignId']
        campaignName = row['campaignName']
        Budget = row['Budget']
        ACOS_7d = row['ACOS_7d']
        ACOS_yesterday = row['ACOS_yesterday']
        clicks_yesterday = row['clicks_yesterday']
        cost_yesterday = row['cost_yesterday']
        country_avg_ACOS_1m = row['country_avg_ACOS_1m']
        ACOS_30d = row['ACOS_30d']
        total_clicks_7d = row['total_clicks_7d']
        total_clicks_30d = row['total_clicks_30d']
        total_sales14d_7d = row['total_sales14d_7d']
        total_sales14d_30d = row['total_sales14d_30d']
        total_cost_7d = row['total_cost_7d']
        if version == 1:
            # 定义一
            if (ACOS_7d > 0.24 and ACOS_yesterday > 0.24 and clicks_yesterday >= 10 and ACOS_30d > country_avg_ACOS_1m):
                reason = '定义一'
                new_budget = max(8, Budget - 5)
                results.append([campaignId, campaignName, Budget, new_budget, clicks_yesterday, ACOS_yesterday, ACOS_7d,
                                total_clicks_7d, total_sales14d_7d, ACOS_30d, total_clicks_30d, total_sales14d_30d,
                                country_avg_ACOS_1m, reason])

            # 定义二
            elif (ACOS_7d > 0.24 and ACOS_yesterday > 0.24 and cost_yesterday > 0.8 * Budget and ACOS_30d > country_avg_ACOS_1m):
                reason = '定义二'
                new_budget = max(8, Budget - 5)
                results.append([campaignId, campaignName, Budget, new_budget, clicks_yesterday, ACOS_yesterday, ACOS_7d,
                                total_clicks_7d, total_sales14d_7d, ACOS_30d, total_clicks_30d, total_sales14d_30d,
                                country_avg_ACOS_1m, reason])

            # 定义三
            elif (ACOS_30d > 0.24 and ACOS_30d > country_avg_ACOS_1m and total_sales14d_7d == 0 and total_clicks_7d >= 15):
                reason = '定义三'
                new_budget = max(5, Budget - 5)
                results.append([campaignId, campaignName, Budget, new_budget, clicks_yesterday, ACOS_yesterday, ACOS_7d,
                                total_clicks_7d, total_sales14d_7d, ACOS_30d, total_clicks_30d, total_sales14d_30d,
                                country_avg_ACOS_1m, reason])

            # 定义四
            elif (total_sales14d_30d == 0 and total_clicks_30d >= 75):
                reason = '定义四'
                new_budget = '关闭'
                results.append([campaignId, campaignName, Budget, new_budget, clicks_yesterday, ACOS_yesterday, ACOS_7d,
                                total_clicks_7d, total_sales14d_7d, ACOS_30d, total_clicks_30d, total_sales14d_30d,
                                country_avg_ACOS_1m, reason])
        elif version == 2:
            # 定义一
            if (ACOS_7d > 0.24 and ACOS_yesterday > 0.24 and clicks_yesterday >= 10 and ACOS_30d > country_avg_ACOS_1m):
                reason = '定义一'
                bid_adjust = -5
                new_budget = max(8, Budget - 5) if Budget >= 8 else Budget
                results.append([campaignId, campaignName, Budget, new_budget, clicks_yesterday, ACOS_yesterday, ACOS_7d,
                                total_clicks_7d, total_sales14d_7d, ACOS_30d, total_clicks_30d, total_sales14d_30d,
                                country_avg_ACOS_1m, reason, bid_adjust])

            # 定义二
            elif (
                ACOS_7d > 0.24 and ACOS_yesterday > 0.24 and cost_yesterday > 0.8 * Budget and ACOS_30d > country_avg_ACOS_1m):
                reason = '定义二'
                bid_adjust = -5
                new_budget = max(8, Budget - 5) if Budget >= 8 else Budget
                results.append([campaignId, campaignName, Budget, new_budget, clicks_yesterday, ACOS_yesterday, ACOS_7d,
                                total_clicks_7d, total_sales14d_7d, ACOS_30d, total_clicks_30d, total_sales14d_30d,
                                country_avg_ACOS_1m, reason, bid_adjust])

            # 定义三
            elif (
                ACOS_30d > 0.24 and ACOS_30d > country_avg_ACOS_1m and total_sales14d_7d == 0 and total_clicks_7d >= 15):
                reason = '定义三'
                bid_adjust = -5
                new_budget = max(5, Budget - 5)
                results.append([campaignId, campaignName, Budget, new_budget, clicks_yesterday, ACOS_yesterday, ACOS_7d,
                                total_clicks_7d, total_sales14d_7d, ACOS_30d, total_clicks_30d, total_sales14d_30d,
                                country_avg_ACOS_1m, reason, bid_adjust])

            # 定义四
            elif (total_sales14d_30d == 0 and total_clicks_30d >= 75):
                reason = '定义四'
                bid_adjust = 0
                new_budget = '关闭'
                results.append([campaignId, campaignName, Budget, new_budget, clicks_yesterday, ACOS_yesterday, ACOS_7d,
                                total_clicks_7d, total_sales14d_7d, ACOS_30d, total_clicks_30d, total_sales14d_30d,
                                country_avg_ACOS_1m, reason, bid_adjust])
        if version == 3 or version == '初阶':
            if ACOS_30d > 0.24 and ACOS_30d > country_avg_ACOS_1m and total_sales14d_7d == 0 and total_clicks_7d >= 15 and total_cost_7d > 13:
                reason = '定义一'
                bid_adjust = -5
                new_budget = max(5, Budget - 5)
                results.append([campaignId, campaignName, Budget, new_budget, clicks_yesterday, ACOS_yesterday, ACOS_7d,
                                total_clicks_7d, total_sales14d_7d, ACOS_30d, total_clicks_30d, total_sales14d_30d,
                                country_avg_ACOS_1m, reason, bid_adjust])

            # 定义四
            elif (total_sales14d_30d == 0 and total_clicks_30d >= 75):
                reason = '定义二'
                bid_adjust = 0
                new_budget = '关闭'
                results.append([campaignId, campaignName, Budget, new_budget, clicks_yesterday, ACOS_yesterday, ACOS_7d,
                                total_clicks_7d, total_sales14d_7d, ACOS_30d, total_clicks_30d, total_sales14d_30d,
                                country_avg_ACOS_1m, reason, bid_adjust])
    # 转换为DataFrame并保存为新的CSV文件
    columns = ['campaignId', 'campaignName', 'Budget', 'New_Budget', 'clicks_yesterday', 'ACOS_yesterday', 'ACOS_7d',
               'total_clicks_7d', 'total_sales14d_7d', 'ACOS_30d', 'total_clicks_30d', 'total_sales14d_30d',
               'country_avg_ACOS_1m', 'Reason', 'bid_adjust']
    results_df = pd.DataFrame(results, columns=columns)
    api2 = AmazonMysqlRagUitl(brand,country)
    excluded_campaign_ids = api2.get_operated_campaign(country,cur_time)
    if excluded_campaign_ids:
        excluded_campaign_ids = [int(campaign_id) for campaign_id in excluded_campaign_ids]
        results_df = results_df[~results_df['campaignId'].isin(excluded_campaign_ids)]
    results_df.replace({np.nan: None}, inplace=True)
    api = DbNewSpTools(brand,country)
    for index, row in results_df.iterrows():
        api.create_budget_info(country, brand, '日常优化', '自动_劣质', row['campaignId'], row['campaignName'],
                               row['Budget'], row['New_Budget'], None, row['clicks_yesterday'], row['ACOS_yesterday'],
                               row['total_clicks_7d'], row['total_sales14d_7d'], row['ACOS_7d'], row['ACOS_30d'],
                               row['total_clicks_30d'], row['total_sales14d_30d'], row['Reason'],
                               row['country_avg_ACOS_1m'],row['bid_adjust'], cur_time, datetime.now(), 0)
    # 保存到新的CSV文件
    results_df.to_csv(output_file_path, index=False)
    print(f'分析结果已保存在 {output_file_path} 文件中。')
