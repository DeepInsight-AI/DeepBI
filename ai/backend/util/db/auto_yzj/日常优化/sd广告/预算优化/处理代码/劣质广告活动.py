# filename: handle_ad_performance.py
import os
import pandas as pd
import numpy as np
from ai.backend.util.db.auto_process.tools_db_new_sp import DbNewSpTools
from ai.backend.util.db.auto_process.summary.db_tool.tools_db import AmazonMysqlRagUitl
from datetime import datetime


def main(path, brand, cur_time, country, version=2):
    # 读取CSV文件
    file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\预处理.csv'
    file_name = "SD_劣质广告sd活动" + '_' + brand + '_' + country + '_' + cur_time + '.csv'
    output_file_path = os.path.join(path, file_name)
    data = pd.read_csv(file_path)

    # 定义结果列表
    results = []

    # 遍历广告活动数据并检查每条数据是否满足条件
    for index, row in data.iterrows():
        campaignId = row['campaignId']
        campaignName = row['campaignName']
        Budget = row['campaignBudget']
        ACOS_7d = row['ACOS7d']
        ACOS_yesterday = row['ACOSYesterday']
        cost_yesterday = row['costYesterday']
        totalCost7d = row['totalCost7d']
        country_avg_ACOS_1m = row['countryAvgACOS1m']
        ACOS_30d = row['ACOS30d']
        total_sales14d_7d = row['totalSales7d']
        if version == 1:
            # 定义一
            if ACOS_7d > 0.24 and ACOS_yesterday > 0.24 and cost_yesterday >= 5.5 and ACOS_30d > country_avg_ACOS_1m:
                reason = '定义一'
                new_budget = max(8, Budget - 5)
                bid_adjust = -5
                results.append([campaignId, campaignName, Budget, new_budget, ACOS_yesterday,cost_yesterday, ACOS_7d,
                                total_sales14d_7d, ACOS_30d, country_avg_ACOS_1m, reason, bid_adjust])

            # 定义二
            elif ACOS_30d > 0.24 and total_sales14d_7d == 0 and totalCost7d > 10 and ACOS_30d > country_avg_ACOS_1m:
                reason = '定义二'
                new_budget = max(5, Budget - 5)
                bid_adjust = -5
                results.append([campaignId, campaignName, Budget, new_budget, ACOS_yesterday,cost_yesterday, ACOS_7d,
                                total_sales14d_7d, ACOS_30d, country_avg_ACOS_1m, reason, bid_adjust])
        if version == 2:
            # 定义一
            if ACOS_30d > 0.24 and total_sales14d_7d == 0 and totalCost7d > 10 and ACOS_30d > country_avg_ACOS_1m:
                reason = '定义一'
                new_budget = max(5, Budget - 5)
                bid_adjust = -5
                results.append([campaignId, campaignName, Budget, new_budget, ACOS_yesterday,cost_yesterday, ACOS_7d,
                                total_sales14d_7d, ACOS_30d, country_avg_ACOS_1m, reason, bid_adjust])


    # 转换为DataFrame并保存为新的CSV文件
    columns = ['campaignId', 'campaignName', 'Budget', 'New_Budget', 'ACOS_yesterday', 'cost_yesterday', 'ACOS_7d',
               'total_sales14d_7d', 'ACOS_30d', 'country_avg_ACOS_1m', 'Reason', 'bid_adjust']
    results_df = pd.DataFrame(results, columns=columns)
    results_df.replace({np.nan: None}, inplace=True)
    api = DbNewSpTools(brand,country)
    for index, row in results_df.iterrows():
        api.create_budget_info(country,brand,'日常优化','SD_劣质',row['campaignId'],row['campaignName'],row['Budget'],row['New_Budget'],row['cost_yesterday'],None,row['ACOS_yesterday'],None,row['total_sales14d_7d'],row['ACOS_7d'],row['ACOS_30d'],None,None,row['Reason'],row['country_avg_ACOS_1m'],row['bid_adjust'],cur_time,datetime.now(),0)
    # 保存到新的CSV文件

    results_df.to_csv(output_file_path, index=False)
    print(f'分析结果已保存在 {output_file_path} 文件中。')

#main('C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/输出结果/LAPASA_US_2024-07-18','LAPASA','2024-07-18','US')
