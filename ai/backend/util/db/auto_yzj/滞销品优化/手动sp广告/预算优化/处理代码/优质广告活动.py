# filename: increase_budget.py
import os
import pandas as pd
import numpy as np
from ai.backend.util.db.auto_process.tools_db_new_sp import DbNewSpTools
from ai.backend.util.db.auto_process.summary.db_tool.tools_db import AmazonMysqlRagUitl
from datetime import datetime


def main(path, brand, cur_time, country, version=2):
    # 读取CSV文件
    file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\滞销品优化\手动sp广告\预算优化\预处理.csv'
    file_name = "手动_优质广告活动" + '_' + brand + '_' + country + '_' + cur_time + '.csv'
    output_file_path = os.path.join(path, file_name)
    data = pd.read_csv(file_path)
    if version == 1:
        # 定义增加预算的函数
        def increase_budget(row, increase_factor=0.2, max_budget=50):
            new_budget = row['Budget'] * (1 + increase_factor)
            if new_budget > max_budget:
                new_budget = max_budget
            return new_budget

        # 筛选符合条件的广告活动
        good_campaigns = data[
            (data['ACOS_7d'] < 0.27) &
            (data['ACOS_yesterday'] < 0.27) &
            (data['cost_yesterday'] > data['Budget'] * 0.8)
        ]

        # 增加原来预算的1/5，直到预算为50
        good_campaigns['New_Budget'] = good_campaigns.apply(increase_budget, axis=1)

        # 添加原因字段
        good_campaigns['Reason'] = 'Performance is good based on 7d ACOS and yesterday ACOS and yesterday cost exceeds 80% of budget'

        # 选择需要的字段
        output_columns = [
            'campaignId', 'campaignName', 'Budget', 'New_Budget', 'cost_yesterday',
            'clicks_yesterday', 'ACOS_yesterday', 'ACOS_7d', 'ACOS_30d',
            'total_clicks_30d', 'total_sales14d_30d', 'Reason'
        ]
        output_data = good_campaigns[output_columns]
    elif version == 2:
        def get_new_budget(row):
            new_budget = row['Budget']
            reason = None
            bid_adjust = None

            if (
                row['ACOS_7d'] < 0.27 and
                row['ACOS_yesterday'] < 0.27 and
                row['Budget'] >= 20 and
                row['cost_yesterday'] > 0.8 * row['Budget']
            ):
                new_budget = row['Budget'] + 5
                bid_adjust = 5
                reason = "定义一"

            elif (
                row['ACOS_7d'] < 0.3 and
                row['ACOS_yesterday'] < 0.3 and
                row['Budget'] < 20 and
                row['cost_yesterday'] > 0.8 * row['Budget']
            ):
                new_budget = row['Budget'] + 3
                bid_adjust = 3
                reason = "定义二"

            return new_budget, bid_adjust, reason

        # 应用筛选和计算
        data['New_Budget'], data['bid_adjust'], data['Reason'] = zip(*data.apply(get_new_budget, axis=1))

        # 过滤有变化的行
        result_df = data[data['Reason'].notnull()]

        # 选择并重命名需要的列
        output_data = result_df[[
            'campaignId', 'campaignName', 'Budget', 'New_Budget', 'cost_yesterday',
            'clicks_yesterday', 'ACOS_yesterday', 'ACOS_7d', 'ACOS_30d',
            'total_clicks_30d', 'total_sales14d_30d', 'Reason', 'bid_adjust'
        ]]
    api2 = AmazonMysqlRagUitl(brand,country)
    excluded_campaign_ids = api2.get_operated_campaign(country,cur_time)
    if excluded_campaign_ids:
        excluded_campaign_ids = [int(campaign_id) for campaign_id in excluded_campaign_ids]
        output_data = output_data[~output_data['campaignId'].isin(excluded_campaign_ids)]
    output_data.replace({np.nan: None}, inplace=True)
    api = DbNewSpTools(brand,country)
    for index, row in output_data.iterrows():
        api.create_budget_info(country, brand, '滞销品优化', '手动_优质', row['campaignId'], row['campaignName'],
                               row['Budget'], row['New_Budget'], row['cost_yesterday'], row['clicks_yesterday'],
                               row['ACOS_yesterday'],None,None, row['ACOS_7d'], row['ACOS_30d'], row['total_clicks_30d'],
                               row['total_sales14d_30d'], row['Reason'], None,row['bid_adjust'], cur_time, datetime.now(), 0)
    # 保存到新的CSV文件
    output_data.to_csv(output_file_path, index=False)

    print(f"数据已成功保存到 {output_file_path}")

#main('C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/滞销品优化/输出结果/LAPASA_US_2024-07-17','LAPASA','2024-07-17','US')
