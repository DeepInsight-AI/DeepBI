# filename: increase_budget.py
import os
import pandas as pd
import numpy as np
from ai.backend.util.db.auto_process.tools_db_new_sp import DbNewSpTools
from ai.backend.util.db.auto_process.summary.db_tool.tools_db import AmazonMysqlRagUitl
from datetime import datetime


def main(path, brand, cur_time, country):
    # 读取CSV文件
    file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\滞销品优化\自动sp广告\预算优化\预处理.csv'
    file_name = "自动_优质广告活动" + '_' + brand + '_' + country + '_' + cur_time + '.csv'
    output_file_path = os.path.join(path, file_name)
    data = pd.read_csv(file_path)

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
    api2 = AmazonMysqlRagUitl(brand)
    excluded_campaign_ids = api2.get_operated_campaign(country,cur_time)
    if excluded_campaign_ids:
        excluded_campaign_ids = [int(campaign_id) for campaign_id in excluded_campaign_ids]
        output_data = output_data[~output_data['campaignId'].isin(excluded_campaign_ids)]
    output_data.replace({np.nan: None}, inplace=True)
    api = DbNewSpTools(brand)
    for index, row in output_data.iterrows():
        api.create_budget_info(country, brand, '滞销品优化', '自动_优质', row['campaignId'], row['campaignName'],
                               row['Budget'], row['New_Budget'], row['cost_yesterday'], row['clicks_yesterday'],
                               row['ACOS_yesterday'],None,None, row['ACOS_7d'], row['ACOS_30d'], row['total_clicks_30d'],
                               row['total_sales14d_30d'], row['Reason'], None, cur_time, datetime.now(), 0)
    # 保存到新的CSV文件
    output_data.to_csv(output_file_path, index=False)

    print(f"数据已成功保存到 {output_file_path}")
