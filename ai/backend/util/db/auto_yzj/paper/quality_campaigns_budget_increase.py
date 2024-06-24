# filename: quality_campaigns_budget_increase.py

import pandas as pd
from datetime import datetime, timedelta

# 读取CSV数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\预算优化\预处理.csv'
data = pd.read_csv(file_path)

# 确保date列为datetime格式
data['date'] = pd.to_datetime(data['date'])

# 假设今天是2024年5月28日，因此昨天是2024年5月27日
today = datetime(2024, 5, 28)
yesterday = today - timedelta(days=1)

# 筛选最近7天和昨天的数据
last_7_days_data = data[data['date'] >= today - timedelta(days=7)]
yesterday_data = data[data['date'] == yesterday]

# 筛选满足条件的广告活动
good_campaigns = []

for campaign in data['campaignName'].unique():
    campaign_last_7_days = last_7_days_data[last_7_days_data['campaignName'] == campaign]
    campaign_yesterday = yesterday_data[yesterday_data['campaignName'] == campaign]
    
    if not campaign_yesterday.empty and not campaign_last_7_days.empty:
        avg_ACOS_7d = campaign_last_7_days['avg_ACOS_7d'].iloc[0]
        ACOS_yesterday = campaign_yesterday['ACOS'].iloc[0]
        cost_yesterday = campaign_yesterday['cost'].iloc[0]
        budget_yesterday = campaign_yesterday['Budget'].iloc[0]
        
        if avg_ACOS_7d < 0.24 and ACOS_yesterday < 0.24 and cost_yesterday > 0.8 * budget_yesterday:
            new_budget = min(budget_yesterday * 1.2, 50)
            good_campaigns.append({
                'date': yesterday,
                'campaignName': campaign,
                'Budget': new_budget,
                'cost': cost_yesterday,
                'clicks': campaign_yesterday['clicks'].iloc[0],
                'ACOS': ACOS_yesterday,
                'avg_ACOS_7d': avg_ACOS_7d,
                'avg_ACOS_1m': campaign_yesterday['avg_ACOS_1m'].iloc[0],
                'clicks_1m': campaign_yesterday['clicks_1m'].iloc[0],
                'sales_1m': campaign_yesterday['sales_1m'].iloc[0],
                'reason': '符合所有条件，提高预算至新的预算值'
            })

# 转换为DataFrame并保存到CSV
result_df = pd.DataFrame(good_campaigns)
result_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\预算优化\提问策略\优质广告活动_FR.csv'
result_df.to_csv(result_file_path, index=False)

print(f'结果已保存到 {result_file_path}')