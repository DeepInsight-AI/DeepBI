# filename: optimize_campaign.py

import pandas as pd
from datetime import datetime, timedelta

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\预处理.csv'
data = pd.read_csv(file_path)

# 获取今天和昨天的日期
today = datetime(2024, 5, 28)
yesterday = today - timedelta(days=1)
yesterday_str = yesterday.strftime('%Y-%m-%d')

print(f"Today: {today.strftime('%Y-%m-%d')}, Yesterday: {yesterday_str}")

# 筛选表现很好的优质广告活动
print("Filtering good campaigns...")
good_campaigns = data[
    (data['avg_ACOS_7d'] < 0.24) &
    (data['ACOS'] < 0.24) &
    (data['cost'] > data['Budget'] * 0.8)
]
print(f"Found {len(good_campaigns)} good campaigns.")

# 调整预算
def adjust_budget(budget):
    new_budget = min(budget * 1.2, 50)
    return new_budget

print("Adjusting budgets...")
good_campaigns['Budget'] = good_campaigns['Budget'].apply(adjust_budget)

# 增加原因列
good_campaigns['reason'] = '表现很好，预算提高1/5'

# 微信上需要的列
output_columns = [
    'date', 'campaignName', 'Budget', 'cost', 'clicks', 'ACOS',
    'avg_ACOS_7d', 'avg_ACOS_1m', 'clicks_1m', 'sales_1m', 'reason'
]
output_data = good_campaigns[output_columns]

# 添加日期信息
output_data['date'] = yesterday_str

# 将结果保存到CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\提问策略\手动_优质广告活动_IT_2024-06-06.csv'
output_data.to_csv(output_file_path, index=False)

print(f'Successfully saved the optimized campaigns to {output_file_path}')