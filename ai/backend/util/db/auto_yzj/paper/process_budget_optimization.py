# filename: process_budget_optimization.py
import pandas as pd
from datetime import datetime, timedelta

# 读取csv文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\预算优化\预处理.csv'
df = pd.read_csv(file_path)

# 假设今天的日期是2024年5月28日
today = datetime(2024, 5, 28)
yesterday = today - timedelta(days=1)
yesterday_str = yesterday.strftime('%Y-%m-%d')

# 定义结果列表
results = []

# 遍历数据，筛选符合定义一、定义二、定义三的广告活动
for index, row in df.iterrows():
    campaignName = row['campaignName']
    Budget = row['Budget']
    clicks = row['clicks']
    ACOS = row['ACOS']
    avg_ACOS_7d = row['avg_ACOS_7d']
    avg_ACOS_1m = row['avg_ACOS_1m']
    clicks_7d = row['clicks_7d']
    sales_7d = row['sales_1m']
    clicks_1m = row['clicks_1m']
    sales_1m = row['sales_1m']
    country_avg_ACOS_1m = row['country_avg_ACOS_1m']
    date = row['date']

    reason = []
    new_budget = Budget

    # 定义一
    if avg_ACOS_7d > 0.24 and ACOS > 0.24 and clicks >= 10 and avg_ACOS_1m > country_avg_ACOS_1m and date == yesterday_str:
        reason.append('定义一')
        new_budget = max(8, Budget - 5)
        
    # 定义二
    if avg_ACOS_7d > 0.24 and ACOS > 0.24 and row['cost'] > 0.8 * Budget and avg_ACOS_1m > country_avg_ACOS_1m and date == yesterday_str:
        reason.append('定义二')
        new_budget = max(8, Budget - 5)

    # 定义三
    if avg_ACOS_1m > 0.24 and avg_ACOS_1m > country_avg_ACOS_1m and sales_7d == 0 and clicks_7d >= 15:
        reason.append('定义三')
        new_budget = max(5, Budget - 5)
        
    if reason:
        results.append({
            'date': date,
            'campaignName': campaignName,
            'Budget': Budget,
            'clicks': clicks,
            'ACOS': ACOS,
            'avg_ACOS_7d': avg_ACOS_7d,
            'clicks_7d': clicks_7d,
            'sales_7d': sales_7d,
            'avg_ACOS_1m': avg_ACOS_1m,
            'clicks_1m': clicks_1m,
            'sales_1m': sales_1m,
            'country_avg_ACOS_1m': country_avg_ACOS_1m,
            'new_budget': new_budget,
            'reason': ', '.join(reason)
        })

# 结果保存到CSV
output_df = pd.DataFrame(results)
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\预算优化\提问策略\自动_劣质广告活动_ES_2024-06-07.csv'
output_df.to_csv(output_file_path, index=False)

print("结果已保存到: " + output_file_path)