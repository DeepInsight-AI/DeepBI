# filename: identify_and_adjust_budget.py

import pandas as pd
import datetime

# 读取数据集
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\预处理.csv'
df = pd.read_csv(file_path)

# 设置今天的日期，并定义昨天的日期
today = datetime.date(2024, 5, 28)
yesterday = today - datetime.timedelta(days=1)

# 初始化结果列表
result = []

# 定义预算调整函数
def adjust_budget(budget, decrease_amount, min_budget):
    new_budget = budget - decrease_amount
    if new_budget < min_budget:
        new_budget = min_budget
    return new_budget

# 判断条件并调整预算
for index, row in df.iterrows():
    budget = row['Budget']
    reason = None
    new_budget = budget

    # 定义一条件
    if row['avg_ACOS_7d'] > 0.24 and row['ACOS'] > 0.24 and row['clicks'] >= 10 and row['avg_ACOS_1m'] > row['country_avg_ACOS_1m']:
        reason = '定义一'
        new_budget = adjust_budget(budget, 5, 8)

    # 定义二条件
    elif row['avg_ACOS_7d'] > 0.24 and row['ACOS'] > 0.24 and row['cost'] > 0.8 * row['Budget'] and row['avg_ACOS_1m'] > row['country_avg_ACOS_1m']:
        reason = '定义二'
        new_budget = adjust_budget(budget, 5, 8)

    # 定义三条件
    elif row['avg_ACOS_1m'] > 0.24 and row['avg_ACOS_1m'] > row['country_avg_ACOS_1m'] and row['clicks_7d'] >= 15:
        reason = '定义三'
        new_budget = adjust_budget(budget, 5, 5)

    if reason:
        result.append({
            'date': yesterday,
            'campaignName': row['campaignName'],
            'Budget': budget,
            'clicks': row['clicks'],
            'ACOS': row['ACOS'],
            'avg_ACOS_7d': row['avg_ACOS_7d'],
            'clicks_7d': row['clicks_7d'],
            'sales_7d': row.get('sales_7d', 0),   # 使用默认值0
            'avg_ACOS_1m': row['avg_ACOS_1m'],
            'clicks_1m': row['clicks_1m'],
            'sales_1m': row['sales_1m'],
            'country_avg_ACOS_1m': row['country_avg_ACOS_1m'],
            'new_budget': new_budget,
            'reason': reason
        })

# 输出结果到CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\提问策略\手动_劣质广告活动_ES_2024-06-07.csv'
output_df = pd.DataFrame(result)
output_df.to_csv(output_file_path, index=False)

print("Results have been saved to:", output_file_path)