# filename: 优化广告活动预算.py

import pandas as pd

# 读取 CSV 文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\预处理.csv'
df = pd.read_csv(file_path)

# 假设今天是2024年5月28日，那么昨天是2024年5月27日
yesterday = '2024-05-27'

# 条件筛选函数
def filter_and_adjust_budget(row):
    reason = None
    new_budget = row['Budget']
    
    avg_ACOS_7d = row['avg_ACOS_7d']
    yesterday_ACOS = row['ACOS']
    clicks_7d = row['clicks_7d']
    avg_ACOS_1m = row['avg_ACOS_1m']
    country_avg_ACOS_1m = row['country_avg_ACOS_1m']
    clicks = row['clicks']
    cost = row['cost']
    Budget = row['Budget']
    clicks_1m = row['clicks_1m']
    sales_7d = row['sales_7d'] if 'sales_7d' in row else 0
    
    if (avg_ACOS_7d > 0.24 and yesterday_ACOS > 0.24 and clicks >= 10 and
        avg_ACOS_1m > country_avg_ACOS_1m):
        reason = "定义一"
        new_budget = max(Budget - 5, 8)

    elif (avg_ACOS_7d > 0.24 and yesterday_ACOS > 0.24 and cost > (0.8 * Budget) and
          avg_ACOS_1m > country_avg_ACOS_1m):
        reason = "定义二"
        new_budget = max(Budget - 5, 8)

    elif (avg_ACOS_1m > 0.24 and avg_ACOS_1m > country_avg_ACOS_1m and
          sales_7d == 0 and clicks_7d >= 15):
        reason = "定义三"
        new_budget = max(Budget - 5, 5)

    return new_budget, reason

# 筛选符合条件的广告活动
df['new_budget'], df['reason'] = zip(*df.apply(filter_and_adjust_budget, axis=1))

# 筛选出满足任意定义的广告活动
filtered_df = df[df['reason'].notna()]

# 选择需要保存的列
output_columns = [
    'date', 'campaignName', 'Budget', 'clicks', 'ACOS', 'avg_ACOS_7d', 
    'clicks_7d', 'sales_1m', 'avg_ACOS_1m', 'clicks_1m', 'sales_1m', 'country_avg_ACOS_1m', 'new_budget', 'reason'
]
filtered_df = filtered_df[output_columns]

# 保存结果到指定的CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\提问策略\劣质广告活动_FR.csv'
filtered_df.to_csv(output_file_path, index=False)

print(f"结果已保存到 {output_file_path}")