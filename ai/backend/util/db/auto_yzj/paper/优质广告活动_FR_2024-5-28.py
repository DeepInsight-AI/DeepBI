# filename: 优质广告活动_FR_2024-5-28.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\预处理.csv'
df = pd.read_csv(file_path)

# 转换日期字段
df['date'] = pd.to_datetime(df['date'])

# 定义条件
target_date = pd.Timestamp('2024-05-27')
condition_1 = df['date'] == target_date
condition_2 = df['avg_ACOS_7d'] < 0.24
condition_3 = df['ACOS'] < 0.24
condition_4 = df['cost'] > (df['Budget'] * 0.80)

# 筛选符合条件的广告活动
filtered_df = df[condition_1 & condition_2 & condition_3 & condition_4].copy()

# 提高预算
def increase_budget(row):
    new_budget = row['Budget'] * 1.2
    return min(50, new_budget)

filtered_df['New_Budget'] = filtered_df.apply(increase_budget, axis=1)

# 添加增加预算的原因
def generate_reason(row):
    return (
        f"最近7天的平均ACOS值 {row['avg_ACOS_7d']:.2f}，"
        f"昨天的ACOS值 {row['ACOS']:.2f}，"
        f"昨天花费占预算 {row['cost'] / row['Budget']:.2%}"
    )

filtered_df['Reason_to_Increase_Budget'] = filtered_df.apply(generate_reason, axis=1)

# 选择需要输出的字段
output_columns = [
    'date', 'campaignName', 'Budget', 'cost', 'clicks', 'ACOS', 'avg_ACOS_7d',
    'avg_ACOS_1m', 'clicks_1m', 'sales_1m', 'Reason_to_Increase_Budget', 'New_Budget'
]

result_df = filtered_df[output_columns]

# 保存结果到CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\提问策略\优质广告活动_FR_2024-5-28.csv'
result_df.to_csv(output_file_path, index=False)

print("CSV 文件已保存到:", output_file_path)