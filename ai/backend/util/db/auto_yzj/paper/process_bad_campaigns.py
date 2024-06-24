# filename: process_bad_campaigns.py

import pandas as pd

# 定义文件路径
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\预处理.csv'
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\提问策略\手动_劣质广告活动_FR_2024-05-28.csv'

# 读取CSV文件
df = pd.read_csv(file_path)

# 日期条件
yesterday = '2024-05-27'

# 条件一
condition1 = (df['avg_ACOS_7d'] > 0.24) & (df['ACOS'] > 0.24) & (df['clicks'] >= 10) & (df['avg_ACOS_1m'] > df['country_avg_ACOS_1m'])

# 条件二
condition2 = (df['avg_ACOS_7d'] > 0.24) & (df['ACOS'] > 0.24) & (df['cost'] > 0.8 * df['Budget']) & (df['avg_ACOS_1m'] > df['country_avg_ACOS_1m'])

# 条件三
condition3 = (df['avg_ACOS_1m'] > 0.24) & (df['avg_ACOS_1m'] > df['country_avg_ACOS_1m']) & (df['sales'] == 0) & (df['clicks_7d'] >= 15)

# 选出劣质广告活动
bad_campaigns = df[condition1 | condition2 | condition3].copy()

# 计算新的预算
def calculate_new_budget(row):
    new_budget = row['Budget']
    if condition1.loc[row.name] or condition2.loc[row.name]:
        new_budget = max(new_budget - 5, 8)
    if condition3.loc[row.name]:
        new_budget = max(new_budget - 5, 5)
    return new_budget

# 添加新的预算和原因
bad_campaigns['new_budget'] = bad_campaigns.apply(calculate_new_budget, axis=1)
bad_campaigns['reason'] = ''
bad_campaigns.loc[condition1[condition1].index, 'reason'] = '定义一'
bad_campaigns.loc[condition2[condition2].index, 'reason'] = '定义二'
bad_campaigns.loc[condition3[condition3].index, 'reason'] = '定义三'

# 选择要输出的列
output_columns = ['date', 'campaignName', 'Budget', 'clicks', 'ACOS', 'avg_ACOS_7d', 
                  'clicks_7d', 'sales', 'avg_ACOS_1m', 'clicks_1m', 'sales_1m', 
                  'country_avg_ACOS_1m', 'new_budget', 'reason']
output_data = bad_campaigns[output_columns]

# 保存到新的CSV文件
output_data.to_csv(output_path, index=False)

print(f"文件已保存到 {output_path}")