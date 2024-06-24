# filename: close_bad_campaigns.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\预算优化\预处理.csv'
df = pd.read_csv(file_path)

# 定义过滤条件
condition = (df['sales_1m'] == 0) & (df['clicks_1m'] >= 75)

# 筛选满足条件的广告活动
bad_campaigns = df[condition]

# 增加关闭原因列
bad_campaigns['关闭原因'] = '最近一个月的总销售为0 且 最近一个月的总点击次数大于等于75'

# 保留需要的列
result = bad_campaigns[[
    'campaignName', 
    'Budget', 
    'clicks', 
    'ACOS', 
    'avg_ACOS_7d', 
    'avg_ACOS_1m', 
    'clicks_1m', 
    'sales_1m',
    '关闭原因'
]]

# 保存结果到新的CSV文件
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\预算优化\提问策略\自动_关闭的广告活动_IT_2024-06-11.csv'
result.to_csv(output_path, index=False, encoding='utf-8-sig')

print(result)
print("已成功关闭劣质广告活动，并将结果保存至:", output_path)