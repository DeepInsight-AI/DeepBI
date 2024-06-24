# filename: find_extended_bad_campaigns.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\预算优化\预处理.csv'
df = pd.read_csv(file_path)

# 定义新条件
condition_sales_0_clicks_30 = (df['sales_1m'] == 0) & (df['clicks_1m'] >= 30)
condition_sales_100_clicks_70 = (df['sales_1m'] < 100) & (df['clicks_1m'] >= 70)

# 筛选满足新条件的广告活动
bad_campaigns_sales_0_clicks_30 = df[condition_sales_0_clicks_30]
bad_campaigns_sales_100_clicks_70 = df[condition_sales_100_clicks_70]

# 增加关闭原因列
bad_campaigns_sales_0_clicks_30['关闭原因'] = '最近一个月的总销售为0 且 最近一个月的总点击次数大于等于30'
bad_campaigns_sales_100_clicks_70['关闭原因'] = '最近一个月的总销售小于100 且 最近一个月的总点击次数大于等于70'

# 合并两个筛选结果
result = pd.concat([bad_campaigns_sales_0_clicks_30, bad_campaigns_sales_100_clicks_70])

# 去重
result = result.drop_duplicates()

# 保留需要的列
result = result[[
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
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\预算优化\提问策略\扩展_关闭的广告活动_IT_2024-06-11.csv'
result.to_csv(output_path, index=False, encoding='utf-8-sig')

# Output the resulting dataframe length for verification
print("符合新条件的广告活动数量:", len(result))
print(result)
print("已成功识别潜在劣质广告活动，并将结果保存至:", output_path)