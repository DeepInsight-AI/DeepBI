# filename: analyze_conditions_separately.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\预算优化\预处理.csv'
df = pd.read_csv(file_path)

# 定义个别条件
condition_sales_0 = (df['sales_1m'] == 0)
condition_clicks_75 = (df['clicks_1m'] >= 70)

# 筛选满足单个条件的广告活动
bad_campaigns_sales_0 = df[condition_sales_0]
bad_campaigns_clicks_75 = df[condition_clicks_75]

# 打印满足单个条件的广告活动的数量和详情
print("\n满足最近一个月总销售为0条件的广告活动数量:", len(bad_campaigns_sales_0))
print(bad_campaigns_sales_0[[
    'campaignName', 
    'Budget', 
    'clicks', 
    'ACOS', 
    'avg_ACOS_7d', 
    'avg_ACOS_1m', 
    'clicks_1m', 
    'sales_1m'
]])

print("\n满足最近一个月总点击次数大于等于70条件的广告活动数量:", len(bad_campaigns_clicks_75))
print(bad_campaigns_clicks_75[[
    'campaignName', 
    'Budget', 
    'clicks', 
    'ACOS', 
    'avg_ACOS_7d', 
    'avg_ACOS_1m', 
    'clicks_1m', 
    'sales_1m'
]])