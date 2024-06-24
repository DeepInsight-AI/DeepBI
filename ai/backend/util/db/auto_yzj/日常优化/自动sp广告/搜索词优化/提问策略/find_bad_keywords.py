# filename: C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\搜索词优化\提问策略\find_bad_keywords.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\搜索词优化\预处理.csv'
data = pd.read_csv(file_path)

# 添加订单数字段
data['orders_30d'] = data['total_sales14d_30d'] / (data['total_cost_30d'] + 1e-9) * data['ACOS_30d']
data['orders_7d'] = data['total_sales14d_7d'] / (data['total_cost_7d'] + 1e-9) * data['ACOS_7d']

# 筛选符合条件的搜索词
def filter_data(row):
    reasons = []
    if 0.24 < row['ACOS_30d'] < 0.36 and row['orders_30d'] <= 5:
        reasons.append('定义一')
    if row['ACOS_30d'] >= 0.36 and row['orders_30d'] <= 8:
        reasons.append('定义二')
    if row['total_clicks_30d'] > 13 and row['orders_30d'] == 0:
        reasons.append('定义三')
    if 0.24 < row['ACOS_7d'] < 0.36 and row['orders_7d'] <= 3:
        reasons.append('定义四')
    if row['ACOS_7d'] >= 0.36 and row['orders_7d'] <= 5:
        reasons.append('定义五')
    if row['total_clicks_7d'] > 10 and row['orders_7d'] == 0:
        reasons.append('定义六')
    
    return ','.join(reasons) if reasons else None

data['reason'] = data.apply(filter_data, axis=1)
filtered_data = data[data['reason'].notna()]

# 选择并重新命名需要的字段
filtered_data = filtered_data[[
    'campaignName', 'campaignId', 'adGroupName', 'adGroupId', 
    'total_clicks_7d', 'ACOS_7d', 'orders_7d', 'total_clicks_30d', 
    'orders_30d', 'ACOS_30d', 'searchTerm', 'reason'
]].rename(columns={
    'total_clicks_7d': 'week_clicks',
    'ACOS_7d': 'week_acos',
    'orders_7d': 'week_orders',
    'total_clicks_30d': 'sum_clicks',
    'orders_30d': 'month_orders',
    'ACOS_30d': 'month_acos'
})

# 保存结果到目标路径
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\搜索词优化\提问策略\自动_劣质搜索词_v1_1_IT_2024-06-17.csv'
filtered_data.to_csv(output_path, index=False, encoding='utf-8-sig')

print("筛选结果已保存到: ", output_path)