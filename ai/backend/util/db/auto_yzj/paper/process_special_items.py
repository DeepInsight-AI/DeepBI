# filename: process_special_items.py

import pandas as pd

# Step 1: 读取数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\特殊商品投放\预处理.csv'
data = pd.read_csv(file_path)

# Step 2: 数据处理和条件过滤
# 按adGroupName分组并计算广告组的总销售额和总点击次数
grouped_data = data.groupby('adGroupName').agg(
    total_sales_15d=('total_sales_15d', 'sum'),
    total_clicks_7d=('total_clicks_7d', 'sum')
).reset_index()

# 过滤出符合条件的广告组名称：总销售额为0且总点击次数小于等于12
filtered_groups = grouped_data[(grouped_data['total_sales_15d'] == 0) & (
    grouped_data['total_clicks_7d'] <= 12)]['adGroupName']

# 根据过滤出的广告组名称筛选商品投放，并提高竞价0.02
filtered_data = data[data['adGroupName'].isin(filtered_groups)].copy()
filtered_data['new_keywordBid'] = filtered_data['keywordBid'] + 0.02
filtered_data['原因'] = '广告组最近15天总销售为0，且广告组里所有商品投放的最近7天总点击数小于等于12'

# Step 3: 输出结果到CSV
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\特殊商品投放\提问策略\手动_ASIN_特殊商品投放_v1_1_LAPASA_US_2024-07-09.csv'
filtered_data.to_csv(output_file_path, index=False, columns=[
    'campaignName', 'adGroupName', 'total_sales_15d', 'total_clicks_7d', 
    'keyword', 'matchType', 'keywordBid', 'keywordId', 'new_keywordBid', '原因'
])

print("Filtered data has been saved to:", output_file_path)