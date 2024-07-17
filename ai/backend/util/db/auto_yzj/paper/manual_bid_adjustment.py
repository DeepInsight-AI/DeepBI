# filename: manual_bid_adjustment.py

import pandas as pd

# Step 1: 读取数据
csv_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\特殊商品投放\预处理.csv'
data = pd.read_csv(csv_path)

# Step 2: 筛选广告组
ad_group_sales_0 = data.groupby('adGroupName')['total_sales_15d'].sum()
ad_groups_to_adjust = ad_group_sales_0[ad_group_sales_0 == 0].index

# Step 3: 筛选商品投放
filtered_data = data[data['adGroupName'].isin(ad_groups_to_adjust)]
ad_group_clicks = filtered_data.groupby('adGroupName')['total_clicks_7d'].sum()
ad_groups_final = ad_group_clicks[ad_group_clicks <= 12].index

final_data = filtered_data[filtered_data['adGroupName'].isin(ad_groups_final)]

# Step 4: 调整竞价
final_data['new_keywordBid'] = final_data['keywordBid'] + 0.02
final_data['adjustment_reason'] = '广告组的最近15天的总销售额为0，并且广告组里的所有商品投放的最近7天的总点击次数<=12'

# Step 5: 输出结果
output_columns = ['campaignName', 'adGroupName', 'total_sales_15d', 'total_clicks_7d', 'keyword', 'matchType', 'keywordBid', 'keywordId', 'new_keywordBid', 'adjustment_reason']
output_data = final_data[output_columns]

output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\特殊商品投放\提问策略\手动_ASIN_特殊商品投放_v1_1_LAPASA_US_2024-07-02.csv'
output_data.to_csv(output_path, index=False)

print(f"调整后的商品投放信息已保存到: {output_path}")