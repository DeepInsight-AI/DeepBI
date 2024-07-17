# filename: manual_special_product_placement.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\特殊商品投放\预处理.csv'
data = pd.read_csv(file_path)

# 筛选广告组最近15天总销售额为0
zero_sales_groups = data[data['total_sales_15d'] == 0]

# 对于这些广告组，筛选出所有商品投放最近7天总点击次数小于等于12的广告组
grouped = zero_sales_groups.groupby('adGroupName')
ad_groups_to_increase_bid = []

for name, group in grouped:
    if (group['total_clicks_7d'] <= 12).all():
        ad_groups_to_increase_bid.append(group)

# 合并符合条件的数据
result_df = pd.concat(ad_groups_to_increase_bid)

# 提高竞价0.02，并添加新的列
result_df['New Bid'] = result_df['keywordBid'] + 0.02
result_df['操作竞价原因'] = '广告组最近15天总销售额为0且最近7天总点击次数小于等于12'

# 保存结果到新的CSV文件
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\特殊商品投放\提问策略\手动_特殊商品投放_v1_1_IT_2024-06-26.csv'
selected_columns = ['campaignName', 'adGroupName', 'total_sales_15d', 'total_clicks_7d', 'keyword', 'matchType', 'keywordBid', 'keywordId', 'New Bid', '操作竞价原因']
result_df[selected_columns].to_csv(output_path, index=False)

print(f"结果已保存到 {output_path}")