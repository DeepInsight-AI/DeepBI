# filename: filter_and_adjust_bids.py

import pandas as pd

# 1. 读取数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\特殊商品投放\预处理.csv'
data = pd.read_csv(file_path)

# 2. 过滤数据
# 找到广告组的广告组的最近15天的总销售额为0的广告组
ad_group_sales_zero = data[data['total_sales_15d'] == 0]['adGroupName'].unique()

# 从这些广告组中，找出最近7天的总点击次数小于等于12的商品投放
filtered_data = data[(data['adGroupName'].isin(ad_group_sales_zero)) & (data['total_clicks_7d'] <= 12)]

# 3. 调整竞价
filtered_data['new_keywordBid'] = filtered_data['keywordBid'] + 0.02

# 添加竞价原因
filtered_data['adjustment_reason'] = '广告组的最近15天的总销售额为0且总点击次数小于等于12'

# 4. 输出结果到新的CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\特殊商品投放\提问策略\手动_ASIN_特殊商品投放_v1_1_OutdoorMaster_FR_2024-07-09.csv'
filtered_data.to_csv(output_file_path, index=False)
print(f"Results have been saved to {output_file_path}")