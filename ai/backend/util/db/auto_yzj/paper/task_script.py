# filename: task_script.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\特殊商品投放\预处理.csv'
data = pd.read_csv(file_path)

# 筛选符合条件的商品投放
filtered_data = data.groupby('adGroupName').filter(
    lambda x: x['total_sales_15d'].sum() == 0 and all(x['total_clicks_7d'] <= 12)
)

# 调整竞价
filtered_data['new_keywordBid'] = filtered_data['keywordBid'] + 0.02
filtered_data['reason'] = '广告组的最近15天的总销售额为0并且广告组里的所有商品投放的最近7天的总点击次数小于等于12'

# 选择需要保留的列
final_data = filtered_data[['campaignName', 'adGroupName', 'total_sales_15d', 'total_clicks_7d', 'keyword', 'matchType', 'keywordBid', 'keywordId', 'new_keywordBid', 'reason']]

# 保存结果到新CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\特殊商品投放\提问策略\手动_ASIN_特殊商品投放_v1_1_LAPASA_US_2024-07-04.csv'
final_data.to_csv(output_file_path, index=False)

print("任务完成，结果已保存到:", output_file_path)