# filename: 检查输出内容_进一步验证.py

import pandas as pd

# 读取并检查CSV文件内容
output_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_劣质商品投放_v1_1_LAPASA_DE_2024-07-03.csv'
output_data = pd.read_csv(output_file)

# 显示与操作相关的列
columns_to_check = [
    'keywordId', 'keyword', 'campaignName', 'adGroupName', 'matchType', 'keywordBid', 
    'new_keywordBid', 'targeting', 'total_cost_7d', 'total_sales14d_7d', 'total_cost_30d', 
    'total_sales14d_30d', 'ACOS_7d', 'ACOS_30d', 'total_clicks_7d', 'total_clicks_30d', 
    'action_reason'
]
print("输出文件的操作相关的列内容:")
print(output_data[columns_to_check].head())