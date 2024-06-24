# filename: process_ads.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\特殊关键词\预处理.csv'
df = pd.read_csv(file_path)

# 筛选满足 total_sales_15d 为0，且 total_clicks_7d 小于等于 12 的关键词
filtered_df = df[(df['total_sales_15d'] == 0) & (df['total_clicks_7d'] <= 12)]

# 提高关键词竞价0.02
filtered_df['New Bid'] = filtered_df['keywordBid'] + 0.02

# 添加竞价调整的原因
filtered_df['Reason'] = 'Increase bid by 0.02 due to low sales and clicks'

# 选择需要导出的列
export_columns = ['campaignName', 'adGroupName', 'total_sales_15d', 'total_clicks_7d', 
                  'keyword', 'matchType', 'keywordBid', 'keywordId', 'New Bid', 'Reason']

# 导出满足条件的关键词信息到新的CSV文件
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\特殊关键词\提问策略\手动_特殊关键词_v1_1_ES_2024-06-18.csv'
filtered_df.to_csv(output_path, columns=export_columns, index=False)

print("Process completed. Results saved to:", output_path)