# filename: 日常优化_手动sp广告_特殊关键词_预处理_任务.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\特殊关键词\预处理.csv'
data = pd.read_csv(file_path)

# 筛选满足条件的广告组及关键词
filtered_data = data[(data['total_sales_15d'] == 0) & (data['total_clicks_7d'] <= 12)].copy()

# 调整关键词竞价
filtered_data['New_Bid'] = filtered_data['keywordBid'] + 0.02

# 添加操作原因字段
filtered_data['Reason'] = '广告组的最近15天的总销售额为0，并且广告组里的所有关键词的最近7天的总点击次数小于等于12'

# 输出到新的CSV文件
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\特殊关键词\提问策略\手动_特殊关键词_v1_1_IT_2024-06-17.csv'
filtered_data.to_csv(output_path, index=False, encoding='utf-8-sig')

print(f"Filtered data has been saved to {output_path}")