# filename: update_keywords.py

import pandas as pd

# 定义文件路径
input_file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\特殊关键词\预处理.csv"
output_file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\特殊关键词\提问策略\手动_特殊关键词_ES_2024-06-07.csv"

# 读取csv文件
df = pd.read_csv(input_file_path)

# 查找最近15天的总销售额为0，并且广告组里的所有关键词的最近7天的总点击次数小于等于12的广告组
grouped = df.groupby('adGroupName').filter(lambda x: (x['total_sales_15d'] == 0).all() and (x['total_clicks_7d'] <= 12).all())

# 增加竞价0.02
grouped['keywordBid'] += 0.02

# 添加操作竞价原因
grouped['adjustment_reason'] = 'Increased bid by 0.02 due to low performance'

# 选择所需的列
output_df = grouped[['campaignName', 'adGroupName', 'total_sales_15d', 'total_clicks_7d', 'keyword', 'matchType', 'keywordBid', 'keywordId', 'adjustment_reason']]

# 将结果输出到新的csv文件
output_df.to_csv(output_file_path, index=False)

print(f"处理完成，结果已保存到 {output_file_path}")