# filename: process_auto_group_keywords.py

import pandas as pd

# 定义文件路径
input_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\特殊自动定位组\预处理.csv'
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\特殊自动定位组\提问策略\自动_特殊自动定位组_v1_11_ES_2024-06-20.csv'

# 读取CSV数据
df = pd.read_csv(input_file_path)

# 过滤满足条件的广告组
filtered_df = df[(df['total_sales_15d'] == 0) & (df['total_clicks_7d'] <= 12)]

# 提高竞价
filtered_df['New_keywordBid'] = filtered_df['keywordBid'] + 0.02
filtered_df['原因'] = '广告组最近15天的总销售额为0，且总点击次数小于等于12'

# 选择需要导出的列
output_columns = ['campaignName', 'adGroupName', 'total_sales_15d', 'total_clicks_7d', 'keyword', 'keywordBid', 'keywordId', 'New_keywordBid', '原因']
output_df = filtered_df[output_columns]

# 保存结果到CSV
output_df.to_csv(output_file_path, index=False)

print(f"过滤结果已保存到 {output_file_path}")