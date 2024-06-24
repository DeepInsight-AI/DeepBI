# filename: 概念识别.py

import pandas as pd

# 读取CSV文件
file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\搜索词优化\预处理.csv"
df = pd.read_csv(file_path)

# 筛选出近七天有销售额的搜索词
filtered = df[(df['total_sales14d_7d'] > 0) & (df['ACOS_7d'] < 0.2)]

# 添加原因列
filtered['reason'] = '近七天有销售额, ACOS低于0.2'

# 选择所需的列
output_columns = ['campaignName', 'adGroupName', 'ACOS_7d', 'searchTerm', 'matchType', 'reason']
output_df = filtered[output_columns]

# 指定输出路径
output_file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\搜索词优化\提问策略\手动_优质搜索词_ES_2024-06-05.csv"

# 保存为新的CSV文件
output_df.to_csv(output_file_path, index=False, encoding='utf-8-sig')

print("CSV文件已生成并保存在：" + output_file_path)