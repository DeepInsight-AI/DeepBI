# filename: gunala.py

import pandas as pd

# 定义文件路径
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\搜索词优化\预处理.csv'
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\搜索词优化\提问策略\手动_优质搜索词_ES_2024-06-07.csv'

# 读取CSV文件
df = pd.read_csv(file_path)

# 筛选出满足定义一的搜索词
filtered_df = df[(df['total_sales14d_7d'] > 0) & (df['ACOS_7d'] < 0.2)]

# 提取需要的列并添加原因
result_df = filtered_df[['campaignName', 'adGroupName', 'ACOS_7d', 'searchTerm', 'matchType']]
result_df['reason'] = '近七天有销售额，且ACOS值低于0.2'

# 保存结果到新的CSV文件
result_df.to_csv(output_path, index=False)

print("Result saved successfully to:", output_path)