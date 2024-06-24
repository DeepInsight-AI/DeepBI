# filename: create_filtered_csv.py

import pandas as pd

# 加载数据
data_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\搜索词优化\预处理.csv'
df = pd.read_csv(data_path)

# 筛选条件
condition_sales = df['total_sales14d_7d'] > 0
condition_acos = df['ACOS_7d'] < 0.2

# 筛选数据
filtered_df = df[condition_sales & condition_acos].copy()

# 添加原因
filtered_df['reason'] = '近七天有销售额且该搜索词的近七天acos值在0.2以下'

# 选择需要的列并重命名
result_df = filtered_df[['campaignName', 'adGroupName', 'ACOS_7d', 'searchTerm', 'reason']]
result_df.columns = ['Campaign Name', 'adGroup', 'week_acos', 'searchTerm', 'reason']

# 保存结果
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\搜索词优化\提问策略\自动_优质搜索词_ES_2024-06-10.csv'
result_df.to_csv(output_path, index=False)

print("CSV file has been created and saved to:", output_path)