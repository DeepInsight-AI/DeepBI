# filename: search_term_extraction.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\搜索词优化\预处理.csv'
df = pd.read_csv(file_path)

# 筛选符合条件的数据
filtered_df = df[(df['total_sales14d_7d'] > 0) & (df['ACOS_7d'] < 0.2)]

# 添加原因列
filtered_df['reason'] = '近七天有销售额且ACOS值在0.2以下'

# 选择需要的列
result_df = filtered_df[['campaignName', 'adGroupName', 'ACOS_7d', 'searchTerm', 'matchType', 'reason']]

# 重命名列
result_df.columns = ['Campaign Name', 'adGroupName', 'week_acos', 'searchTerm', 'matchtype', 'reason']

# 指定保存路径
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\搜索词优化\提问策略\手动_优质搜索词_IT_2024-06-08.csv'

# 保存结果到CSV文件
result_df.to_csv(output_path, index=False)

print(f"Results saved to {output_path}")