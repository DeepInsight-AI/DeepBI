# filename: gunala_search_terms_analysis.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\搜索词优化\预处理.csv'

# 读取数据
df = pd.read_csv(file_path)

# 筛选符合定义一条件的数据
filtered_df = df[(df['total_sales14d_7d'] > 0) & (df['ACOS_7d'] < 0.2)]

# 添加reason列
filtered_df['reason'] = 'Search term has sales in the last 7 days and ACOS is below 0.2'

# 选择需要的字段
result_df = filtered_df[['campaignName', 'adGroupName', 'ACOS_7d', 'searchTerm', 'matchType', 'reason']]

# 重命名列
result_df.columns = ['Campaign Name', 'adGroupName', 'week_acos', 'searchTerm', 'matchType', 'reason']

# 保存结果到新的CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\搜索词优化\提问策略\手动_优质搜索词_IT_2024-06-11.csv'
result_df.to_csv(output_file_path, index=False)

print(f'Results saved to {output_file_path}')