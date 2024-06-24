# filename: find_ad_campaigns.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\搜索词优化\预处理.csv'
df = pd.read_csv(file_path)

# 筛选满足条件的数据
filtered_df = df[(df['total_sales14d_7d'] > 0) & (df['ACOS_7d'] < 0.2)]

# 添加原因列
filtered_df['reason'] = '近七天有销售额且ACOS值低于0.2'

# 选择所需的列
result_df = filtered_df[['campaignName', 'adGroupName', 'ACOS_7d', 'searchTerm', 'reason']]
result_df.columns = ['Campaign Name', 'adGroupName', 'week_acos', 'searchTerm', 'reason']

# 保存结果到新的CSV文件
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\搜索词优化\提问策略\自动_优质搜索词_IT_2024-06-05.csv'
result_df.to_csv(output_path, index=False, encoding='utf-8-sig')

print(f"结果已保存到 {output_path}")