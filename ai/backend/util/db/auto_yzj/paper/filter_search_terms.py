# filename: filter_search_terms.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放搜索词优化\预处理.csv'
df = pd.read_csv(file_path)

# 定义筛选条件
condition_1 = (df['total_clicks_30d'] > 13) & (df['total_cost_30d'] > 7) & (df['ORDER_1m'] == 0)
condition_2 = (df['total_clicks_7d'] > 10) & (df['ORDER_7d'] == 0) & (df['total_cost_7d'] > 5)

# 筛选符合条件的数据
filtered_df = df[condition_1 | condition_2].copy()

# 添加原因列
filtered_df['reason'] = ''
filtered_df.loc[condition_1, 'reason'] = '定义一'
filtered_df.loc[condition_2, 'reason'] = '定义二'

# 筛选所需列
required_columns = [
    'campaignName', 'adGroupName',  'ORDER_1m','total_clicks_7d', 'ACOS_7d', 'ORDER_7d', 'total_cost_7d',
    'total_clicks_30d', 'total_cost_30d', 'ACOS_30d', 'searchTerm', 'reason'
]
result_df = filtered_df[required_columns]

# 输出结果到CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放搜索词优化\提问策略\手动_劣质_ASIN_搜索词_v1_1_LAPASA_UK_2024-07-15.csv'
result_df.to_csv(output_file_path, index=False)

print(f"结果已保存到 {output_file_path}")
