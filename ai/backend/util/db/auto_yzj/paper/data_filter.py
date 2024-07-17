# filename: data_filter.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放搜索词优化\预处理.csv'
df = pd.read_csv(file_path)

# 筛选满足定义一的数据
condition1 = (df['total_clicks_30d'] > 13) & (df['total_cost_30d'] > 7) & (df['ORDER_1m'] == 0)

# 筛选满足定义二的数据
condition2 = (df['total_clicks_7d'] > 10) & (df['ORDER_7d'] == 0) & (df['total_cost_7d'] > 5)

# 合并满足条件一和条件二的数据
filtered_df = df[condition1 | condition2].copy()

# 添加满足定义的原因列
filtered_df['reason'] = '定义一'
filtered_df.loc[condition2, 'reason'] = '定义二'

# 选择需要输出的列
result_df = filtered_df[['campaignName', 'adGroupName', 'total_clicks_7d', 'ACOS_7d', 'ORDER_7d', 'total_cost_7d', 
                         'total_clicks_30d', 'total_cost_30d', 'ORDER_1m', 'ACOS_30d', 'searchTerm', 'reason']]

# 定义输出文件路径
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放搜索词优化\提问策略\手动_劣质_ASIN_搜索词_v1_1_LAPASA_UK_2024-07-14.csv'

# 将结果保存到CSV文件
result_df.to_csv(output_path, index=False)

print("任务完成，筛选结果已保存到指定文件中。")