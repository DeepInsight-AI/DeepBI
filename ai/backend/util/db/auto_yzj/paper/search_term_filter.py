# filename: search_term_filter.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放搜索词优化\预处理.csv'
df = pd.read_csv(file_path)

# 初始化 reason 列
df['reason'] = ''

# 定义筛选条件
condition1 = (df['ACOS_30d'] > 0.24) & (df['ACOS_30d'] < 0.36) & (df['ORDER_1m'] <= 5)
condition2 = (df['ACOS_30d'] >= 0.36) & (df['ORDER_1m'] <= 8)
condition3 = (df['total_clicks_30d'] > 13) & (df['ORDER_1m'] == 0)
condition4 = (df['ACOS_7d'] > 0.24) & (df['ACOS_7d'] < 0.36) & (df['ORDER_7d'] <= 3)
condition5 = (df['ACOS_7d'] >= 0.36) & (df['ORDER_7d'] <= 5)
condition6 = (df['total_clicks_7d'] > 10) & (df['ORDER_7d'] == 0)

# 标注满足条件的原因
df.loc[condition1, 'reason'] += '定义一 '
df.loc[condition2, 'reason'] += '定义二 '
df.loc[condition3, 'reason'] += '定义三 '
df.loc[condition4, 'reason'] += '定义四 '
df.loc[condition5, 'reason'] += '定义五 '
df.loc[condition6, 'reason'] += '定义六 '

# 只保留满足条件的行
filtered_df = df[df['reason'] != ''].copy()

# 去掉reason中的多余空格
filtered_df['reason'] = filtered_df['reason'].str.strip()

# 选取需要的列
filtered_df = filtered_df[['campaignName', 'adGroupName', 'total_clicks_7d', 'ACOS_7d', 'ORDER_7d', 'total_clicks_30d', 'ORDER_1m', 'ACOS_30d', 'searchTerm', 'reason']]

# 保存结果到新的CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放搜索词优化\提问策略\手动_劣质_ASIN_搜索词_v1_1_DELOMO_FR_2024-07-09.csv'
filtered_df.to_csv(output_file_path, index=False)

print(f"筛选结果已保存到 {output_file_path}")