# filename: deepbi_analysis.py
import pandas as pd

# 定义数据集路径
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放搜索词优化\预处理.csv'
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放搜索词优化\提问策略\手动_劣质_ASIN_搜索词_v1_1_DELOMO_DE_2024-07-09.csv'

# 加载数据
df = pd.read_csv(file_path)

# 创建条件
condition1 = (df['ACOS_30d'] > 0.24) & (df['ACOS_30d'] < 0.36) & (df['ORDER_1m'] <= 5)
condition2 = (df['ACOS_30d'] >= 0.36) & (df['ORDER_1m'] <= 8)
condition3 = (df['total_clicks_30d'] > 13) & (df['ORDER_1m'] == 0)
condition4 = (df['ACOS_7d'] > 0.24) & (df['ACOS_7d'] < 0.36) & (df['ORDER_7d'] <= 3)
condition5 = (df['ACOS_7d'] >= 0.36) & (df['ORDER_7d'] <= 5)
condition6 = (df['total_clicks_7d'] > 10) & (df['ORDER_7d'] == 0)

# 应用条件，加入reason列
df_filtered = df[condition1 | condition2 | condition3 | condition4 | condition5 | condition6].copy()
df_filtered['reason'] = ''

df_filtered.loc[condition1, 'reason'] += '定义一;'
df_filtered.loc[condition2, 'reason'] += '定义二;'
df_filtered.loc[condition3, 'reason'] += '定义三;'
df_filtered.loc[condition4, 'reason'] += '定义四;'
df_filtered.loc[condition5, 'reason'] += '定义五;'
df_filtered.loc[condition6, 'reason'] += '定义六;'

# 准备输出的列
output_cols = [
    'campaignName', 'adGroupName', 'total_clicks_7d', 'ACOS_7d', 'ORDER_7d',
    'total_clicks_30d', 'ORDER_1m', 'ACOS_30d', 'searchTerm', 'reason'
]

# 选择这些列
df_output = df_filtered[output_cols]

# 保存结果
df_output.to_csv(output_path, index=False)

print(f"数据处理完成，结果已保存到 {output_path}")