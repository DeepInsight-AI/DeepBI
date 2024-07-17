# filename: generate_search_words_report.py
import pandas as pd

# 读取csv数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放搜索词优化\预处理.csv'
df = pd.read_csv(file_path)

# 定义筛选条件
condition_1 = (df['ACOS_30d'] > 0.24) & (df['ACOS_30d'] < 0.36) & (df['ORDER_1m'] <= 5)
condition_2 = (df['ACOS_30d'] >= 0.36) & (df['ORDER_1m'] <= 8)
condition_3 = (df['total_clicks_30d'] > 13) & (df['ORDER_1m'] == 0)
condition_4 = (df['ACOS_7d'] > 0.24) & (df['ACOS_7d'] < 0.36) & (df['ORDER_7d'] <= 3)
condition_5 = (df['ACOS_7d'] >= 0.36) & (df['ORDER_7d'] <= 5)
condition_6 = (df['total_clicks_7d'] > 10) & (df['ORDER_7d'] == 0)

# 提取符合条件的行
filtered_df = df[condition_1 | condition_2 | condition_3 | condition_4 | condition_5 | condition_6].copy()

# 添加原因列
filtered_df['reason'] = ''
filtered_df.loc[condition_1, 'reason'] += '定义一, '
filtered_df.loc[condition_2, 'reason'] += '定义二, '
filtered_df.loc[condition_3, 'reason'] += '定义三, '
filtered_df.loc[condition_4, 'reason'] += '定义四, '
filtered_df.loc[condition_5, 'reason'] += '定义五, '
filtered_df.loc[condition_6, 'reason'] += '定义六, '

# 去掉末尾的逗号和空格
filtered_df['reason'] = filtered_df['reason'].str.rstrip(', ')

# 选择要保存的列
result_df = filtered_df[['campaignName', 'adGroupName', 'total_clicks_7d', 'ACOS_7d', 'ORDER_7d', 'total_clicks_30d', 'ORDER_1m', 'ACOS_30d', 'searchTerm', 'reason']]

# 保存结果到csv
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放搜索词优化\提问策略\手动_劣质_ASIN_搜索词_v1_1_LAPASA_DE_2024-07-02.csv'
result_df.to_csv(output_path, index=False)

print(f"结果已成功保存到 {output_path}")