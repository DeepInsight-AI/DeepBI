# filename: process_poor_performance_ads.py
import pandas as pd

# 文件路径
input_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv'
output_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_劣质商品投放_v1_1_LAPASA_US_2024-07-03.csv'

# 读取数据
df = pd.read_csv(input_file)

# 初始化new_keywordBid列
df['New_keywordBid'] = df['keywordBid']

# 定义动作日志
df['Action'] = ''

# 定义一
condition1 = (df['ACOS_7d'] > 0.24) & (df['ACOS_7d'] <= 0.5) & (df['ACOS_30d'] > 0) & (df['ACOS_30d'] <= 0.5)
df.loc[condition1, 'New_keywordBid'] = df['keywordBid'] / ((df['ACOS_7d'] - 0.24)/0.24 + 1)
df.loc[condition1, 'Action'] = 'Adjust bid according to definition 1'

# 定义二
condition2 = (df['ACOS_7d'] > 0.5) & (df['ACOS_30d'] <= 0.36)
df.loc[condition2, 'New_keywordBid'] = df['keywordBid'] / ((df['ACOS_7d'] - 0.24)/0.24 + 1)
df.loc[condition2, 'Action'] = 'Adjust bid according to definition 2'

# 定义三
condition3 = (df['total_clicks_7d'] > 10) & (df['total_sales14d_7d'] == 0) & (df['ACOS_30d'] <= 0.36)
df.loc[condition3, 'New_keywordBid'] = df['keywordBid'] - 0.04
df.loc[condition3, 'Action'] = 'Adjust bid according to definition 3'

# 定义四
condition4 = (df['total_clicks_7d'] > 10) & (df['total_sales14d_7d'] == 0) & (df['ACOS_30d'] > 0.5)
df.loc[condition4, 'New_keywordBid'] = '关闭'
df.loc[condition4, 'Action'] = 'Close according to definition 4'

# 定义五
condition5 = (df['ACOS_7d'] > 0.5) & (df['ACOS_30d'] > 0.36)
df.loc[condition5, 'New_keywordBid'] = '关闭'
df.loc[condition5, 'Action'] = 'Close according to definition 5'

# 定义六
condition6 = (df['total_sales14d_30d'] == 0) & (df['total_cost_30d'] >= 5)
df.loc[condition6, 'New_keywordBid'] = '关闭'
df.loc[condition6, 'Action'] = 'Close according to definition 6'

# 定义七
condition7 = (df['total_sales14d_30d'] == 0) & (df['total_clicks_30d'] >= 15) & (df['total_clicks_7d'] > 0)
df.loc[condition7, 'New_keywordBid'] = '关闭'
df.loc[condition7, 'Action'] = 'Close according to definition 7'

# 筛选出需要调整和关闭的记录
filtered_df = df[df['Action'] != '']

# 输出到 CSV 文件
columns = [
    'keywordId',
    'keyword',
    'campaignName',
    'adGroupName',
    'matchType',
    'keywordBid',
    'New_keywordBid',
    'targeting',
    'total_cost_7d',
    'total_sales14d_7d',
    'total_clicks_7d',
    'ACOS_7d',
    'ACOS_30d',
    'total_clicks_30d',
    'Action'
]
filtered_df[columns].to_csv(output_file, index=False)

print(f'Results have been saved to {output_file}')