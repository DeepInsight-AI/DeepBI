# filename: analyze_bad_ad_campaigns.py

import pandas as pd
import numpy as np

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv'
df = pd.read_csv(file_path)

# 初始化新列来存储调整后的竞价
df['New_keywordBid'] = df['keywordBid']
df['Operation'] = np.nan
df['Reason'] = np.nan

# 定义一
condition_1 = (df['ACOS_7d'] > 0.24) & (df['ACOS_7d'] <= 0.5) & (df['ACOS_30d'] > 0) & (df['ACOS_30d'] <= 0.5)
df.loc[condition_1, 'New_keywordBid'] = df['keywordBid'] / (((df['ACOS_7d'] - 0.24) / 0.24) + 1)
df.loc[condition_1, 'Operation'] = 'Adjust Bid'
df.loc[condition_1, 'Reason'] = '定义一'

# 定义二
condition_2 = (df['ACOS_7d'] > 0.5) & (df['ACOS_30d'] <= 0.36)
df.loc[condition_2, 'New_keywordBid'] = df['keywordBid'] / (((df['ACOS_7d'] - 0.24) / 0.24) + 1)
df.loc[condition_2, 'Operation'] = 'Adjust Bid'
df.loc[condition_2, 'Reason'] = '定义二'

# 定义三
condition_3 = (df['total_clicks_7d'] >= 10) & (df['total_sales14d_7d'] == 0) & (df['ACOS_30d'] <= 0.36)
df.loc[condition_3, 'New_keywordBid'] -= 0.04
df.loc[condition_3, 'Operation'] = 'Adjust Bid'
df.loc[condition_3, 'Reason'] = '定义三'

# 定义四
condition_4 = (df['total_clicks_7d'] > 10) & (df['total_sales14d_7d'] == 0) & (df['ACOS_30d'] > 0.5)
df.loc[condition_4, 'New_keywordBid'] = '关闭'
df.loc[condition_4, 'Operation'] = 'Close'
df.loc[condition_4, 'Reason'] = '定义四'

# 定义五
condition_5 = (df['ACOS_7d'] > 0.5) & (df['ACOS_30d'] > 0.36)
df.loc[condition_5, 'New_keywordBid'] = '关闭'
df.loc[condition_5, 'Operation'] = 'Close'
df.loc[condition_5, 'Reason'] = '定义五'

# 定义六
condition_6 = (df['total_sales14d_30d'] == 0) & (df['total_cost_30d'] >= 5)
df.loc[condition_6, 'New_keywordBid'] = '关闭'
df.loc[condition_6, 'Operation'] = 'Close'
df.loc[condition_6, 'Reason'] = '定义六'

# 定义七
condition_7 = (df['total_sales14d_30d'] == 0) & (df['total_clicks_30d'] >= 15) & (df['total_clicks_7d'] > 0)
df.loc[condition_7, 'New_keywordBid'] = '关闭'
df.loc[condition_7, 'Operation'] = 'Close'
df.loc[condition_7, 'Reason'] = '定义七'

# 过滤掉没有操作的记录
result_df = df.dropna(subset=['Operation'])

# 选择并重命名需要输出的列
output_columns = [
    'keyword', 'keywordId', 'campaignName', 'adGroupName', 'matchType', 'keywordBid', 
    'New_keywordBid', 'targeting', 'total_cost_yesterday', 'total_sales14d_yesterday', 
    'total_cost_7d', 'total_sales14d_7d', 'ACOS_7d', 'ACOS_30d', 
    'total_clicks_30d', 'Operation', 'Reason'
]
result_df = result_df[output_columns]

# 保存到新的CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_劣质商品投放_v1_1_OutdoorMaster_IT_2024-07-09.csv'
result_df.to_csv(output_file_path, index=False)

print(f'Results saved to {output_file_path}')