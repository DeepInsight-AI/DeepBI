# filename: quality_keyword_analysis.py

import pandas as pd

# 读取数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\滞销品优化\手动sp广告\关键词优化\预处理.csv'
df = pd.read_csv(file_path)

# 计算最近7天和30天的平均ACOS
df['avg_ACOS_7d'] = df['ACOS_7d']
df['avg_ACOS_30d'] = df['ACOS_30d']

# 定义新的关键词竞价和操作原因
df['new_keywordBid'] = df['keywordBid']
df['action_reason'] = ''

# 定义关键词竞价调整和关闭条件并应用
# 定义一
condition_1 = (df['avg_ACOS_7d'] > 0.27) & (df['avg_ACOS_7d'] <= 0.5) & (df['avg_ACOS_30d'] > 0) & (df['avg_ACOS_30d'] <= 0.5) & (df['ORDER_1m'] < 5)
df.loc[condition_1, 'new_keywordBid'] = df.loc[condition_1, 'keywordBid'] / ((df.loc[condition_1, 'avg_ACOS_7d'] - 0.27) / 0.27 + 1)
df.loc[condition_1, 'action_reason'] = '定义一'

# 定义二
condition_2 = (df['avg_ACOS_7d'] > 0.5) & (df['avg_ACOS_30d'] <= 0.36)
df.loc[condition_2, 'new_keywordBid'] = df.loc[condition_2, 'keywordBid'] / ((df.loc[condition_2, 'avg_ACOS_7d'] - 0.27) / 0.27 + 1)
df.loc[condition_2, 'action_reason'] = '定义二'

# 定义三
condition_3 = (df['total_clicks_7d'] >= 10) & (df['total_sales14d_7d'] == 0) & (df['avg_ACOS_30d'] <= 0.36)
df.loc[condition_3, 'new_keywordBid'] = df.loc[condition_3, 'keywordBid'] - 0.04
df.loc[condition_3, 'action_reason'] = '定义三'

# 定义四
condition_4 = (df['total_clicks_7d'] >= 10) & (df['total_sales14d_7d'] == 0) & (df['avg_ACOS_30d'] > 0.5)
df.loc[condition_4, 'new_keywordBid'] = '关闭'
df.loc[condition_4, 'action_reason'] = '定义四'

# 定义五
condition_5 = (df['avg_ACOS_7d'] > 0.5) & (df['avg_ACOS_30d'] > 0.36)
df.loc[condition_5, 'new_keywordBid'] = '关闭'
df.loc[condition_5, 'action_reason'] = '定义五'

# 定义六
condition_6 = (df['total_sales14d_30d'] == 0) & (df['total_cost_30d'] >= 5)
df.loc[condition_6, 'new_keywordBid'] = '关闭'
df.loc[condition_6, 'action_reason'] = '定义六'

# 定义七
condition_7 = (df['total_sales14d_30d'] == 0) & (df['total_clicks_30d'] >= 15) & (df['total_clicks_7d'] > 0)
df.loc[condition_7, 'new_keywordBid'] = '关闭'
df.loc[condition_7, 'action_reason'] = '定义七'

# 输出符合条件的关键词的数据到新的CSV文件
output_columns = [
    'keyword', 'keywordId', 'campaignName', 'adGroupName', 'matchType', 'keywordBid', 'new_keywordBid',
    'targeting', 'total_cost_30d', 'total_clicks_30d', 'total_cost_7d', 'total_sales14d_7d',
    'avg_ACOS_7d', 'avg_ACOS_30d', 'total_clicks_30d', 'action_reason'
]
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\滞销品优化\手动sp广告\关键词优化\提问策略\手动_劣质关键词_v1_1_LAPASA_DE_2024-07-03.csv'
df.loc[df['action_reason'] != '', output_columns].to_csv(output_file_path, index=False)

print("分析完成，输出文件路径为：", output_file_path)