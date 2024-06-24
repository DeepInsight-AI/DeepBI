# filename: identify_poor_keywords.py

import pandas as pd

# 读取CSV文件
file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\关键词优化\预处理.csv"
data = pd.read_csv(file_path)

# 定义广告组最近7天的总花费计算方法
def calculate_ad_group_7d_cost(df):
    ad_group_costs = df.groupby('adGroupName')['total_cost_7d'].sum()
    return df['adGroupName'].map(ad_group_costs)

# 添加广告组最近7天的总花费列
data['adGroup_total_cost_7d'] = calculate_ad_group_7d_cost(data)

# 为各个定义创建标识列
data['adjustment_reason'] = ''
data['closure_reason'] = ''
data['new_keywordBid'] = data['keywordBid']

# 定义一
mask1 = (data['ACOS_7d'] > 0.24) & (data['ACOS_7d'] <= 0.5) & (data['ACOS_30d'] > 0) & (data['ACOS_30d'] <= 0.5)
data.loc[mask1, 'new_keywordBid'] = data['keywordBid'] / ((data['ACOS_7d'] - 0.24) / 0.24 + 1)
data.loc[mask1, 'adjustment_reason'] = '定义一'

# 定义二
mask2 = (data['ACOS_7d'] > 0.5) & (data['ACOS_30d'] <= 0.36)
data.loc[mask2, 'new_keywordBid'] = data['keywordBid'] / ((data['ACOS_7d'] - 0.24) / 0.24 + 1)
data.loc[mask2, 'adjustment_reason'] = '定义二'

# 定义三
mask3 = (data['total_clicks_7d'] >= 10) & (data['total_cost_7d'] == 0) & (data['ACOS_30d'] <= 0.36)
data.loc[mask3, 'new_keywordBid'] = data['keywordBid'] - 0.04
data.loc[mask3, 'adjustment_reason'] = '定义三'

# 定义四
mask4 = (data['total_clicks_7d'] >= 10) & (data['total_cost_7d'] == 0) & (data['ACOS_30d'] > 0.5)
data.loc[mask4, 'closure_reason'] = '定义四'

# 定义五
mask5 = (data['ACOS_7d'] > 0.5) & (data['ACOS_30d'] > 0.36)
data.loc[mask5, 'closure_reason'] = '定义五'

# 定义六
mask6 = (data['total_sales14d_30d'] == 0) & (data['total_cost_7d'] > data['adGroup_total_cost_7d'] / 5)
data.loc[mask6, 'closure_reason'] = '定义六'

# 定义七
mask7 = (data['total_sales14d_30d'] == 0) & (data['total_clicks_30d'] >= 15)
data.loc[mask7, 'closure_reason'] = '定义七'

# 选择需要导出的列
output_columns = [
    'date', 'keyword', 'keywordId', 'campaignName', 'adGroupName', 'matchType', 
    'keywordBid', 'targeting', 'total_cost_yesterday', 'total_clicks_yesterday', 
    'total_cost_7d', 'total_sales14d_7d', 'total_sales14d_30d', 'adGroup_total_cost_7d', 
    'ACOS_7d', 'ACOS_30d', 'new_keywordBid', 'adjustment_reason', 'closure_reason'
]

# 添加日期列
data['date'] = pd.Timestamp.now().strftime('%Y-%m-%d')

# 筛选符合条件的关键词
poor_keywords = data[(data['adjustment_reason'] != '') | (data['closure_reason'] != '')]

# 指定输出路径
output_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\关键词优化\提问策略\手动_劣质关键词_IT_2024-06-11.csv"
poor_keywords.to_csv(output_path, columns=output_columns, index=False)

print('任务完成，已生成结果文件:', output_path)