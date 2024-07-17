# filename: 表现较差的广告位更新竞价.py

import pandas as pd
from datetime import datetime

# 读取CSV数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\预处理.csv'
data = pd.read_csv(file_path)

# 定义一个新的列用于存储新的竞价
data['new_bid'] = data['bid']

# --- 定义一 ---#
# 最近7天的总sales为0，但最近7天的总点击数大0的广告位竞价变为0
data.loc[(data['total_sales14d_7d'] == 0) & (data['total_clicks_7d'] > 0), 'new_bid'] = 0

# --- 定义二 ---#
# 找出同一广告活动中满足条件的广告位，并降低指定竞价
for campaign_id in data['campaignId'].unique():
    campaign_data = data[data['campaignId'] == campaign_id]
    if len(campaign_data) == 3:
        acos_7d = campaign_data['ACOS_7d']
        if acos_7d.between(24, 50).all() and (acos_7d.max() - acos_7d.min() >= 0.2):
            max_acos_index = acos_7d.idxmax()
            data.loc[max_acos_index, 'new_bid'] = max(0, data.loc[max_acos_index, 'new_bid'] - 3)

# --- 定义三 ---#
# 最近7天的平均ACOS值大于等于50%的广告位竞价变为0
data.loc[data['ACOS_7d'] >= 50, 'new_bid'] = 0

# 添加竞价操作的具体原因的列
data['竞价操作原因'] = ''

# 填写竞价操作原因
data.loc[(data['total_sales14d_7d'] == 0) & (data['total_clicks_7d'] > 0), '竞价操作原因'] = '定义一'

for campaign_id in data['campaignId'].unique():
    campaign_data = data[data['campaignId'] == campaign_id]
    if len(campaign_data) == 3:
        acos_7d = campaign_data['ACOS_7d']
        if acos_7d.between(24, 50).all() and (acos_7d.max() - acos_7d.min() >= 0.2):
            max_acos_index = acos_7d.idxmax()
            data.loc[max_acos_index, '竞价操作原因'] = '定义二'

data.loc[data['ACOS_7d'] >= 50, '竞价操作原因'] = '定义三'

# 过滤需要输出的列
output_columns = [
    'campaignName',
    'campaignId',
    'placementClassification',
    'ACOS_7d',
    'ACOS_3d',
    'total_clicks_7d',
    'total_clicks_3d',
    'bid',
    'new_bid',
    '竞价操作原因'
]

result_data = data[data['竞价操作原因'] != ''][output_columns]

# 输出到新的CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\提问策略\自动_劣质广告位_v1_1_ES_2024-06-21.csv'
result_data.to_csv(output_file_path, index=False)

print(f"处理完成，结果已保存到{output_file_path}")