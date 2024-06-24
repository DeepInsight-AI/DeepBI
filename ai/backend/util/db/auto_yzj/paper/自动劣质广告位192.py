# filename: update_poor_placements.py
import pandas as pd

# 读取csv文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\预处理.csv'
data = pd.read_csv(file_path)

# 初始化新的列
data['new_bid'] = data['bid']
data['reason'] = ''

# 定义一：最近7天的总sales为0，但最近7天的总点击数大于0的广告位 -> 竞价变为0
definition_one = data[(data['total_sales14d_7d'] == 0) & (data['total_clicks_7d'] > 0)].index
data.loc[definition_one, 'new_bid'] = 0
data.loc[definition_one, 'reason'] = 'Definition One'

# 定义三：最近7天的平均ACOS值大于等于50%的广告位 -> 竞价变为0
definition_three = data[data['ACOS_7d'] >= 50].index
data.loc[definition_three, 'new_bid'] = 0
data.loc[definition_three, 'reason'] = 'Definition Three'

# 确定定义二的逻辑
def check_definition_two(group):
    highest_acos = group['ACOS_7d'].max()
    lowest_acos = group['ACOS_7d'].min()
    if highest_acos > 24 and highest_acos < 50 and lowest_acos > 24 and lowest_acos < 50 and (highest_acos - lowest_acos >= 0.2):
        max_bid_idx = group[group['ACOS_7d'] == highest_acos].index
        group.loc[max_bid_idx, 'new_bid'] = group.loc[max_bid_idx, 'bid'] * 0.97
        group.loc[max_bid_idx, 'reason'] = 'Definition Two'
    return group

# 对每个广告活动进行定义二的检查
data = data.groupby('campaignName').apply(check_definition_two)

# 删除无用列并过滤有更改的行
result = data[~data['reason'].isna()]
result = result[['campaignName', 'campaignId', 'placementClassification', 'ACOS_7d', 'ACOS_3d', 'total_clicks_7d', 'total_clicks_3d', 'bid', 'new_bid', 'reason']]

# 保存结果到指定路径
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\提问策略\自动_劣质广告位_v1_1_IT_2024-06-19.csv'
result.to_csv(output_file_path, index=False)

print("结果已保存到:", output_file_path)