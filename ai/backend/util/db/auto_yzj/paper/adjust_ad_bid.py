# filename: adjust_ad_bid.py

import pandas as pd

# 读取csv文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\预处理.csv'
data = pd.read_csv(file_path)

# 定义竞价调整函数
def adjust_bid(row):
    if row['bid'] + 5 > 50:
        return 50
    return row['bid'] + 5

# 筛选符合定义一的广告位
def filter_definition_one(group):
    min_acos_7d = group['ACOS_7d'].min()
    min_acos_3d = group['ACOS_3d'].min()
    if min_acos_7d > 0 and min_acos_7d <= 0.24 and min_acos_3d > 0 and min_acos_3d <= 0.24:
        min_acos_7d_row = group[group['ACOS_7d'] == min_acos_7d].iloc[0]
        min_acos_3d_row = group[group['ACOS_3d'] == min_acos_3d].iloc[0]
        condition_one = (min_acos_7d_row['total_clicks_7d'] < group['total_clicks_7d'].max() and 
                          min_acos_3d_row['total_clicks_3d'] < group['total_clicks_3d'].max())
        if condition_one:
            min_acos_7d_row['reason'] = '定义一'
            min_acos_7d_row['adjusted_bid'] = adjust_bid(min_acos_7d_row)
            return min_acos_7d_row

# 筛选符合定义二的广告位
def filter_definition_two(group):
    min_acos_7d = group['ACOS_7d'].min()
    if min_acos_7d > 0 and min_acos_7d <= 0.24:
        min_acos_7d_row = group[group['ACOS_7d'] == min_acos_7d].iloc[0]
        condition_two = (min_acos_7d_row['total_clicks_7d'] < group['total_clicks_7d'].max() and 
                         min_acos_7d_row['total_cost_3d'] < 4 and 
                         min_acos_7d_row['total_clicks_3d'] < group['total_clicks_3d'].max())
        if condition_two:
            min_acos_7d_row['reason'] = '定义二'
            min_acos_7d_row['adjusted_bid'] = adjust_bid(min_acos_7d_row)
            return min_acos_7d_row

results = []

# 分组处理每个广告活动的广告位
grouped = data.groupby('campaignId')
for name, group in grouped:
    group_one = filter_definition_one(group)
    group_two = filter_definition_two(group)
    if group_one is not None:
        results.append(group_one)
    if group_two is not None:
        results.append(group_two)

# 创建结果的DataFrame
results_df = pd.DataFrame(results, columns=[
    'campaignName', 'campaignId', 'placementClassification', 'ACOS_7d', 'ACOS_3d', 'total_clicks_7d', 'total_clicks_3d', 'bid', 'adjusted_bid', 'reason'
])

# 保存结果到csv文件
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\提问策略\手动_优质广告位_v1_1_LAPASA_IT_2024-07-12.csv'
results_df.to_csv(output_path, index=False)

print("广告位优化完毕，结果已保存。")