# filename: find_poor_placements.py

import pandas as pd

# 读取 CSV 文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\预处理.csv'
data = pd.read_csv(file_path)

# 计算最近7天和3天的平均ACOS值
data['avg_ACOS_7d'] = data['ACOS_7d'] / 7
data['avg_ACOS_3d'] = data['ACOS_3d'] / 3

# 初始化结果列表
results = []

# 定义一: 最近7天的总sales为0，但最近7天的总点击数大于0的广告位
condition1 = (data['total_sales14d_7d'] == 0) & (data['total_clicks_7d'] > 0)
result_condition1 = data[condition1].copy()
result_condition1['reason'] = '定义一：最近7天的总sales为0，但最近7天的总点击数大于0的广告位'
result_condition1['bid_adjustment'] = '竞价变为0'
results.append(result_condition1)

# 定义二: 在同一广告活动中，三个广告位的平均ACOS值都大于0.24且小于0.5，且最大与最小之间的差值大于等于0.2，降低最大ACOS广告位的竞价3%
campaign_groups = data.groupby('campaignId')
condition2_results = []

for campaignId, group in campaign_groups:
    if len(group) < 3:
        continue
    
    trimmed_group = group[(group['avg_ACOS_7d'] > 0.24) & (group['avg_ACOS_7d'] < 0.5)]
    if len(trimmed_group) >= 3:
        max_ACOS = trimmed_group['avg_ACOS_7d'].max()
        min_ACOS = trimmed_group['avg_ACOS_7d'].min()
        if (max_ACOS - min_ACOS) >= 0.2:
            max_ACOS_row = trimmed_group[trimmed_group['avg_ACOS_7d'] == max_ACOS]
            max_ACOS_row['reason'] = '定义二：降低最大ACOS广告位的竞价3%'
            max_ACOS_row['bid_adjustment'] = '竞价降低3%'
            condition2_results.append(max_ACOS_row)
            
if condition2_results:
    condition2_results = pd.concat(condition2_results, ignore_index=True)
else:
    condition2_results = pd.DataFrame()
results.append(condition2_results)

# 定义三: 最近7天的平均ACOS值大于等于0.5的广告位
condition3 = (data['avg_ACOS_7d'] >= 0.5)
result_condition3 = data[condition3].copy()
result_condition3['reason'] = '定义三：最近7天的平均ACOS值大于等于0.5的广告位'
result_condition3['bid_adjustment'] = '竞价变为0'
results.append(result_condition3)

# 合并所有结果
final_results = pd.concat(results, ignore_index=True)

# 选择输出所需的列，并重命名
output_columns = [
    'campaignName', 'campaignId', 'placementClassification',
    'avg_ACOS_7d', 'avg_ACOS_3d', 'total_clicks_7d', 'total_clicks_3d',
    'bid_adjustment', 'reason'
]
final_results = final_results[output_columns]

# 保存结果到CSV文件
output_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\提问策略\手动_劣质广告位_v1_1_ES_2024-06-14.csv'
final_results.to_csv(output_file, index=False)

print(f"结果已保存到 {output_file}")