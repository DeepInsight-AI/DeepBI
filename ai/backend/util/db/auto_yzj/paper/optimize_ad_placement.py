# filename: optimize_ad_placement.py

import pandas as pd

# Step 1: 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\预处理.csv'
data = pd.read_csv(file_path)

# Step 2: 筛选广告位
good_placements = []

# 对于每个广告活动
for campaign_id, group in data.groupby('campaignId'):
    # 计算需要的各项指标
    group['avg_ACOS_3d'] = group['ACOS_3d']
    group['avg_ACOS_7d'] = group['ACOS_7d']
    
    # 定义1 过滤条件
    condition1 = (
        (group['avg_ACOS_7d'] > 0) & (group['avg_ACOS_7d'] <= 0.24) &
        (group['avg_ACOS_3d'] > 0) & (group['avg_ACOS_3d'] <= 0.24) &
        (group['total_cost_3d'] > 4) &
        (group['total_clicks_7d'] != group['total_clicks_7d'].max()) &
        (group['total_clicks_3d'] != group['total_clicks_3d'].max())
    )
    
    # 定义2 过滤条件
    condition2 = (
        (group['avg_ACOS_7d'] > 0) & (group['avg_ACOS_7d'] <= 0.24) &
        (group['total_cost_3d'] < 4) &
        (group['total_clicks_7d'] != group['total_clicks_7d'].max()) &
        (group['total_clicks_3d'] != group['total_clicks_3d'].max())
    )

    # 筛选符合条件的广告位
    selected = group[condition1 | condition2]
    
    for idx, row in selected.iterrows():
        reason = '满足定义一' if condition1[idx] else '满足定义二'
        adjusted_bid = min(row['bid'] + 5, 50)
        good_placements.append([
            row['campaignName'],
            row['campaignId'],
            row['placementClassification'],
            row['avg_ACOS_7d'],
            row['avg_ACOS_3d'],
            row['total_clicks_7d'],
            row['total_clicks_3d'],
            row['bid'],
            adjusted_bid,
            reason
        ])

# Step 3: 创建新的DataFrame并保存CSV
columns = [
    'campaignName', 'campaignId', 'placementClassification', 'avg_ACOS_7d', 
    'avg_ACOS_3d', 'total_clicks_7d', 'total_clicks_3d', 'bid', 'adjusted_bid', 'reason'
]

result_df = pd.DataFrame(good_placements, columns=columns)
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\提问策略\自动_优质广告位_v1_1_LAPASA_US_2024-07-14.csv'
result_df.to_csv(output_path, index=False)

print(f"生成的CSV文件已保存到: {output_path}")