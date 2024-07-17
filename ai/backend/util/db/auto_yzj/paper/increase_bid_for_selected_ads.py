# filename: increase_bid_for_selected_ads.py

import pandas as pd

# 读取 CSV 文件
csv_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\预处理.csv'
df = pd.read_csv(csv_path)

# 初始化
df['ACOS_7d'] = pd.to_numeric(df['ACOS_7d'], errors='coerce')
df['ACOS_3d'] = pd.to_numeric(df['ACOS_3d'], errors='coerce')
df['total_clicks_7d'] = pd.to_numeric(df['total_clicks_7d'], errors='coerce')
df['total_clicks_3d'] = pd.to_numeric(df['total_clicks_3d'], errors='coerce')
df['total_cost_3d'] = pd.to_numeric(df['total_cost_3d'], errors='coerce')
df['bid'] = pd.to_numeric(df['bid'], errors='coerce')

# 定义广告位类型
placements = ['Top of Search on-Amazon', 'Detail Page on-Amazon', 'Other on-Amazon']

# 存储结果
results = []

for campaign_id, group in df.groupby('campaignId'):
    for placement in placements:
        placement_group = group[group['placementClassification'] == placement]
        
        # 筛选符合定义一的广告位
        definition_one = placement_group[
            (placement_group['ACOS_7d'] == placement_group['ACOS_7d'].min()) &
            (placement_group['ACOS_7d'] > 0) &
            (placement_group['ACOS_7d'] <= 0.24) &
            (placement_group['total_clicks_7d'] != placement_group['total_clicks_7d'].max()) &
            (placement_group['ACOS_3d'] == placement_group['ACOS_3d'].min()) &
            (placement_group['ACOS_3d'] > 0) &
            (placement_group['ACOS_3d'] <= 0.24) &
            (placement_group['total_clicks_3d'] != placement_group['total_clicks_3d'].max())
        ]

        # 定义一的竞价调整
        for _, row in definition_one.iterrows():
            new_bid = min(row['bid'] + 5, 50)
            results.append([
                row['campaignName'], row['campaignId'], row['placementClassification'], 
                row['ACOS_7d'], row['ACOS_3d'], row['total_clicks_7d'], 
                row['total_clicks_3d'], row['bid'], new_bid, '定义一: 调整竞价5，直到50'
            ])
        
        # 筛选符合定义二的广告位
        definition_two = placement_group[
            (placement_group['ACOS_7d'] == placement_group['ACOS_7d'].min()) &
            (placement_group['ACOS_7d'] > 0) &
            (placement_group['ACOS_7d'] <= 0.24) &
            (placement_group['total_clicks_7d'] != placement_group['total_clicks_7d'].max()) &
            (placement_group['total_cost_3d'] < 4) &
            (placement_group['total_clicks_3d'] != placement_group['total_clicks_3d'].max())
        ]

        # 定义二的竞价调整
        for _, row in definition_two.iterrows():
            new_bid = min(row['bid'] + 5, 50)
            results.append([
                row['campaignName'], row['campaignId'], row['placementClassification'], 
                row['ACOS_7d'], row['ACOS_3d'], row['total_clicks_7d'], 
                row['total_clicks_3d'], row['bid'], new_bid, '定义二: 调整竞价5，直到50'
            ])

# 生成新的CSV文件内容
results_df = pd.DataFrame(results, columns=[
    'campaignName', 'campaignId', 'placementClassification', 
    'ACOS_7d', 'ACOS_3d', 'total_clicks_7d', 
    'total_clicks_3d', 'bid', 'new_bid', 'reason'
])

output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\提问策略\手动_优质广告位_v1_1_LAPASA_UK_2024-07-12.csv'
results_df.to_csv(output_path, index=False)

print("CSV 文件已生成并保存至", output_path)