# filename: optimal_ad_placement.py

import pandas as pd

# Load the CSV data
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\预处理.csv'
data = pd.read_csv(file_path)

# Data Filtering based on the definition
def find_optimal_placements(data):
    result = []

    for campaign_name, group in data.groupby('campaignName'):
        # 7-day ACOS and Clicks
        group['min_ACOS_7d'] = group['ACOS_7d'].min()
        group = group[(group['ACOS_7d'] == group['min_ACOS_7d']) & (group['ACOS_7d'] > 0) & (group['ACOS_7d'] <= 0.24)]
        
        if group.empty or group['total_clicks_7d'].max() == group['total_clicks_7d'].iloc[0]:
            continue

        # 3-day ACOS and Clicks
        group['min_ACOS_3d'] = group['ACOS_3d'].min()
        group = group[(group['ACOS_3d'] == group['min_ACOS_3d']) & (group['ACOS_3d'] > 0) & (group['ACOS_3d'] <= 0.24)]

        if group.empty or group['total_clicks_3d'].max() == group['total_clicks_3d'].iloc[0]:
            continue

        for _, row in group.iterrows():
            result.append({
                'campaignName': campaign_name,
                'placementClassification': row['placementClassification'],
                'ACOS_7d': row['ACOS_7d'],
                'ACOS_3d': row['ACOS_3d'],
                'total_clicks_7d': row['total_clicks_7d'],
                'total_clicks_3d': row['total_clicks_3d'],
                'reason': '7-day ACOS and 3-day ACOS are the lowest but not the highest clicks in their corresponding time periods'
            })

    return pd.DataFrame(result)

# Find the optimal placements
optimal_placements = find_optimal_placements(data)

# Save to CSV
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\提问策略\自动_优质广告位_IT_2024-06-05.csv'
optimal_placements.to_csv(output_path, index=False)

print(f"Optimal placements have been saved to {output_path}")