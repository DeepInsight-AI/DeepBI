# filename: bid_adjustment.py

import pandas as pd
from datetime import datetime

# Load the data
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\预处理.csv'
data = pd.read_csv(file_path)

# Define date for logging purposes
today_date = '2024-05-27'

# Filter and identify best performing ads
def identify_best_ads(df):
    result = []
    
    # Group by campaign name
    grouped_campaigns = df.groupby('campaignName')
    
    for campaign_name, group in grouped_campaigns:
        group = group.copy()
        
        # Identify best performing ads as per recent 7 and 3 days ACOS and clicks
        group_7d = group[(group['ACOS_7d'] <= 0.24) & (group['ACOS_7d'] > 0)]
        if group_7d.empty:
            continue

        group_3d = group[(group['ACOS_3d'] <= 0.24) & (group['ACOS_3d'] > 0)]
        if group_3d.empty:
            continue
        
        # Find the ad placement with minimum ACOS and not maximum clicks in 7 and 3 days respectively
        min_acos_7d = group_7d.loc[group_7d['ACOS_7d'].idxmin()]
        max_clicks_7d = group_7d['total_clicks_7d'].max()
        
        if min_acos_7d['total_clicks_7d'] < max_clicks_7d:
            min_acos_3d = group_3d.loc[group_3d['ACOS_3d'].idxmin()]
            max_clicks_3d = group_3d['total_clicks_3d'].max()

            if min_acos_3d['total_clicks_3d'] < max_clicks_3d:
                # Assuming bid adjustment reason
                reason = "最近7天和3天的平均ACOS值最小, 并且点击次数不是最大"
                result.append([
                    today_date,
                    campaign_name,
                    min_acos_7d['placementClassification'],
                    min_acos_7d['ACOS_7d'],
                    min_acos_3d['ACOS_3d'],
                    min_acos_7d['total_clicks_7d'],
                    min_acos_3d['total_clicks_3d'],
                    reason
                ])
    
    return result

# Identify best performing ads
best_ads = identify_best_ads(data)

# Prepare output DataFrame
columns = ['date', 'campaignName', 'placementClassification', 'ACOS_7d', 'ACOS_3d', 'total_clicks_7d', 'total_clicks_3d', 'reason']
output_df = pd.DataFrame(best_ads, columns=columns)

# Save to CSV
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\提问策略\手动_优质广告位_ES_2024-06-05.csv'
output_df.to_csv(output_path, index=False)

print(f'Successfully saved the output to {output_path}')