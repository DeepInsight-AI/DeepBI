# filename: process_ad_performance.py

import pandas as pd

# Load the dataset
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\预处理.csv'
data = pd.read_csv(file_path)

# Add a new column for bid adjustment reason initialized as an empty string
data['竞价调整'] = ''
data['原因'] = ''

# Definition 1: Total sales for the last 7 days is 0, and the total clicks for the last 7 days is greater than 0
definition_1 = (data['total_sales14d_7d'] == 0) & (data['total_clicks_7d'] > 0)

# For such ad placements, bid adjustment should be 0
data.loc[definition_1, ['竞价调整', '原因']] = [0, '最近7天的总sales为0，但最近7天的总点击数大0']

# Definition 3: The average ACOS for the last 7 days is greater than or equal to 0.5
definition_3 = data['ACOS_7d'] >= 0.5

# For such ad placements, bid adjustment should be 0
data.loc[definition_3, ['竞价调整', '原因']] = [0, '最近7天的平均ACOS值大于等于0.5']

# Definition 2: Special conditions for ad placements within the same campaign
for campaign in data['campaignName'].unique():
    campaign_data = data[data['campaignName'] == campaign]
    if len(campaign_data) < 3:
        continue
    
    # Check if all ad placements within the same campaign meet the criteria
    condition = (campaign_data['ACOS_7d'] > 0.24) & (campaign_data['ACOS_7d'] < 0.5)
    if condition.sum() == len(campaign_data):
        max_acos_idx = campaign_data['ACOS_7d'].idxmax()
        min_acos_idx = campaign_data['ACOS_7d'].idxmin()
        if (campaign_data.loc[max_acos_idx, 'ACOS_7d'] - campaign_data.loc[min_acos_idx, 'ACOS_7d']) >= 0.2:
            current_bid_adjustment = data.loc[max_acos_idx, '竞价调整']
            if current_bid_adjustment == '':
                data.loc[max_acos_idx, '竞价调整'] = -0.03
                data.loc[max_acos_idx, '原因'] = '同一广告活动内的三个广告位ACOS值差异大于0.2'

# Filter poor performing ads
poor_performing_ads = data[data['竞价调整'] != '']

# Select required columns
result = poor_performing_ads[[
    'campaignName', 
    'placementClassification', 
    'ACOS_7d', 
    'ACOS_3d', 
    'total_clicks_7d', 
    'total_clicks_3d', 
    '竞价调整',
    '原因'
]]

# Save the result to a new CSV file
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\提问策略\劣质广告位_FR_2024-5-27.csv'
result.to_csv(output_path, index=False)

print(f"Results have been saved to {output_path}")