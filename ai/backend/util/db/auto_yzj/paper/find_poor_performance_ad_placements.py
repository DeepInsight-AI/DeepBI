# filename: find_poor_performance_ad_placements.py

import pandas as pd

# Step 1: Load the data from the CSV file
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\预处理.csv'
data = pd.read_csv(file_path)

# Step 2: Applying definitions to identify poor performing ad placements
poor_ad_placements = []

# Definition 1
def1 = data[(data['total_sales14d_7d'] == 0) & (data['total_clicks_7d'] > 0)]
def1['reason'] = 'Definition 1: Total sales in the last 7 days is 0, but total clicks > 0'
def1['new_bid'] = 0
poor_ad_placements.append(def1)

# Definition 2
campaigns = data['campaignName'].unique()
for campaign in campaigns:
    campaign_data = data[data['campaignName'] == campaign]
    avg_acos_7d = campaign_data['ACOS_7d'].mean()
    if len(campaign_data) >= 3:
        sorted_campaign_data = campaign_data.sort_values(by='ACOS_7d', ascending=False)
        high_acos = sorted_campaign_data.iloc[0]
        low_acos = sorted_campaign_data.iloc[2]
        if (high_acos['ACOS_7d'] > 0.24) & (high_acos['ACOS_7d'] < 0.5) & \
           (low_acos['ACOS_7d'] > 0.24) & (low_acos['ACOS_7d'] < 0.5) & \
           (high_acos['ACOS_7d'] - low_acos['ACOS_7d'] >= 0.2):
            high_acos['reason'] = f'Definition 2: ACOS in last 7 days for three placements are between 0.24 and 0.5, and difference between highest and lowest > 0.2'
            high_acos['new_bid'] = high_acos['ACOS_7d'] * 0.97
            poor_ad_placements.append(high_acos)

# Definition 3
def3 = data[data['ACOS_7d'] >= 0.5]
def3['reason'] = 'Definition 3: ACOS in the last 7 days is >= 0.5'
def3['new_bid'] = 0
poor_ad_placements.append(def3)

# Step 3: Output the results to a CSV file
result = pd.concat(poor_ad_placements)
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\提问策略\手动_劣质广告位_IT_2024-06-08.csv'
result.to_csv(output_path, index=False)

print(f"Results have been saved to {output_path}")