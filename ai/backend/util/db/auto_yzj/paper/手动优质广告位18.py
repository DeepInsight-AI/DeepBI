# filename: optimize_ads.py

import pandas as pd
import os
from datetime import datetime

# Step 1: Load Data
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\预处理.csv'
data = pd.read_csv(file_path)

# Step 2: Filter the data based on the given conditions

# Calculate min ACOS for the same campaign
min_acos_7d = data.groupby('campaignId')['ACOS_7d'].transform('min')
min_acos_3d = data.groupby('campaignId')['ACOS_3d'].transform('min')

# Find max clicks for the same campaign
max_clicks_7d = data.groupby('campaignId')['total_clicks_7d'].transform('max')
max_clicks_3d = data.groupby('campaignId')['total_clicks_3d'].transform('max')

# Conditions
condition = (
    (data['ACOS_7d'] == min_acos_7d) &
    (data['ACOS_7d'] > 0) & (data['ACOS_7d'] <= 0.24) &
    (data['total_clicks_7d'] != max_clicks_7d) &
    (data['ACOS_3d'] == min_acos_3d) &
    (data['ACOS_3d'] > 0) & (data['ACOS_3d'] <= 0.24) &
    (data['total_clicks_3d'] != max_clicks_3d)
)

# Apply the condition
filtered_data = data[condition].copy()

# Step 3: Adjust the bid
filtered_data['new_bid'] = filtered_data['bid'] * 1.05
filtered_data.loc[filtered_data['new_bid'] > filtered_data['bid'] * 1.5, 'new_bid'] = filtered_data['bid'] * 1.5

# Adding reason for bid adjustment
filtered_data['reason'] = 'Good performance with low ACOS and non-maximum clicks.'

# Step 4: Save the result to a new CSV file
output_folder = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\提问策略'
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

output_file_path = os.path.join(output_folder, f'手动_优质广告位_v1_1_ES_{datetime.now().strftime("%Y-%m-%d")}.csv')

filtered_data.to_csv(output_file_path, index=False, columns=[
    'campaignName', 'campaignId', 'placementClassification', 'ACOS_7d', 'ACOS_3d',
    'total_clicks_7d', 'total_clicks_3d', 'bid', 'new_bid', 'reason'
])

print(f"CSV file saved to: {output_file_path}")