# filename: ad_placement_bid_adjustment.py
import pandas as pd

# Load the CSV file
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\预处理.csv'
data = pd.read_csv(file_path)

# Define the conditions to filter the placements
condition_7d = (data['ACOS_7d'] > 0) & (data['ACOS_7d'] <= 0.24)
condition_3d = (data['ACOS_3d'] > 0) & (data['ACOS_3d'] <= 0.24)

# Calculate the average ACOS for each campaign
avg_acos_7d = data.groupby('campaignName')['ACOS_7d'].transform('mean')
avg_acos_3d = data.groupby('campaignName')['ACOS_3d'].transform('mean')

# Find the minimum ACOS values within each campaign
min_acos_7d = data.groupby('campaignName')['ACOS_7d'].transform('min')
min_acos_3d = data.groupby('campaignName')['ACOS_3d'].transform('min')

# Calculate total clicks for the last 7 days and 3 days for each campaign
total_clicks_7d = data.groupby('campaignName')['total_clicks_7d'].transform('sum')
total_clicks_3d = data.groupby('campaignName')['total_clicks_3d'].transform('sum')

# Filter the data based on the provided conditions
filtered_data = data[
    condition_7d & condition_3d &
    (data['ACOS_7d'] == min_acos_7d) & 
    (data['ACOS_3d'] == min_acos_3d) & 
    (data['total_clicks_7d'] != total_clicks_7d) & 
    (data['total_clicks_3d'] != total_clicks_3d)
]

# Calculate the bid adjustment (increase by 5% up to a maximum of 50%)
filtered_data['Bid_Adjustment'] = 1.05
filtered_data['Bid_Adjustment_Capped'] = filtered_data['Bid_Adjustment'].apply(lambda x: min(x, 1.50))

# Add reason for bid adjustment
filtered_data['Reason'] = 'ACOS within range; Increasing bid by 5%'

# Select the required columns for output
output_columns = [
    'campaignName', 'campaignId', 'placementClassification', 
    'ACOS_7d', 'ACOS_3d', 'total_clicks_7d', 
    'total_clicks_3d', 'Bid_Adjustment_Capped', 'Reason'
]
output_data = filtered_data[output_columns]

# Save the output data to a new CSV file
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\提问策略\手动_优质广告位_v1_1_ES_2024-06-14.csv'
output_data.to_csv(output_file_path, index=False)

print(f"Output saved to {output_file_path}")