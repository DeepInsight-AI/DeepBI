# filename: process_ad_placements.py
import pandas as pd

# Load the CSV file into a DataFrame
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\预处理.csv'
df = pd.read_csv(file_path)

# Initialize an empty list to collect the filtered placements
filtered_list = []

# Step 1: Group by campaign
grouped = df.groupby('campaignId')

for campaign_id, group in grouped:
    # Ensure there are at least 3 different placements in the campaign
    if len(group) >= 3:
        # Filter placements with 7d ACOS between 0 and 0.24, and 3d ACOS between 0 and 0.24
        relevant = group[(group['ACOS_7d'] > 0) & (group['ACOS_7d'] <= 0.24) & (group['ACOS_3d'] > 0) & (group['ACOS_3d'] <= 0.24)]
        
        # If there are relevant placements
        if not relevant.empty:
            # Identify the placement with the minimum 7d ACOS
            min_7d_acos = relevant['ACOS_7d'].min()
            min_7d_acos_placement = relevant[relevant['ACOS_7d'] == min_7d_acos]
            
            # Ensure this placement is not the one with the highest 7d clicks
            max_7d_clicks = relevant['total_clicks_7d'].max()
            min_7d_acos_placement = min_7d_acos_placement[min_7d_acos_placement['total_clicks_7d'] != max_7d_clicks]
            
            # Ensure this placement also has the minimum 3d ACOS and not the highest 3d clicks
            if not min_7d_acos_placement.empty:
                min_3d_acos = min_7d_acos_placement['ACOS_3d'].min()
                min_3d_acos_placement = min_7d_acos_placement[min_7d_acos_placement['ACOS_3d'] == min_3d_acos]
                max_3d_clicks = relevant['total_clicks_3d'].max()
                min_3d_acos_placement = min_3d_acos_placement[min_3d_acos_placement['total_clicks_3d'] != max_3d_clicks]
                
                if not min_3d_acos_placement.empty:
                    # Increase bid by 5, but cap it at 50
                    min_3d_acos_placement['new_bid'] = min_3d_acos_placement['bid'].apply(lambda x: min(x + 5, 50))
                    min_3d_acos_placement['reason'] = 'Performance is good with low ACOS but not highest clicks'

                    # Collect the filtered data
                    filtered_list.append(min_3d_acos_placement)

# Concatenate all filtered placements into a single DataFrame
filtered_df = pd.concat(filtered_list, ignore_index=True)

# Select required columns
output_df = filtered_df[['campaignName', 'campaignId', 'placementClassification', 'ACOS_7d', 'ACOS_3d', 'total_clicks_7d', 'total_clicks_3d', 'bid', 'new_bid', 'reason']]

# Save the output to a new CSV file
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\提问策略\手动_优质广告位_v1_1_ES_2024-06-21.csv'
output_df.to_csv(output_file_path, index=False)

print(f"Filtered placements data saved to {output_file_path}")