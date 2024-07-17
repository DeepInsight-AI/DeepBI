# filename: adjust_bids_debug_v7.py

import pandas as pd

# Load the dataset
input_file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\预处理.csv"
output_file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\提问策略\自动_优质广告位_v1_1_LAPASA_UK_2024-07-12.csv"
df = pd.read_csv(input_file_path)

# Print the first few rows of the dataframe for review
print(df.head())
print(df.describe())

# Handle missing values: Remove rows where critical columns have missing values
df = df.dropna(subset=['ACOS_7d', 'ACOS_3d', 'total_clicks_7d', 'total_clicks_3d', 'total_cost_3d', 'bid'])

# Ensure that zero values are not considered
df = df[(df['ACOS_7d'] > 0) &
        (df['total_clicks_7d'] > 0) &
        (df['ACOS_3d'] > 0) &
        (df['total_clicks_3d'] > 0)]

# Define the function to adjust bids based on definitions
def adjust_bid(row, reason):
    if row['bid'] + 5 > 50:
        new_bid = 50
    else:
        new_bid = row['bid'] + 5
    row['adjusted_bid'] = new_bid
    row['adjustment_reason'] = reason
    return row

records = []

# Fetch unique campaigns
campaigns = df['campaignName'].unique()

for campaign in campaigns:
    campaign_df = df[df['campaignName'] == campaign]
    
    for placement in ['Top of Search on-Amazon', 'Detail Page on-Amazon', 'Other on-Amazon']:
        placement_df = campaign_df[campaign_df['placementClassification'] == placement]
        
        # Check conditions for Definition 1
        condition1 = (placement_df['ACOS_7d'] > 0) & (placement_df['ACOS_7d'] <= 0.4) & \
                     (placement_df['total_clicks_7d'] < placement_df['total_clicks_7d'].max()) & \
                     (placement_df['ACOS_3d'] > 0) & (placement_df['ACOS_3d'] <= 0.4) & \
                     (placement_df['total_clicks_3d'] < placement_df['total_clicks_3d'].max())
        
        if condition1.any():
            selected_df = placement_df[condition1]
            print("\nDefinition 1 matched records:")
            print(selected_df)
            selected_df = selected_df.apply(lambda row: adjust_bid(row, 'Definition 1 matched'), axis=1)
            records.append(selected_df)
            
        # Check conditions for Definition 2
        condition2 = (placement_df['ACOS_7d'] > 0) & (placement_df['ACOS_7d'] <= 0.4) & \
                     (placement_df['total_clicks_7d'] < placement_df['total_clicks_7d'].max()) & \
                     (placement_df['total_cost_3d'] < 6) & \
                     (placement_df['total_clicks_3d'] < placement_df['total_clicks_3d'].max())
        
        if condition2.any():
            selected_df = placement_df[condition2]
            print("\nDefinition 2 matched records:")
            print(selected_df)
            selected_df = selected_df.apply(lambda row: adjust_bid(row, 'Definition 2 matched'), axis=1)
            records.append(selected_df)
                    
if records:
    # Combine all records into one DataFrame
    result_df = pd.concat(records, ignore_index=True)
    
    # Save to the specified output file
    columns_to_save = ['campaignName', 'campaignId', 'placementClassification', 'ACOS_7d', 
                       'ACOS_3d', 'total_clicks_7d', 'total_clicks_3d', 'bid', 'adjusted_bid', 'adjustment_reason']
    result_df[columns_to_save].to_csv(output_file_path, index=False)
    
    print(f"Result saved to {output_file_path}")
else:
    print("No records found that match the criteria.")