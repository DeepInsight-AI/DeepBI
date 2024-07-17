# filename: update_bid.py

import os
import pandas as pd

# Load the CSV file
file_path = "C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/手动sp广告/广告位优化/预处理.csv"
df = pd.read_csv(file_path)

# Ensure 'bid' column is in float format for arithmetic operations
df['bid'] = df['bid'].astype(float)

# Display statistics for ACOS columns before filtering
print("ACOS_7d Statistics:")
print(df['ACOS_7d'].describe())
print("\nACOS_3d Statistics:")
print(df['ACOS_3d'].describe())

# Define function to return updated rows based on conditions
def update_bid(df):
    updated_rows = []
    
    def log_filtered_count(step_desc, filtered_df):
        print(f"{step_desc}: {len(filtered_df)} rows")
        print(filtered_df[['campaignId', 'placementClassification', 'ACOS_7d', 'ACOS_3d', 'total_clicks_7d', 'total_clicks_3d', 'total_cost_3d']])
        
    def debug_intermediate_steps(campaign_data):
        cond_acos_7d = campaign_data[(campaign_data['ACOS_7d'] > 0) & (campaign_data['ACOS_7d'] <= 0.24)]
        log_filtered_count("满足ACOS_7d条件广告位", cond_acos_7d)
        
        cond_acos_3d = cond_acos_7d[(cond_acos_7d['ACOS_3d'] > 0) & (cond_acos_7d['ACOS_3d'] <= 0.24)]
        log_filtered_count("满足ACOS_3d条件广告位", cond_acos_3d)
        
        max_clicks_7d = cond_acos_3d['total_clicks_7d'].max()
        if max_clicks_7d != 0:
            threshold_7d = max_clicks_7d * 0.9  # Top 10% filter
        else:
            threshold_7d = 0  # No filtering if max_clicks_7d is zero
        cond_clicks_7d = cond_acos_3d[cond_acos_3d['total_clicks_7d'] < threshold_7d]
        log_filtered_count("满足7天点击数条件广告位", cond_clicks_7d)
        
        max_clicks_3d = cond_clicks_7d['total_clicks_3d'].max()
        if max_clicks_3d != 0:
            threshold_3d = max_clicks_3d * 0.9  # Top 10% filter
        else:
            threshold_3d = 0  # No filtering if max_clicks_3d is zero
        cond_clicks_3d = cond_clicks_7d[cond_clicks_7d['total_clicks_3d'] < threshold_3d]
        log_filtered_count("满足3天点击数条件广告位", cond_clicks_3d)
        
        return cond_clicks_3d

    for campaign_id in df['campaignId'].unique():
        campaign_data = df[df['campaignId'] == campaign_id]
        
        for placement in ['Top of Search on-Amazon', 'Detail Page on-Amazon', 'Other on-Amazon']:
            placement_data = campaign_data[campaign_data['placementClassification'] == placement]
            
            if placement_data.empty:
                continue

            debug_intermediate_steps(placement_data)

            min_acos_7d = placement_data['ACOS_7d'].min()
            min_acos_3d = placement_data['ACOS_3d'].min()
            max_clicks_7d = placement_data['total_clicks_7d'].max()
            max_clicks_3d = placement_data['total_clicks_3d'].max()

            for index, row in placement_data.iterrows():
                condition_1 = (row['ACOS_3d'] == min_acos_3d and row['ACOS_7d'] == min_acos_7d and
                               row['ACOS_3d'] > 0 and row['ACOS_3d'] <= 0.24 and
                               row['ACOS_7d'] > 0 and row['ACOS_7d'] <= 0.24 and
                               row['total_clicks_7d'] < threshold_7d and
                               row['total_clicks_3d'] < threshold_3d)

                condition_2 = (row['ACOS_7d'] == min_acos_7d and
                               row['ACOS_7d'] > 0 and row['ACOS_7d'] <= 0.24 and
                               row['total_clicks_7d'] < threshold_7d and
                               row['total_cost_3d'] < 4 and
                               row['total_clicks_3d'] < threshold_3d)

                if condition_1 or condition_2:
                    row['bid'] = min(row['bid'] + 5, 50)
                    row['reason'] = "定义一" if condition_1 else "定义二"
                    updated_rows.append(row)
                    log_filtered_count(f"Selected row for campaign: {campaign_id}, placement: {placement}", pd.DataFrame([row]))

    return pd.DataFrame(updated_rows)

# Perform the bid update
df_filtered = update_bid(df)

# Check if reason column is added correctly
if 'reason' not in df_filtered.columns:
    df_filtered['reason'] = None

# Define output columns
output_columns = [
    'campaignName',
    'campaignId',
    'placementClassification',
    'ACOS_7d',
    'ACOS_3d',
    'total_clicks_7d',
    'total_clicks_3d',
    'bid',
    'reason'
]

# Ensure all required columns exist
missing_columns = set(output_columns) - set(df_filtered.columns)
for col in missing_columns:
    df_filtered[col] = None

df_output = df_filtered[output_columns]

# Ensure the output directory exists
output_dir = "C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/提问策略"
os.makedirs(output_dir, exist_ok=True)

# Save to a CSV file
output_file_path = os.path.join(output_dir, "手动_优质广告位_v1_1_LAPASA_FR_2024-07-14.csv")
df_output.to_csv(output_file_path, index=False)

# Verify successful save
print(f"CSV file saved to {output_file_path} with {len(df_output)} records.")