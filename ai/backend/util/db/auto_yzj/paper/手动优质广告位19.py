# filename: identify_high_performing_slots.py

import pandas as pd

# Step 1: Load the data
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\预处理.csv'
data = pd.read_csv(file_path)

# Step 2: Filter and process data
def identify_high_performing_slots(data):
    high_performing_slots = []

    # Group by campaign to ensure operations done per campaign
    grouped = data.groupby('campaignId')

    for campaignId, group in grouped:
        # Find the minimum ACOS values within the specified range
        filtered_7d = group[(group['ACOS_7d'] > 0) & (group['ACOS_7d'] <= 0.24)]
        filtered_3d = group[(group['ACOS_3d'] > 0) & (group['ACOS_3d'] <= 0.24)]

        if not filtered_7d.empty and not filtered_3d.empty:
            min_acos_7d = filtered_7d['ACOS_7d'].min()
            min_acos_3d = filtered_3d['ACOS_3d'].min()

            # Get slots with minimum ACOS
            best_slot_7d = filtered_7d[filtered_7d['ACOS_7d'] == min_acos_7d]
            best_slot_3d = filtered_3d[filtered_3d['ACOS_3d'] == min_acos_3d]

            # Ensure the slot with minimum ACOS doesn't have the highest clicks
            for idx, slot in best_slot_7d.iterrows():
                campaign_slots_7d = group['total_clicks_7d'].tolist()
                campaign_slots_3d = group['total_clicks_3d'].tolist()

                if slot['total_clicks_7d'] < max(campaign_slots_7d) and slot['total_clicks_3d'] < max(campaign_slots_3d):
                    high_performing_slots.append(slot)
    
    # Create DataFrame from high-performing slots
    result = pd.DataFrame(high_performing_slots)
    
    # Step 3: Adjust the bid
    result['new_bid'] = result['bid'] * 1.05
    result.loc[result['new_bid'] > result['bid'] * 1.5, 'new_bid'] = result['bid'] * 1.5
    
    return result

# Get the high-performing slots
result_df = identify_high_performing_slots(data)

# Columns we need in the output file
output_columns = [
    'campaignName', 'campaignId', 'placementClassification', 'ACOS_7d', 'ACOS_3d', 
    'total_clicks_7d', 'total_clicks_3d', 'bid', 'new_bid'
]
result_df = result_df[output_columns]

# Step 4: Save result to new CSV
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\提问策略\手动_优质广告位_v1_1_IT_2024-06-19.csv'
result_df.to_csv(output_file_path, index=False)

print(f"Result saved to {output_file_path}")