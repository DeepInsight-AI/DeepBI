# filename: identify_poor_performance_ads.py
import pandas as pd

# Load the dataset
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\预处理.csv'
df = pd.read_csv(file_path)

# Initialize a list to store the results
results = []

# Definition 1:
def check_definition_1(row):
    return (row['total_sales14d_7d'] == 0) and (row['total_clicks_7d'] > 0)

# Definition 2:
def check_definition_2(campaign_group):
    acoss = campaign_group['ACOS_7d']
    if len(acoss) == 3 and (0.24 < acoss.min() < 0.5) and (0.24 < acoss.max() < 0.5) and (acoss.max() - acoss.min() >= 0.2):
        max_acos_idx = acoss.idxmax()
        campaign_group.at[max_acos_idx, 'bid_reduction'] = -3  # Mark for reduction by 3%
        return campaign_group
    return pd.DataFrame()

# Definition 3:
def check_definition_3(row):
    return row['ACOS_7d'] >= 0.5

# Process the dataset row-wise for Definition 1 and Definition 3
for idx, row in df.iterrows():
    if check_definition_1(row):
        results.append([row['campaignName'], row['placementClassification'], row['ACOS_7d'], row['ACOS_3d'],
                        row['total_clicks_7d'], row['total_clicks_3d'], 'Definition 1: Total sales last 7 days is 0'])
    elif check_definition_3(row):
        results.append([row['campaignName'], row['placementClassification'], row['ACOS_7d'], row['ACOS_3d'],
                        row['total_clicks_7d'], row['total_clicks_3d'], 'Definition 3: Average ACOS last 7 days >= 0.5'])

# Group by campaignName and apply Definition 2
grouped_df = df.groupby('campaignName')
for campaign_name, grouped_data in grouped_df:
    updated_campaign_data = check_definition_2(grouped_data)
    if not updated_campaign_data.empty:
        for _, updated_row in updated_campaign_data.iterrows():
            if 'bid_reduction' in updated_row and updated_row['bid_reduction'] == -3:
                results.append([updated_row['campaignName'], updated_row['placementClassification'], updated_row['ACOS_7d'], 
                                updated_row['ACOS_3d'], updated_row['total_clicks_7d'], updated_row['total_clicks_3d'], 
                                'Definition 2: Reduced bid by 3%'])

# Convert results to DataFrame
results_df = pd.DataFrame(results, columns=['campaignName', 'placementClassification', 'ACOS_7d', 'ACOS_3d', 
                                            'total_clicks_7d', 'total_clicks_3d', 'Reason'])

# Save results to a new CSV file
output_filepath = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\提问策略\手动_劣质广告位_ES_2024-06-05.csv'
results_df.to_csv(output_filepath, index=False)

print("Process completed. Results have been saved to:", output_filepath)