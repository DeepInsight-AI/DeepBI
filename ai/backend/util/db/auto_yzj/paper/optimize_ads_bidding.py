# filename: optimize_ads_bidding.py
import pandas as pd

# Step 1: Read the dataset
csv_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\预处理.csv'
data = pd.read_csv(csv_path)

# Step 2: Define conditions based on "Definition One"
cond1 = (data['total_sales14d_7d'] == 0) & (data['total_clicks_7d'] > 0)

# Step 3: Define conditions based on "Definition Three"
cond3 = data['ACOS_7d'] >= 0.5

# Step 4: Find sub-datasets for condition three checking
cond2_campaigns = data.groupby('campaignName').filter(lambda x: len(x) == 3)
cond2 = cond2_campaigns.groupby('campaignName').apply(
    lambda x: (
        x['ACOS_7d'].max() - x['ACOS_7d'].min() >= 0.2 and 
        x['ACOS_7d'].between(0.24, 0.5).all()
    )
).reset_index(name='condition_met').query('condition_met == True')['campaignName']

# Step 5: Collect all poor performing ad placements
poor_ads = data[cond1 | cond3]
for campaign in cond2:
    temp = data[data['campaignName'] == campaign]
    max_acos_idx = temp['ACOS_7d'].idxmax()
    poor_ads = poor_ads.append(temp.loc[max_acos_idx])

# Step 6: Add reason for poor performance
def get_reason(row):
    if row['ACOS_7d'] >= 0.5:
        return 'Definition Three'
    if row['total_sales14d_7d'] == 0 and row['total_clicks_7d'] > 0:
        return 'Definition One'
    campaign_rows = data[data['campaignName'] == row['campaignName']]
    if row.name == campaign_rows['ACOS_7d'].idxmax():
        return 'Definition Two - High ACOS, Reduce 3%'
    return 'Definition Two - Same ACOS but high variation'

poor_ads['Reason'] = poor_ads.apply(get_reason, axis=1)

# Step 7: Select necessary columns for output and save to CSV
output_cols = [
    'campaignName', 'placementClassification', 'ACOS_7d',
    'ACOS_3d', 'total_clicks_7d', 'total_clicks_3d', 'Reason'
]
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\提问策略\手动_劣质广告位_IT_2024-06-06.csv'
poor_ads.to_csv(output_path, columns=output_cols, index=False)

print("The poor performing ads have been successfully saved to the CSV file.")