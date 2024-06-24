# filename: ad_performance_analysis.py

import pandas as pd

# Load data from CSV
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\预处理.csv'
data = pd.read_csv(file_path)

# Prepare empty list to store results
results = []

# Define function to check and record poor perfoming ads
def check_poor_performance(row):
    # Initialize an empty dictionary for each row to store reasons
    result = {
        'campaignName': row['campaignName'],
        'placementClassification': row['placementClassification'],
        'ACOS_7d': row['ACOS_7d'],
        'ACOS_3d': row['ACOS_3d'],
        'total_clicks_7d': row['total_clicks_7d'],
        'total_clicks_3d': row['total_clicks_3d'],
        'bid_adjustment': 0,
        'reason': []
    }
    
    # Definition One
    if row['total_sales14d_7d'] == 0 and row['total_clicks_7d'] > 0:
        result['bid_adjustment'] = 'Bid set to 0'
        result['reason'].append('Definition One')
    
    # Definition Three
    if row['ACOS_7d'] >= 0.5:
        result['bid_adjustment'] = 'Bid set to 0'
        result['reason'].append('Definition Three')
    
    return result

# Definition Two Specific Functionality
def process_definition_two(group):
    acos_values = group['ACOS_7d']
    
    if acos_values.max() > 0.24 and acos_values.max() < 0.5 and (acos_values.max() - acos_values.min() >= 0.2):
        max_acos_idx = acos_values.idxmax()
        group.loc[max_acos_idx, 'bid_adjustment'] = 'Reduced 3%'
        group.loc[max_acos_idx, 'reason'].append('Definition Two')
    
    return group

# Apply check_poor_performance function to each row and store results
for idx, row in data.iterrows():
    result = check_poor_performance(row)
    results.append(result)

# Convert results list to DataFrame
results_df = pd.DataFrame(results)

# Process for Definition Two
grouped = results_df.groupby('campaignName').apply(process_definition_two)

# Filter only rows with bid adjustments
results_df = grouped[grouped['bid_adjustment'] != 0]

# Save the results to a CSV file
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\提问策略\劣质广告位_FR_2024-5-27.csv'
results_df.to_csv(output_path, index=False)

print("Analysis complete and results saved to", output_path)