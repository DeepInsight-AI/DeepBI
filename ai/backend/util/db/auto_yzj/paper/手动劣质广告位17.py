# filename: adjust_bids.py
import pandas as pd

# Define file paths
input_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\预处理.csv'
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\提问策略\手动_劣质广告位_v1_1_IT_2024-06-17.csv'

# Read the CSV file
data = pd.read_csv(input_file_path)

# Step 2: Data Processing to Identify Underperforming Placements

# Define the required columns for ease of access
cols = ['campaignName', 'campaignId', 'placementClassification', 'total_clicks_7d', 'total_sales14d_7d', 'ACOS_3d', 'ACOS_7d']

# Check if the required columns exist in the dataframe
missing_cols = [col for col in cols if col not in data.columns]
if missing_cols:
    raise ValueError(f"Columns missing in the input data: {missing_cols}")

# Definition 1: 7 days total sales = 0 and 7 days total clicks > 0
def1 = data[(data['total_sales14d_7d'] == 0) & (data['total_clicks_7d'] > 0)]

# Definition 2: Three placements with 7 days average ACOS between 0.24 and 0.5
# and max ACOS - min ACOS >= 0.2, only lower bid for max ACOS placement by 3%
placements = data.groupby(['campaignName', 'campaignId'])
def2 = []
for name, group in placements:
    if group.shape[0] == 3:
        avg_ACOS_7d = group['ACOS_7d']
        if all((avg_ACOS_7d >= 0.24) & (avg_ACOS_7d <= 0.5)):
            max_ACOS = avg_ACOS_7d.max()
            min_ACOS = avg_ACOS_7d.min()
            if max_ACOS - min_ACOS >= 0.2:
                max_ACOS_placement = group.loc[group['ACOS_7d'] == max_ACOS]
                def2.append(max_ACOS_placement.assign(adjust='lower_3%', reason='Def2: High ACOS'))
def2 = pd.concat(def2) if def2 else pd.DataFrame()

# Definition 3: 7 days average ACOS >= 0.5
def3 = data[data['ACOS_7d'] >= 0.5]

# Combine all definitions
result = pd.concat([def1.assign(reason='Def1: No sales in 7 days'),
                    def2,
                    def3.assign(reason='Def3: High ACOS (>= 0.5)')])

# Add additional key information
result['ACOS_3d'] = data.loc[result.index, 'ACOS_3d']
result['recent_clicks_3d'] = data.loc[result.index, 'total_clicks_3d']

# Export the results to a CSV file
result.to_csv(output_file_path, index=False)

print(f"Results have been saved to {output_file_path}")