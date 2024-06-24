# filename: analyze_and_adjust_bids.py
import pandas as pd
from datetime import datetime, timedelta

# Load data from CSV
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\预处理.csv'
df = pd.read_csv(file_path)

# Define the output columns
output_cols = ['date', 'campaignName', 'placementClassification', 'ACOS_7d', 'ACOS_3d', 
               'total_clicks_7d', 'total_clicks_3d', '竞价调整', '原因']

# Today's date and yesterday's date
today_date = datetime(2024, 5, 27)
yesterday_date = today_date - timedelta(days=1)
yesterday_str = yesterday_date.strftime("%Y-%m-%d")

# Initialize an empty DataFrame for output
output_df = pd.DataFrame(columns=output_cols)

# Function to log the result
def log_result(row, reason, adjustment):
    return pd.Series([yesterday_str, row['campaignName'], row['placementClassification'], row['ACOS_7d'], 
                      row['ACOS_3d'], row['total_clicks_7d'], row['total_clicks_3d'], adjustment, reason], index=output_cols)

# Process Definition One
cond1 = (df['total_sales14d_7d'] == 0) & (df['total_clicks_7d'] > 0)
for _, row in df[cond1].iterrows():
    output_df = pd.concat([output_df, log_result(row, '最近7天sales为0且总点击数大于0', '竞价变为0').to_frame().T], ignore_index=True)

# Process Definition Two
campaign_groups = df.groupby('campaignName')
for campaign_name, group in campaign_groups:
    if len(group) == 3:
        acos_7d_min = group['ACOS_7d'].min()
        acos_7d_max = group['ACOS_7d'].max()
        if all((group['ACOS_7d'] > 0.24) & (group['ACOS_7d'] < 0.5)) and (acos_7d_max - acos_7d_min >= 0.2):
            max_acos_row = group.loc[group['ACOS_7d'].idxmax()]
            output_df = pd.concat([output_df, log_result(max_acos_row, '最近7天平均ACOS最大值和最小值相差>=0.2的广告位降低竞价3%', '降低竞价3%').to_frame().T], ignore_index=True)

# Process Definition Three
for campaign_name, group in campaign_groups:
    if len(group) == 3 and all(group['ACOS_7d'] >= 0.5):
        for _, row in group.iterrows():
            output_df = pd.concat([output_df, log_result(row, '最近7天平均ACOS值>=0.5的广告位', '竞价变为0').to_frame().T], ignore_index=True)

# Save the results to a new CSV file
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\提问策略\劣质广告位_FR.csv'
output_df.to_csv(output_path, index=False, encoding='utf-8')
print(f"Results saved to {output_path}")