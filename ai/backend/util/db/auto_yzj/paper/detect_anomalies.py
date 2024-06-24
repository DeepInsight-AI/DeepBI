# filename: detect_anomalies.py

import pandas as pd

# Define the file paths
input_csv_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\投放词\预处理1.csv'
output_csv_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\投放词\提问策略\投放词_点击量足够但ACOS异常1_ES_2024-06-12.csv'

# Read the CSV data
df = pd.read_csv(input_csv_path)

# Calculate average clicks per purchase for the past 7 days and 30 days
df['avg_clicks_per_purchase_7d'] = (df['total_clicks_7d'] / df['total_purchases7d_7d']).round(2)
df['avg_clicks_per_purchase_30d'] = (df['total_clicks_30d'] / df['total_purchases7d_30d']).round(2)

# Initialize an empty list to store anomaly results
anomalies = []

# Iterate through each row to identify anomalies
for index, row in df.iterrows():
    anomaly_desc = []

    # Calculate relative changes for ACOS
    if row['ACOS_yesterday'] and row['ACOS_7d']:
        acos_7d_change = abs(row['ACOS_yesterday'] - row['ACOS_7d']) / row['ACOS_7d']
        if acos_7d_change > 0.3:
            anomaly_desc.append("ACOS 波动异常 (7天)")

    if row['ACOS_yesterday'] and row['ACOS_30d']:
        acos_30d_change = abs(row['ACOS_yesterday'] - row['ACOS_30d']) / row['ACOS_30d']
        if acos_30d_change > 0.3:
            anomaly_desc.append("ACOS 波动异常 (30天)")

    # Check for sufficient clicks but no sales
    if pd.isna(row['ACOS_yesterday']):
        if row['clicks_yesterday'] > row['avg_clicks_per_purchase_7d'] and row['sales14d_yesterday'] == 0:
            anomaly_desc.append("昨天点击量足够但无销售 (7天)")

        if row['clicks_yesterday'] > row['avg_clicks_per_purchase_30d'] and row['sales14d_yesterday'] == 0:
            anomaly_desc.append("昨天点击量足够但无销售 (30天)")

    if anomaly_desc:
        anomalies.append([
            row['campaignName'],
            row['adGroupName'],
            row['targeting'], # Assumed advertisedSku
            '; '.join(anomaly_desc),
            row['ACOS_yesterday'],
            row['ACOS_7d'],
            row['ACOS_30d'],
            row['clicks_yesterday'],
            row['avg_clicks_per_purchase_7d'],
            row['avg_clicks_per_purchase_30d']
        ])

# Define the columns of the output CSV
columns = [
    'CampaignName',
    'adGroupName',
    'advertisedSku',
    'Anomaly Description',
    'ACOS_yesterday',
    'ACOS_7d',
    'ACOS_30d',
    'clicks_yesterday',
    'avg_clicks_per_purchase_7d',
    'avg_clicks_per_purchase_30d'
]

# Create a DataFrame for anomalies and save it to CSV
anomalies_df = pd.DataFrame(anomalies, columns=columns)
anomalies_df.to_csv(output_csv_path, index=False)

print("Anomaly detection completed and results saved to:", output_csv_path)