# filename: analyze_ad_clicks.py

import pandas as pd

# Step 1: Read the CSV file
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告位\预处理.csv'
df = pd.read_csv(file_path)

# Step 2: Calculate average clicks for 7 days and 30 days
df['avg_clicks_7d'] = df['total_clicks_7d'] / 7
df['avg_clicks_30d'] = df['total_clicks_30d'] / 30

# Step 3: Identify anomalies
anomalies = df[
    ((df['clicks_yesterday'] > df['avg_clicks_7d'] * 1.3) | (df['clicks_yesterday'] < df['avg_clicks_7d'] * 0.7)) |
    ((df['clicks_yesterday'] > df['avg_clicks_30d'] * 1.3) | (df['clicks_yesterday'] < df['avg_clicks_30d'] * 0.7))
]

# Step 4: Add anomaly description
def describe_anomalies(row):
    anomalies = []
    if row['clicks_yesterday'] > row['avg_clicks_7d'] * 1.3:
        anomalies.append("Clicks yesterday > 130% of 7-day avg")
    if row['clicks_yesterday'] < row['avg_clicks_7d'] * 0.7:
        anomalies.append("Clicks yesterday < 70% of 7-day avg")
    if row['clicks_yesterday'] > row['avg_clicks_30d'] * 1.3:
        anomalies.append("Clicks yesterday > 130% of 30-day avg")
    if row['clicks_yesterday'] < row['avg_clicks_30d'] * 0.7:
        anomalies.append("Clicks yesterday < 70% of 30-day avg")
    return "; ".join(anomalies)

anomalies['Anomaly Description'] = anomalies.apply(describe_anomalies, axis=1)

# Selecting required columns
output_df = anomalies[[
    'campaignId', 'campaignName', 'placementClassification', 'Anomaly Description',
    'clicks_yesterday', 'avg_clicks_7d', 'avg_clicks_30d'
]]

# Step 5: Output result to csv
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告位\提问策略\广告位_点击量异常_IT_2024-06-11.csv'
output_df.to_csv(output_file_path, index=False)

print(f"Anomalies analysis complete. Results saved to {output_file_path}")