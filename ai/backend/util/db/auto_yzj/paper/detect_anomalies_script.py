# filename: detect_anomalies_script.py

import pandas as pd
import numpy as np

# CSV file path
file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\商品\预处理1.csv"
output_file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\商品\提问策略\异常检测_商品_点击量足够但ACOS异常1_v1_0_LAPASA_FR_2024-07-10.csv"

# Load CSV file
data = pd.read_csv(file_path)

# Check the necessary columns exist
required_columns = [
    'advertisedSku', 'campaignName', 'adGroupName', 'ACOS_yesterday',
    'ACOS_7d', 'ACOS_30d', 'clicks_yesterday', 'purchases7d_yesterday',
    'sales14d_yesterday', 'total_clicks_7d', 'total_clicks_30d',
    'total_purchases7d_7d', 'total_purchases7d_30d'
]

missing_columns = [col for col in required_columns if col not in data.columns]
if missing_columns:
    raise ValueError(f"Missing columns in CSV data: {missing_columns}")

# Calculate 7 days and 30 days per order average clicks
data['avg_clicks_per_order_7d'] = np.where(data['total_purchases7d_7d'] != 0, data['total_clicks_7d'] / data['total_purchases7d_7d'], 0)
data['avg_clicks_per_order_30d'] = np.where(data['total_purchases7d_30d'] != 0, data['total_clicks_30d'] / data['total_purchases7d_30d'], 0)

# Identify anomalies
anomalies = []

for idx, row in data.iterrows():
    advertisedSku = row['advertisedSku']
    campaignName = row['campaignName']
    adGroupName = row['adGroupName']
    acos_yesterday = row['ACOS_yesterday']
    acos_7d = row['ACOS_7d']
    acos_30d = row['ACOS_30d']
    clicks_yesterday = row['clicks_yesterday']
    sales_yesterday = row['sales14d_yesterday']
    
    avg_clicks_per_order_7d = row['avg_clicks_per_order_7d']
    avg_clicks_per_order_30d = row['avg_clicks_per_order_30d']
    
    # Check for ACOS anomaly
    acos_anomaly = False
    if pd.notna(acos_yesterday):
        if pd.notna(acos_7d) and acos_7d != 0:
            if abs(acos_yesterday - acos_7d) > 0.3 * acos_7d:
                acos_anomaly = True
        if pd.notna(acos_30d) and acos_30d != 0:
            if abs(acos_yesterday - acos_30d) > 0.3 * acos_30d:
                acos_anomaly = True

    # Check for sufficient clicks but no sales anomaly
    sufficient_clicks_anomaly = False
    if sales_yesterday == 0 and ((clicks_yesterday > avg_clicks_per_order_7d and avg_clicks_per_order_7d > 0) or (clicks_yesterday > avg_clicks_per_order_30d and avg_clicks_per_order_30d > 0)):
        sufficient_clicks_anomaly = True

    # Record anomalies
    if acos_anomaly or sufficient_clicks_anomaly:
        anomaly_description = []
        if acos_anomaly:
            anomaly_description.append("ACOS异常")
        if sufficient_clicks_anomaly:
            anomaly_description.append("昨天点击量足够但无销售")

        anomalies.append({
            "CampaignName": campaignName,
            "adGroupName": adGroupName,
            "advertisedSku": advertisedSku,
            "Anomaly Description": "; ".join(anomaly_description),
            "ACOS_yesterday": acos_yesterday,
            "ACOS_7d": acos_7d,
            "ACOS_30d": acos_30d,
            "clicks_yesterday": clicks_yesterday,
            "avg_clicks_per_order_7d": avg_clicks_per_order_7d,
            "avg_clicks_per_order_30d": avg_clicks_per_order_30d
        })

# Output results to CSV
output_df = pd.DataFrame(anomalies)
output_df.to_csv(output_file_path, index=False)