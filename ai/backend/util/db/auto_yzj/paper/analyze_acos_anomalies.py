# filename: analyze_acos_anomalies.py

import pandas as pd

# Step 1: Read CSV
csv_file = 'C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\异常定位检测\\广告位\\预处理.csv'
df = pd.read_csv(csv_file)

# Step 2: Data Cleaning
df['ACOS_yesterday'].fillna(pd.NA, inplace=True)
df['ACOS_7d'].fillna(pd.NA, inplace=True)
df['ACOS_30d'].fillna(pd.NA, inplace=True)

# Step 3: Anomaly Detection
anomalies = []

for index, row in df.iterrows():
    # Yesterday ACOS comparison with last 7 days and 30 days
    try:
        acos_yesterday = float(row['ACOS_yesterday'])
        acos_7d = float(row['ACOS_7d'])
        acos_30d = float(row['ACOS_30d'])
        
        if (abs(acos_yesterday - acos_7d) / acos_7d > 0.3):
            anomalies.append([
                row['campaignId'],
                row['campaignName'],
                row['placementClassification'],
                'ACOS deviation > 30% from last 7 days average',
                acos_yesterday,
                acos_7d,
                acos_30d
            ])
        elif (abs(acos_yesterday - acos_30d) / acos_30d > 0.3):
            anomalies.append([
                row['campaignId'],
                row['campaignName'],
                row['placementClassification'],
                'ACOS deviation > 30% from last 30 days average',
                acos_yesterday,
                acos_7d,
                acos_30d
            ])
    except (ValueError, TypeError):
        # Handling NaN values in ACOS_yesterday
        acos_7d = row['ACOS_7d']
        acos_30d = row['ACOS_30d']
        if pd.isna(row['ACOS_yesterday']):
            if acos_7d and (acos_7d < 0.25 or acos_7d < 0.20):
                anomalies.append([
                    row['campaignId'],
                    row['campaignName'],
                    row['placementClassification'],
                    'ACOS missing yesterday but past week had good ACOS',
                    row['ACOS_yesterday'],
                    acos_7d,
                    acos_30d
                ])
            elif acos_30d and (acos_30d < 0.25 or acos_30d < 0.20):
                anomalies.append([
                    row['campaignId'],
                    row['campaignName'],
                    row['placementClassification'],
                    'ACOS missing yesterday but past month had good ACOS',
                    row['ACOS_yesterday'],
                    acos_7d,
                    acos_30d
                ])

# Step 4: Output results to CSV
output_file = 'C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\异常定位检测\\广告位\\提问策略\\异常检测_广告位_ACOS异常_v1_0_LAPASA_ES_2024-07-13.csv'
anomalies_df = pd.DataFrame(anomalies, columns=[
    'campaignId', 
    'campaignName', 
    'placementClassification', 
    'Anomaly Description',
    'ACOS_yesterday',
    'ACOS_7d',
    'ACOS_30d'
])
anomalies_df.to_csv(output_file, index=False)

print("Anomaly detection completed. Results saved to:", output_file)