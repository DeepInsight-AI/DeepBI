# filename: anomaly_detection_acos.py

import pandas as pd

# Step 1: Load the dataset
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\商品\预处理1.csv'
df = pd.read_csv(file_path)

# Step 2: Calculate 7-day and 30-day per-order average clicks
def calculate_avg_clicks(row):
    avg_clicks_7d = None if row['total_purchases7d_7d'] == 0 else row['total_clicks_7d'] / row['total_purchases7d_7d']
    avg_clicks_30d = None if row['total_purchases7d_30d'] == 0 else row['total_clicks_30d'] / row['total_purchases7d_30d']
    return avg_clicks_7d, avg_clicks_30d

df[['avg_clicks_7d', 'avg_clicks_30d']] = df.apply(lambda row: pd.Series(calculate_avg_clicks(row)), axis=1)

# Step 3: Identify anomalies
anomalies = []

for index, row in df.iterrows():
    acos_yesterday = row['ACOS_yesterday']
    acos_7d = row['ACOS_7d']
    acos_30d = row['ACOS_30d']
    clicks_yesterday = row['clicks_yesterday']
    sales_yesterday = row['sales14d_yesterday']

    # Check if it's an ACOS wave anomaly
    if acos_yesterday is not None and acos_7d is not None and acos_30d is not None:
        if abs(acos_yesterday - acos_7d) / acos_7d > 0.30 or abs(acos_yesterday - acos_30d) / acos_30d > 0.30:
            anomalies.append([
                row['campaignName'], row['adGroupName'], row['advertisedSku'],
                'ACOS 波动异常', acos_yesterday, acos_7d, acos_30d,
                clicks_yesterday, row['avg_clicks_7d'], row['avg_clicks_30d']
            ])

    # Check if it's a sales-zero anomaly
    avg_clicks_7d = row['avg_clicks_7d']
    avg_clicks_30d = row['avg_clicks_30d']
    if acos_yesterday is None and sales_yesterday == 0 and (avg_clicks_7d is not None and avg_clicks_30d is not None):
        if clicks_yesterday > avg_clicks_7d or clicks_yesterday > avg_clicks_30d:
            anomalies.append([
                row['campaignName'], row['adGroupName'], row['advertisedSku'],
                '昨天点击量足够但无销售', acos_yesterday, acos_7d, acos_30d,
                clicks_yesterday, avg_clicks_7d, avg_clicks_30d
            ])

# Step 4: Save anomalies to a CSV file
anomalies_df = pd.DataFrame(anomalies, columns=[
    'campaignName', 'adGroupName', 'advertisedSku', 'Anomaly Description',
    'ACOS_yesterday', 'ACOS_7d', 'ACOS_30d', 'clicks_yesterday', 
    'avg_clicks_7d', 'avg_clicks_30d'
])
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\商品\提问策略\商品_点击量足够但ACOS异常1_IT_2024-06-11.csv'
anomalies_df.to_csv(output_path, index=False)

print(f"Anomaly detection completed. Results saved to {output_path}.")