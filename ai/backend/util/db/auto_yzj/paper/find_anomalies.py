# filename: find_anomalies.py
import pandas as pd

# Step 1: Read the dataset
file_path = 'C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/异常定位检测/投放词/预处理1.csv'
df = pd.read_csv(file_path)

# Step 2 and 3: Calculate average clicks per order for the last 7 and 30 days and identify anomalies
def identify_anomalies(df):
    # Calculate average clicks per order for 7d and 30d
    df['avg_clicks_per_order_7d'] = df['total_clicks_7d'] / df['total_purchases7d_7d']
    df['avg_clicks_per_order_30d'] = df['total_clicks_30d'] / df['total_purchases7d_30d']
    
    anomalies = []

    for index, row in df.iterrows():
        campaign = row['campaignName']
        ad_group = row['adGroupName']
        acos_yesterday = row['ACOS_yesterday']
        avg_acos_7d = row['ACOS_7d']
        avg_acos_30d = row['ACOS_30d']
        clicks_yesterday = row['clicks_yesterday']
        avg_clicks_per_order_7d = row['avg_clicks_per_order_7d']
        avg_clicks_per_order_30d = row['avg_clicks_per_order_30d']

        if pd.isnull(acos_yesterday):
            # Check for "enough clicks but no sales" anomaly
            purchases_yesterday = row['purchases7d_yesterday']
            sales_yesterday = row['sales14d_yesterday']
            if clicks_yesterday > max(avg_clicks_per_order_7d, avg_clicks_per_order_30d) and sales_yesterday == 0:
                anomalies.append([
                    campaign, ad_group, row['targeting'], '昨天点击量足够但无销售', 
                    acos_yesterday, avg_acos_7d, avg_acos_30d, clicks_yesterday, avg_clicks_per_order_7d, avg_clicks_per_order_30d
                ])
        else:
            # Check for ACOS fluctuation anomaly
            if abs(acos_yesterday - avg_acos_7d)/avg_acos_7d > 0.3 or abs(acos_yesterday - avg_acos_30d)/avg_acos_30d > 0.3:
                anomalies.append([
                    campaign, ad_group, row['targeting'], 'ACOS值波动异常', 
                    acos_yesterday, avg_acos_7d, avg_acos_30d, clicks_yesterday, avg_clicks_per_order_7d, avg_clicks_per_order_30d
                ])

    return anomalies

anomalies = identify_anomalies(df)

# Step 4: Output the result to a CSV file
output_path = 'C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/异常定位检测/投放词/提问策略/投放词_点击量足够但ACOS异常1_v1_0_IT_2024-06-23.csv'
output_columns = [
    'CampaignName', 'AdGroupName', 'AdvertisedSku', 'Anomaly Description', 
    'ACOS_yesterday', 'ACOS_7d', 'ACOS_30d', 'clicks_yesterday', 
    'avg_clicks_per_order_7d', 'avg_clicks_per_order_30d'
]
output_df = pd.DataFrame(anomalies, columns=output_columns)
output_df.to_csv(output_path, index=False)

print("Anomalies have been successfully identified and saved.")