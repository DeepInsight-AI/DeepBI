# filename: detect_ad_campaign_anomalies.py

import pandas as pd

# Define file paths
input_file_path = 'C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\异常定位检测\\广告活动\\预处理.csv'
output_file_path = 'C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\异常定位检测\\广告活动\\提问策略\\广告活动_预算花费异常_IT_2024-06-11.csv'

# Read the CSV file
df = pd.read_csv(input_file_path)

# Add columns for daily cost averages
df['avg_cost_7d'] = df['total_cost_7d'] / 7
df['avg_cost_30d'] = df['total_cost_30d'] / 30

# Initialize a list to store anomalies
anomalies = []

# Loop through each row to detect anomalies
for index, row in df.iterrows():
    campaign_id = row['campaignId']
    campaign_name = row['campaignName']
    cost_yesterday = row['cost_yesterday']
    budget = row['campaignBudgetAmount']
    avg_cost_7d = row['avg_cost_7d']
    avg_cost_30d = row['avg_cost_30d']
    
    # Initialize an empty list to hold descriptions of anomalies for this campaign
    anomaly_description = []
    
    # Budget exceedence anomaly check
    if (cost_yesterday > budget) and (avg_cost_7d <= 0.5 * budget or avg_cost_30d <= 0.5 * budget):
        anomaly_description.append('超出预算异常：昨天花费超出预算且最近7天或30天日均花费未达到预算50%')
    
    # Cost fluctuation anomaly check
    # Check for non-zero averages before calculating the fluctuation
    if avg_cost_7d > 0 and avg_cost_30d > 0:
        if (abs(cost_yesterday - avg_cost_7d) / avg_cost_7d > 0.3) or (abs(cost_yesterday - avg_cost_30d) / avg_cost_30d > 0.3):
            anomaly_description.append('波动异常：昨天花费波动超过30%')

    # If there are any anomalies, add them to the list
    if anomaly_description:
        anomalies.append([campaign_id, campaign_name, '; '.join(anomaly_description), cost_yesterday, avg_cost_7d, avg_cost_30d])

# Create a DataFrame from the anomalies list
anomalies_df = pd.DataFrame(anomalies, columns=['campaignId', 'campaignName', 'Anomaly Description', 'cost_yesterday', 'avg_cost_7d', 'avg_cost_30d'])

# Save the anomalies to a CSV file
anomalies_df.to_csv(output_file_path, index=False)

print('Anomaly detection completed and results saved to:', output_file_path)