# filename: detect_click_anomalies.py

import pandas as pd

# Load the data
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\投放词\预处理1.csv'
data = pd.read_csv(file_path)

# Calculate daily averages for clicks
data['daily_avg_clicks_7d'] = data['total_clicks_7d'] / 7
data['daily_avg_clicks_30d'] = data['total_clicks_30d'] / 30

# Function to identify anomalies
def detect_anomalies(row):
    if pd.isna(row['CPC_yesterday']):
        return False
    cpc_7d_increase = (row['CPC_yesterday'] > row['CPC_7d'] * 1.3)
    cpc_30d_increase = (row['CPC_yesterday'] > row['CPC_30d'] * 1.3)
    clicks_7d_drop = (row['clicks_yesterday'] < row['daily_avg_clicks_7d'] * 0.7)
    clicks_30d_drop = (row['clicks_yesterday'] < row['daily_avg_clicks_30d'] * 0.7)
    if (cpc_7d_increase or cpc_30d_increase) and (clicks_7d_drop or clicks_30d_drop):
        return True
    return False

# Apply the anomaly detection function
anomalies = data[data.apply(detect_anomalies, axis=1)]

# Add a description for the detected anomalies
anomalies['Anomaly Description'] = 'CPC high but clicks low'

# Select and rename the necessary columns for output
output_columns = ['campaignName', 'adGroupName', 'targeting', 'matchType', 'Anomaly Description', 
                  'clicks_yesterday', 'daily_avg_clicks_7d', 'daily_avg_clicks_30d',
                  'CPC_yesterday', 'CPC_7d', 'CPC_30d']

anomalies = anomalies[output_columns]

# Output the result to a CSV file
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\投放词\提问策略\异常检测_投放词_CPC高但clicks少1_v1_0_LAPASA_US_2024-07-14.csv'
anomalies.to_csv(output_file_path, index=False)

print("Anomaly detection completed and saved to CSV.")