# filename: analyze_clicks_anomaly.py
import pandas as pd

# Load the dataset
file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\投放词\预处理1.csv"
df = pd.read_csv(file_path)

# Rename the columns based on provided field descriptions
df.rename(columns={
    'campaignName': 'CampaignName',
    'adGroupName': 'AdGroupName',
    'targeting': 'Targeting',
    'matchType': 'MatchType',
    'clicks_yesterday': 'ClicksYesterday',
    'total_clicks_7d': 'Clicks7d',
    'total_clicks_30d': 'Clicks30d',
    'CPC_yesterday': 'CPCYesterday',
    'CPC_7d': 'CPC7d',
    'CPC_30d': 'CPC30d'
}, inplace=True)

# Calculate daily average clicks for 7 days and 30 days
df['AvgClicks7d'] = df['Clicks7d'] / 7
df['AvgClicks30d'] = df['Clicks30d'] / 30

# Identify anomalies based on the specified criteria
anomalies = df[
    (
        (df['CPCYesterday'] > df['CPC7d'] * 1.3) | 
        (df['CPCYesterday'] > df['CPC30d'] * 1.3)
    ) & (
        (df['ClicksYesterday'] < df['AvgClicks7d'] * 0.7) | 
        (df['ClicksYesterday'] < df['AvgClicks30d'] * 0.7)
    )
]

# Add anomaly description
anomalies['AnomalyDescription'] = 'CPC高但clicks少'

# Select required columns for the output
output_columns = [
    'CampaignName', 'AdGroupName', 'Targeting', 'MatchType', 'AnomalyDescription',
    'ClicksYesterday', 'AvgClicks7d', 'AvgClicks30d', 'CPCYesterday', 'CPC7d', 'CPC30d'
]
anomalies_output = anomalies[output_columns]

# Save the results to a new CSV file
output_file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\投放词\提问策略\投放词_CPC高但clicks少1_IT_2024-06-08.csv"
anomalies_output.to_csv(output_file_path, index=False)

print("Analysis completed and results saved to:", output_file_path)