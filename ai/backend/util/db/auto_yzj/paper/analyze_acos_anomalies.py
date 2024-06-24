# filename: analyze_acos_anomalies.py

import pandas as pd

# 加载CSV文件
data_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\投放词\预处理1.csv'
data = pd.read_csv(data_path)

# 字段重命名
field_mapping = {
    "campaignName": "CampaignName",
    "adGroupName": "AdGroupName",
    "targeting": "advertisedSku",
    "clicks_yesterday": "clicks_yesterday",
    "total_clicks_7d": "clicks_7days",
    "total_clicks_30d": "clicks_30days",
    "purchases7d_yesterday": "purchases7d_yesterday",
    "total_purchases7d_7d": "purchases_7days",
    "total_purchases7d_30d": "purchases_30days",
    "ACOS_yesterday": "ACOS_yesterday",
    "ACOS_7d": "ACOS_7days",
    "ACOS_30d": "ACOS_30days",
    "sales14d_yesterday": "sales14d_yesterday"
}
data.rename(columns=field_mapping, inplace=True)

# 计算平均点击次数
data['avg_clicks_per_order_7days'] = data['clicks_7days'] / data['purchases_7days']
data['avg_clicks_per_order_30days'] = data['clicks_30days'] / data['purchases_30days']

# 定义异常检测逻辑
anomalies = []

for index, row in data.iterrows():
    anomaly_description = []
    
    # ACOS 波动异常检测
    if not pd.isna(row['ACOS_yesterday']) and not pd.isna(row['ACOS_7days']) and not pd.isna(row['ACOS_30days']):
        acos_diff_7days = abs(row['ACOS_yesterday'] - row['ACOS_7days']) / row['ACOS_7days']
        acos_diff_30days = abs(row['ACOS_yesterday'] - row['ACOS_30days']) / row['ACOS_30days']
        
        if acos_diff_7days > 0.3 or acos_diff_30days > 0.3:
            anomaly_description.append("ACOS波动异常")

    # 足够点击无销售异常检测
    if pd.isna(row['ACOS_yesterday']):
        if (row['clicks_yesterday'] > row['avg_clicks_per_order_7days'] and row['sales14d_yesterday'] == 0) or \
           (row['clicks_yesterday'] > row['avg_clicks_per_order_30days'] and row['sales14d_yesterday'] == 0):
            anomaly_description.append("昨天点击量足够但无销售")
    
    if anomaly_description:
        anomalies.append([
            row['CampaignName'],
            row['AdGroupName'],
            row['advertisedSku'],
            "; ".join(anomaly_description),
            row['ACOS_yesterday'],
            row['ACOS_7days'],
            row['ACOS_30days'],
            row['clicks_yesterday'],
            round(row['avg_clicks_per_order_7days'], 2),
            round(row['avg_clicks_per_order_30days'], 2)
        ])

# 构造异常结果的DataFrame
anomalies_df = pd.DataFrame(anomalies, columns=[
    'CampaignName', 'AdGroupName', 'advertisedSku', 'Anomaly Description',
    'ACOS_yesterday', 'ACOS_7days', 'ACOS_30days', 'clicks_yesterday',
    'avg_clicks_per_order_7days', 'avg_clicks_per_order_30days'
])

# 输出结果到CSV
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\投放词\提问策略\投放词_点击量足够但ACOS异常1_ES_2024-06-07.csv'
anomalies_df.to_csv(output_path, index=False)
print(f"Results have been saved to {output_path}.")