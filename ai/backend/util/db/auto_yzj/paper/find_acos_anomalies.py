# filename: find_acos_anomalies.py
import pandas as pd
import numpy as np

# 加载数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\商品\预处理1.csv'
data = pd.read_csv(file_path)

# 昨天的日期
yesterday_date = '2024-05-17'

# 计算7天和30天每单平均点击次数
data['mean_clicks_per_order_7d'] = (data['total_clicks_7d'] / data['total_purchases7d_7d']).replace([np.inf, -np.inf], np.nan).round(2)
data['mean_clicks_per_order_30d'] = (data['total_clicks_30d'] / data['total_purchases7d_30d']).replace([np.inf, -np.inf], np.nan).round(2)

anomalies = []

# 检查每个商品sku
for index, row in data.iterrows():
    acos_yesterday = row['ACOS_yesterday']
    acos_7d = row['ACOS_7d']
    acos_30d = row['ACOS_30d']
    clicks_yesterday = row['clicks_yesterday']
    sales_yesterday = row['sales14d_yesterday']

    # 检查销售额为0且点击量大于每单平均点击次数的情况
    if clicks_yesterday > row['mean_clicks_per_order_7d'] and sales_yesterday == 0:
        anomaly_description = "Yesterday's clicks were enough but no sales."
        anomalies.append([
            row['campaignName'],
            row['adGroupName'],
            row['advertisedSku'],
            anomaly_description,
            acos_yesterday,
            acos_7d,
            acos_30d,
            clicks_yesterday,
            row['mean_clicks_per_order_7d'],
            row['mean_clicks_per_order_30d']
        ])
    
    # 检查ACOS值波动超过30%的情况
    if not pd.isna(acos_yesterday):
        if abs(acos_yesterday - acos_7d) / (acos_7d + 1e-5) > 0.30 or abs(acos_yesterday - acos_30d) / (acos_30d + 1e-5) > 0.30:
            anomaly_description = "ACOS fluctuation exceeds 30%."
            anomalies.append([
                row['campaignName'],
                row['adGroupName'],
                row['advertisedSku'],
                anomaly_description,
                acos_yesterday,
                acos_7d,
                acos_30d,
                clicks_yesterday,
                row['mean_clicks_per_order_7d'],
                row['mean_clicks_per_order_30d']
            ])

# 输出结果到CSV文件
output_df = pd.DataFrame(anomalies, columns=[
    'CampaignName',
    'AdGroupName',
    'AdvertisedSku',
    'Anomaly Description',
    'ACOS_yesterday',
    'ACOS_7d',
    'ACOS_30d',
    'Clicks_yesterday',
    'Mean_clicks_per_order_7d',
    'Mean_clicks_per_order_30d'
])

output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\商品\提问策略\商品_点击量足够但ACOS异常1_FR_2024-05-18.csv'
output_df.to_csv(output_file_path, index=False)

print(f"The anomalies data has been saved to {output_file_path}")