# filename: check_sku_anomaly.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\商品\预处理1.csv'
data = pd.read_csv(file_path)

# 计算7天和30天内总订单数不为0的每单平均点击次数
data['avg_clicks_per_purchase_7d'] = data.apply(
    lambda row: round(row['total_clicks_7d'] / row['total_purchases7d_7d'], 2) if row['total_purchases7d_7d'] > 0 else None, axis=1)
data['avg_clicks_per_purchase_30d'] = data.apply(
    lambda row: round(row['total_clicks_30d'] / row['total_purchases7d_30d'], 2) if row['total_purchases7d_30d'] > 0 else None, axis=1)

anomalies = []

def check_anomalies(row):
    anomaly_desc = []

    # 检查ACOS波动
    acos_yesterday = row['ACOS_yesterday']
    acos_7d = row['ACOS_7d']
    acos_30d = row['ACOS_30d']

    if pd.notna(acos_yesterday) and (pd.notna(acos_7d) or pd.notna(acos_30d)):
        if pd.notna(acos_7d) and abs(acos_yesterday - acos_7d) / acos_7d > 0.30:
            anomaly_desc.append('ACOS yesterday deviates from 7d average by more than 30%')
        if pd.notna(acos_30d) and abs(acos_yesterday - acos_30d) / acos_30d > 0.30:
            anomaly_desc.append('ACOS yesterday deviates from 30d average by more than 30%')

    # 检查足够点击无销售
    if pd.isna(acos_yesterday):
        avg_clicks_7d = row['avg_clicks_per_purchase_7d']
        avg_clicks_30d = row['avg_clicks_per_purchase_30d']
        clicks_yesterday = row['clicks_yesterday']
        purchases_yesterday = row['purchases7d_yesterday']
        sales_yesterday = row['sales14d_yesterday']

        if purchases_yesterday == 0 and sales_yesterday == 0:
            if (avg_clicks_7d and clicks_yesterday > avg_clicks_7d) or (avg_clicks_30d and clicks_yesterday > avg_clicks_30d):
                anomaly_desc.append('Clicks sufficient but no sales yesterday')

    if anomaly_desc:
        anomalies.append({
            'campaignName': row['campaignName'],
            'adGroupName': row['adGroupName'],
            'advertisedSku': row['advertisedSku'],
            'Anomaly Description': "; ".join(anomaly_desc),
            'ACOS_yesterday': acos_yesterday,
            'ACOS_7d': acos_7d,
            'ACOS_30d': acos_30d,
            'clicks_yesterday': row['clicks_yesterday'],
            'avg_clicks_per_purchase_7d': row['avg_clicks_per_purchase_7d'],
            'avg_clicks_per_purchase_30d': row['avg_clicks_per_purchase_30d'],
        })

data.apply(check_anomalies, axis=1)

# 将结果保存到CSV文件中
output_df = pd.DataFrame(anomalies)
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\商品\提问策略\异常检测_商品_点击量足够但ACOS异常1_v1_0_LAPASA_US_2024-07-09.csv'
output_df.to_csv(output_file_path, index=False)

print(f"Anomaly detection results have been saved to: {output_file_path}")