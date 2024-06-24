# filename: analyze_anomalies.py

import pandas as pd

# 读取CSV文件
data = pd.read_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\商品\预处理1.csv')

# 确保所有需要的字段都存在
required_fields = [
    'campaignName', 'adGroupName', 'advertisedSku', 'clicks_yesterday',
    'total_clicks_7d', 'total_clicks_30d', 'purchases7d_yesterday',
    'total_purchases7d_7d', 'total_purchases7d_30d', 'ACOS_yesterday',
    'ACOS_7d', 'ACOS_30d'
]
for field in required_fields:
    if field not in data.columns:
        raise ValueError(f"Missing required field: {field}")

# 计算每单平均点击次数
data['avg_clicks_per_order_7d'] = data['total_clicks_7d'] / data['total_purchases7d_7d']
data['avg_clicks_per_order_30d'] = data['total_clicks_30d'] / data['total_purchases7d_30d']

# 异常检测
anomalies = []
for index, row in data.iterrows():
    # 销售额为0检查
    if pd.isna(row['ACOS_yesterday']) and row['purchases7d_yesterday'] == 0 and row['clicks_yesterday'] > row['avg_clicks_per_order_7d']:
        anomalies.append({
            'CampaignName': row['campaignName'],
            'adGroupName': row['adGroupName'],
            'advertisedSku': row['advertisedSku'],
            'Anomaly Description': '昨天点击量足够但无销售',
            'ACOS_yesterday': row['ACOS_yesterday'],
            'ACOS_7d': row['ACOS_7d'],
            'ACOS_30d': row['ACOS_30d'],
            'clicks_yesterday': row['clicks_yesterday'],
            'avg_clicks_per_order_7d': row['avg_clicks_per_order_7d'],
            'avg_clicks_per_order_30d': row['avg_clicks_per_order_30d']
        })
    # ACOS波动检查
    if row['ACOS_7d'] != 0 and row['ACOS_30d'] != 0:
        if abs((row['ACOS_yesterday'] - row['ACOS_7d']) / row['ACOS_7d']) > 0.3 or abs((row['ACOS_yesterday'] - row['ACOS_30d']) / row['ACOS_30d']) > 0.3:
            anomalies.append({
                'CampaignName': row['campaignName'],
                'adGroupName': row['adGroupName'],
                'advertisedSku': row['advertisedSku'],
                'Anomaly Description': 'ACOS波动异常',
                'ACOS_yesterday': row['ACOS_yesterday'],
                'ACOS_7d': row['ACOS_7d'],
                'ACOS_30d': row['ACOS_30d'],
                'clicks_yesterday': row['clicks_yesterday'],
                'avg_clicks_per_order_7d': row['avg_clicks_per_order_7d'],
                'avg_clicks_per_order_30d': row['avg_clicks_per_order_30d']
            })

# 输出结果到CSV文件
result_df = pd.DataFrame(anomalies)
result_df.to_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\商品\提问策略\商品_点击量足够但ACOS异常1_FR_2024-05-13_deepseek.csv', index=False)