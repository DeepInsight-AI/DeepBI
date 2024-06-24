# filename: check_acos_anomalies.py

import pandas as pd

def main():
    # 加载数据
    file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\商品\预处理1.csv'
    df = pd.read_csv(file_path)

    # 计算7天的每单平均点击次数和30天的每单平均点击次数
    df['avg_clicks_per_order_7d'] = df['total_clicks_7d'] / df['total_purchases7d_7d']
    df['avg_clicks_per_order_30d'] = df['total_clicks_30d'] / df['total_purchases7d_30d']

    # 处理可能出现的除零问题
    df['avg_clicks_per_order_7d'] = df['avg_clicks_per_order_7d'].replace([float('inf'), -float('inf')], 0).fillna(0)
    df['avg_clicks_per_order_30d'] = df['avg_clicks_per_order_30d'].replace([float('inf'), -float('inf')], 0).fillna(0)

    # 用于存储异常的列表
    anomalies = []

    # 遍历每个商品sku
    for index, row in df.iterrows():
        acos_yesterday = row['ACOS_yesterday']
        acos_7d = row['ACOS_7d']
        acos_30d = row['ACOS_30d']
        clicks_yesterday = row['clicks_yesterday']
        sales14d_yesterday = row['sales14d_yesterday']
        avg_clicks_per_order_7d = row['avg_clicks_per_order_7d']
        avg_clicks_per_order_30d = row['avg_clicks_per_order_30d']
        
        # 初始化异常描述列表
        anomaly_descriptions = []
        
        # ACOS波动检查
        if pd.notna(acos_yesterday) and pd.notna(acos_7d) and pd.notna(acos_30d):
            if acos_7d != 0 and abs(acos_yesterday - acos_7d) / acos_7d > 0.3:
                anomaly_descriptions.append('ACOS 7d 波动异常')
            if acos_30d != 0 and abs(acos_yesterday - acos_30d) / acos_30d > 0.3:
                anomaly_descriptions.append('ACOS 30d 波动异常')
        
        # 销售额为0检查
        if pd.isna(acos_yesterday) and clicks_yesterday > avg_clicks_per_order_7d and sales14d_yesterday == 0:
            anomaly_descriptions.append('昨日点击量足够但无销售 (7d)')
        if pd.isna(acos_yesterday) and clicks_yesterday > avg_clicks_per_order_30d and sales14d_yesterday == 0:
            anomaly_descriptions.append('昨日点击量足够但无销售 (30d)')
        
        # 如果存在异常，添加到异常列表
        if anomaly_descriptions:
            anomalies.append({
                'campaignName': row['campaignName'],
                'adGroupName': row['adGroupName'],
                'advertisedSku': row['advertisedSku'],
                'Anomaly Description': '; '.join(anomaly_descriptions),
                'ACOS_yesterday': acos_yesterday,
                'ACOS_7d': acos_7d,
                'ACOS_30d': acos_30d,
                'clicks_yesterday': clicks_yesterday,
                'avg_clicks_per_order_7d': avg_clicks_per_order_7d,
                'avg_clicks_per_order_30d': avg_clicks_per_order_30d
            })

    # 将异常保存到一个新的CSV文件中
    output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\商品\提问策略\商品_点击量足够但ACOS异常1_ES_2024-06-12.csv'
    anomalies_df = pd.DataFrame(anomalies)
    anomalies_df.to_csv(output_file_path, index=False)

    print("Anomalies have been successfully detected and saved to the CSV file.")

if __name__ == "__main__":
    main()