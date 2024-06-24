# filename: detect_ad_anomalies.py

import pandas as pd

def main():
    # 读取CSV文件路径
    file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告位\预处理.csv"
    
    # 读取CSV文件
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        print(f"Error reading the file: {e}")
        return

    # 计算日均点击量
    try:
        df['daily_clicks_7d'] = df['total_clicks_7d'] / 7
        df['daily_clicks_30d'] = df['total_clicks_30d'] / 30
    except KeyError as e:
        print(f"Missing expected column: {e}")
        return

    # 判断点击量是否存在波动异常
    anomaly_thresh = 0.30
    df['clicks_anomaly_7d'] = (abs(df['clicks_yesterday'] - df['daily_clicks_7d']) / df['daily_clicks_7d']) > anomaly_thresh
    df['clicks_anomaly_30d'] = (abs(df['clicks_yesterday'] - df['daily_clicks_30d']) / df['daily_clicks_30d']) > anomaly_thresh

    # 找出异常广告活动并描述异常现象
    anomalies = df[(df['clicks_anomaly_7d'] | df['clicks_anomaly_30d'])]

    def describe_anomaly(row):
        descriptions = []
        if row['clicks_anomaly_7d']:
            descriptions.append("点击量比近7天日均点击量变化超过30%")
        if row['clicks_anomaly_30d']:
            descriptions.append("点击量比近30天日均点击量变化超过30%")
        return ", ".join(descriptions)

    anomalies['Anomaly_Description'] = anomalies.apply(describe_anomaly, axis=1)
    output_columns = ['campaignId', 'campaignName', 'placementClassification', 'Anomaly_Description', 'clicks_yesterday', 'daily_clicks_7d', 'daily_clicks_30d']
    anomalies = anomalies[output_columns]

    # 输出结果到CSV文件路径
    output_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告位\提问策略\广告位_点击量异常_ES_2024-06-10.csv"
    
    # 将结果保存到CSV文件
    try:
        anomalies.to_csv(output_path, index=False)
    except Exception as e:
        print(f"Error writing the file: {e}")
        return

    print("Anomalies detection and export completed successfully!")

if __name__ == "__main__":
    main()