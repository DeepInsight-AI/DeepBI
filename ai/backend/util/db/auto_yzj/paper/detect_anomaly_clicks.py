# filename: detect_anomaly_clicks.py

import pandas as pd

def main():
    # 1. 数据准备：读取数据集
    file_path = "C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\异常定位检测\\广告位\\预处理.csv"
    df = pd.read_csv(file_path)

    # 2. 计算日均点击量
    df['avg_clicks_7d'] = df['total_clicks_7d'] / 7
    df['avg_clicks_30d'] = df['total_clicks_30d'] / 30

    # 3. 异常判断：波动异常计算和标注
    def detect_anomaly(row):
        anomaly_desc = []
        # Protect against division by zero
        if row['avg_clicks_7d'] != 0:
            if abs(row['clicks_yesterday'] - row['avg_clicks_7d']) / row['avg_clicks_7d'] > 0.3:
                anomaly_desc.append("点击量相较于近7天日均点击量波动超过30%")
        else:
            anomaly_desc.append("近7天日均点击量为零")
        
        if row['avg_clicks_30d'] != 0:
            if abs(row['clicks_yesterday'] - row['avg_clicks_30d']) / row['avg_clicks_30d'] > 0.3:
                anomaly_desc.append("点击量相较于近30天日均点击量波动超过30%")
        else:
            anomaly_desc.append("近30天日均点击量为零")
        
        return ', '.join(anomaly_desc)

    df['anomaly_desc'] = df.apply(detect_anomaly, axis=1)
    anomalies = df[df['anomaly_desc'] != '']

    # 4. 保存结果到CSV文件中
    anomalies_result = anomalies[['campaignId', 'campaignName', 'placementClassification', 'anomaly_desc', 'clicks_yesterday', 'avg_clicks_7d', 'avg_clicks_30d']]
    output_file_path = "C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\异常定位检测\\广告位\\提问策略\\异常检测_广告位_点击量异常_v1_0_LAPASA_US_2024-07-12.csv"
    anomalies_result.to_csv(output_file_path, index=False)

    print("CSV保存成功！路径为：", output_file_path)

if __name__ == "__main__":
    main()