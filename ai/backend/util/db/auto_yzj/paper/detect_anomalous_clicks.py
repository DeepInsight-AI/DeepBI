# filename: detect_anomalous_clicks.py

import pandas as pd

# 定义数据文件路径和输出文件路径
file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\投放词\预处理1.csv"
output_file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\投放词\提问策略\异常检测_投放词_CPC高但clicks少1_v1_0_LAPASA_FR_2024-07-12.csv"

try:
    # 读取数据
    print("正在读取数据文件: ", file_path)
    data = pd.read_csv(file_path)
    print("数据读取成功. 列标题: ", data.columns)

    # 检查必要字段
    required_fields = ['campaignName', 'adGroupName', 'targeting', 'matchType', 'clicks_yesterday', 
                       'total_clicks_7d', 'total_clicks_30d', 'CPC_yesterday', 'CPC_7d', 'CPC_30d']
    missing_fields = [field for field in required_fields if field not in data.columns]
    if missing_fields:
        raise ValueError(f"缺少必要的字段: {missing_fields}")

    # 计算日均点击量
    data['daily_clicks_7d'] = data['total_clicks_7d'] / 7
    data['daily_clicks_30d'] = data['total_clicks_30d'] / 30

    # 筛选异常数据
    anomalies = data[
        ((data['CPC_yesterday'] > data['CPC_7d'] * 1.3) | (data['CPC_yesterday'] > data['CPC_30d'] * 1.3)) &
        ((data['clicks_yesterday'] < data['daily_clicks_7d'] * 0.7) | (data['clicks_yesterday'] < data['daily_clicks_30d'] * 0.7))
    ]

    print(f"找到 {len(anomalies)} 条异常的记录.")

    # 添加异常描述
    anomalies['Anomaly Description'] = 'CPC高但clicks少'

    # 选择输出字段
    output_fields = ['campaignName', 'adGroupName', 'targeting', 'matchType', 'Anomaly Description', 
                     'clicks_yesterday', 'daily_clicks_7d', 'daily_clicks_30d', 'CPC_yesterday', 'CPC_7d', 'CPC_30d']
    output_data = anomalies[output_fields]

    # 保存到CSV文件
    print("正在保存异常数据到文件: ", output_file_path)
    output_data.to_csv(output_file_path, index=False)
    print(f"异常检测结果已保存到: {output_file_path}")

except Exception as e:
    print("执行过程中遇到错误: ", str(e))