# filename: find_click_anomalies.py
import pandas as pd

# 读取数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\投放词\预处理1.csv'
data = pd.read_csv(file_path)

# 计算近7天和近30天的日均点击量
data['daily_clicks_7d'] = data['total_clicks_7d'] / 7
data['daily_clicks_30d'] = data['total_clicks_30d'] / 30

# 查找异常点击量的关键词
anomalies = data[
    ((data['CPC_yesterday'] > data['CPC_7d'] * 1.3) |
     (data['CPC_yesterday'] > data['CPC_30d'] * 1.3)) &
    ((data['clicks_yesterday'] < data['daily_clicks_7d'] * 0.7) |
     (data['clicks_yesterday'] < data['daily_clicks_30d'] * 0.7)) &
    data['CPC_yesterday'].notna()
]

# 添加异常现象描述
anomalies['Anomaly Description'] = 'CPC高但clicks少'

# 选择要输出的列
output_columns = [
    'campaignName', 'adGroupName', 'targeting', 'matchType', 'Anomaly Description',
    'clicks_yesterday', 'daily_clicks_7d', 'daily_clicks_30d', 
    'CPC_yesterday', 'CPC_7d', 'CPC_30d'
]
anomalies_output = anomalies[output_columns]

# 输出结果到CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\投放词\提问策略\投放词_CPC高但clicks少1_v1_0_IT_2024-06-23.csv'
anomalies_output.to_csv(output_file_path, index=False)

print(f"异常关键词已经保存到 {output_file_path}")