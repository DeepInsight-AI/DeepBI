# filename: detect_ad_anomalies.py
import pandas as pd

# 读取数据
file_path = "C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\异常定位检测\\投放词\\预处理1.csv"
data = pd.read_csv(file_path)

# 数据准备
data['近七天日均点击量'] = data['total_clicks_7d'] / 7
data['近30天日均点击量'] = data['total_clicks_30d'] / 30

# 计算昨天CPC与近七天CPC和近30天CPC的相对变化百分比
data['CPC_相对于近7天变化'] = (data['CPC_yesterday'] - data['CPC_7d']) / data['CPC_7d']
data['CPC_相对于近30天变化'] = (data['CPC_yesterday'] - data['CPC_30d']) / data['CPC_30d']

# 计算昨天点击量与近七天日均点击量和近30天日均点击量的相对变化百分比
data['点击量_相对于近7天日均变化'] = (data['clicks_yesterday'] - data['近七天日均点击量']) / data['近七天日均点击量']
data['点击量_相对于近30天日均变化'] = (data['clicks_yesterday'] - data['近30天日均点击量']) / data['近30天日均点击量']

# 选择CPC不为空的行
filtered_data = data.dropna(subset=['CPC_yesterday', 'CPC_7d', 'CPC_30d'])

# 判断异常
anomalies = filtered_data[
    ((filtered_data['CPC_相对于近7天变化'] > 0.3) & (filtered_data['点击量_相对于近7天日均变化'] < -0.3)) |
    ((filtered_data['CPC_相对于近30天变化'] > 0.3) & (filtered_data['点击量_相对于近30天日均变化'] < -0.3))
]

anomalies['Anomaly Description'] = 'CPC高但clicks少'

# 输出到CSV
output_columns = [
    'campaignName', 'adGroupName', 'targeting', 'matchType', 'Anomaly Description',
    'clicks_yesterday', '近七天日均点击量', '近30天日均点击量', 'CPC_yesterday', 'CPC_7d', 'CPC_30d'
]

output_file_path = "C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\异常定位检测\\投放词\\提问策略\\异常检测_投放词_CPC高但clicks少1_v1_0_LAPASA_US_2024-07-09.csv"
anomalies.to_csv(output_file_path, columns=output_columns, index=False)

print("异常检测完成，结果已保存至指定文件。")