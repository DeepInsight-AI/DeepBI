# filename: process_and_detect_anomalies.py
import pandas as pd

# 读取CSV文件
data = pd.read_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\商品\预处理1.csv')

# 计算7天和30天的每单平均点击次数，处理订单数为0的情况
data['average_clicks_per_order_7d'] = data.apply(lambda row: row['total_clicks_7d'] / row['total_purchases7d_7d'] if row['total_purchases7d_7d'] != 0 else 0, axis=1)
data['average_clicks_per_order_30d'] = data.apply(lambda row: row['total_clicks_30d'] / row['total_purchases7d_30d'] if row['total_purchases7d_30d'] != 0 else 0, axis=1)

# 进行异常检测
data['anomaly_description'] = ''
data.loc[(data['ACOS_yesterday'].isnull()) & (data['clicks_yesterday'] > data['average_clicks_per_order_7d']) & (data['sales14d_yesterday'] == 0), 'anomaly_description'] = '昨天点击量足够但无销售'
data.loc[(data['ACOS_yesterday'] > data['ACOS_7d'] * 1.3) | (data['ACOS_yesterday'] < data['ACOS_7d'] * 0.7), 'anomaly_description'] = 'ACOS波动异常'
data.loc[(data['ACOS_yesterday'] > data['ACOS_30d'] * 1.3) | (data['ACOS_yesterday'] < data['ACOS_30d'] * 0.7), 'anomaly_description'] = 'ACOS波动异常'

# 输出异常结果到CSV文件
anomalies = data[data['anomaly_description'] != ''].reset_index(drop=True)
anomalies.to_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\商品\提问策略\商品_点击量足够但ACOS异常1_FR_2024-05-18_deepseek.csv', index=False)

# 输出异常数据以供检查
print(anomalies)