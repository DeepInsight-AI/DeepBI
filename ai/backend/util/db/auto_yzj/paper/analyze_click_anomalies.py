# filename: analyze_click_anomalies.py
import pandas as pd

# 读取数据
data = pd.read_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\投放词\预处理1.csv')

# 计算日均点击量
data['avg_clicks_7d'] = data['total_clicks_7d'] / 7
data['avg_clicks_30d'] = data['total_clicks_30d'] / 30

# 异常判断
data['CPC_change_7d'] = data['CPC_yesterday'].sub(data['CPC_7d']).div(data['CPC_7d']).fillna(0)
data['CPC_change_30d'] = data['CPC_yesterday'].sub(data['CPC_30d']).div(data['CPC_30d']).fillna(0)
data['clicks_drop_7d'] = data['avg_clicks_7d'].sub(data['clicks_yesterday']).div(data['avg_clicks_7d']).fillna(0)
data['clicks_drop_30d'] = data['avg_clicks_30d'].sub(data['clicks_yesterday']).div(data['avg_clicks_30d']).fillna(0)

# 筛选异常数据
anomalies = data[(data['CPC_change_7d'] > 0.30) & (data['clicks_drop_7d'] > 0.30) | (data['CPC_change_30d'] > 0.30) & (data['clicks_drop_30d'] > 0.30)]

# 创建异常描述列
anomalies['Anomaly Description'] = 'CPC高但clicks少'

# 输出结果
anomalies[['campaignName', 'adGroupName', 'targeting', 'matchType', 'Anomaly Description', 'clicks_yesterday', 'avg_clicks_7d', 'avg_clicks_30d', 'CPC_yesterday', 'CPC_7d', 'CPC_30d']].to_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\投放词\提问策略\投放词_CPC高但clicks少1_FR_2024-05-18_deepseek.csv', index=False)