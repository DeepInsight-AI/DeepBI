# filename: detect_and_output_anomalies.py
import pandas as pd

# 加载数据
data = pd.read_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\商品\预处理1.csv')

# 计算7天和30天的每单平均点击次数
data['average_clicks_per_order_7d'] = data['total_clicks_7d'] / data['total_purchases7d_7d']
data['average_clicks_per_order_30d'] = data['total_clicks_30d'] / data['total_purchases7d_30d']

# 定义异常检测函数
def detect_anomalies(row):
    # ACOS波动异常检测
    if row['ACOS_yesterday'] is not None:
        if row['ACOS_7d'] != 0:
            change_7d = abs((row['ACOS_yesterday'] - row['ACOS_7d']) / row['ACOS_7d'])
        else:
            change_7d = 0
        if row['ACOS_30d'] != 0:
            change_30d = abs((row['ACOS_yesterday'] - row['ACOS_30d']) / row['ACOS_30d'])
        else:
            change_30d = 0
        if change_7d > 0.3 or change_30d > 0.3:
            return f"ACOS波动异常: 7天变化{change_7d*100}%, 30天变化{change_30d*100}%"
    
    # 点击量足够但无销售异常检测
    if row['ACOS_yesterday'] is None and row['purchases7d_yesterday'] == 0 and row['clicks_yesterday'] > row['average_clicks_per_order_7d']:
        return "点击量足够但无销售异常"
    
    return None

# 应用异常检测函数
data['Anomaly Description'] = data.apply(detect_anomalies, axis=1)

# 过滤出异常记录
anomalies = data[data['Anomaly Description'].notnull()]

# 输出结果到CSV文件
anomalies[['campaignName', 'adGroupName', 'advertisedSku', 'Anomaly Description', 'ACOS_yesterday', 'ACOS_7d', 'ACOS_30d', 'clicks_yesterday', 'average_clicks_per_order_7d', 'average_clicks_per_order_30d']].to_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\商品\提问策略\商品_点击量足够但ACOS异常1_FR_2024-05-15_deepseek.csv', index=False)

# 打印异常记录
print(anomalies)