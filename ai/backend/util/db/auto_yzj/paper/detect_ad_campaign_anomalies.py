# filename: detect_ad_campaign_anomalies.py

import pandas as pd

# 读取数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告活动\预处理.csv'
df = pd.read_csv(file_path)

# 获取昨天日期
import datetime
yesterday = datetime.date(2024, 5, 18) - datetime.timedelta(days=1)

# 计算日均花费
df['avg_cost_7d'] = df['total_cost_7d'] / 7
df['avg_cost_30d'] = df['total_cost_30d'] / 30

# 定义一个函数判断异常
def check_anomalies(row):
    anomalies = []
    
    # 超出预算异常
    if row['cost_yesterday'] > row['campaignBudgetAmount'] and (
        row['avg_cost_7d'] < 0.5 * row['campaignBudgetAmount'] or row['avg_cost_30d'] < 0.5 * row['campaignBudgetAmount']):
        anomalies.append('超出预算异常')
    
    # 波动异常（相对近7天日均花费）
    if row['avg_cost_7d'] != 0 and abs(row['cost_yesterday'] - row['avg_cost_7d']) / row['avg_cost_7d'] > 0.3:
        anomalies.append('波动异常（相对近7天日均花费）')
    
    # 波动异常（相对近30天日均花费）
    if row['avg_cost_30d'] != 0 and abs(row['cost_yesterday'] - row['avg_cost_30d']) / row['avg_cost_30d'] > 0.3:
        anomalies.append('波动异常（相对近30天日均花费）')
    
    return ', '.join(anomalies)

# 将异常添加到数据框
df['Anomaly Description'] = df.apply(check_anomalies, axis=1)

# 过滤只包含异常的广告活动
df_anomalies = df[df['Anomaly Description'] != '']

# 选择需要的列输出
df_anomalies_output = df_anomalies[[
    'campaignId',
    'campaignName',
    'Anomaly Description',
    'cost_yesterday',
    'avg_cost_7d',
    'avg_cost_30d'
]]

# 保存结果
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告活动\提问策略\异常检测_广告活动_预算花费异常_v1_0_LAPASA_FR_2024-07-11.csv'
df_anomalies_output.to_csv(output_file_path, index=False)

print("异常检测完成，结果保存在：", output_file_path)