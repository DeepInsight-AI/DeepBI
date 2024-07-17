# filename: detect_click_anomalies_v3.py

import pandas as pd
import os

# 读取文件路径
file_path = "C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/异常定位检测/广告位/预处理.csv"
output_file_path = "C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/异常定位检测/广告位/提问策略/异常检测_广告位_点击量异常_v1_0_LAPASA_IT_2024-07-09.csv"

# 检查文件是否存在
if not os.path.exists(file_path):
    raise FileNotFoundError(f"文件路径不存在: {file_path}")

# 读取数据集
df = pd.read_csv(file_path)

# 确保所需字段存在
required_columns = ['campaignId', 'campaignName', 'placementClassification', 'clicks_yesterday', 'total_clicks_7d', 'total_clicks_30d']
missing_columns = [col for col in required_columns if col not in df.columns]
if missing_columns:
    raise ValueError(f"缺少必要的字段: {missing_columns}")

# 计算日均点击量
df['avg_clicks_7d'] = df['total_clicks_7d'] / 7
df['avg_clicks_30d'] = df['total_clicks_30d'] / 30

# 定义波动异常的阈值
threshold = 0.30

# 查找昨天点击量相对于7天日均点击量波动超过30%的广告活动
df['anomaly_7d'] = abs(df['clicks_yesterday'] - df['avg_clicks_7d']) > threshold * df['avg_clicks_7d']

# 查找昨天点击量相对于30天日均点击量波动超过30%的广告活动
df['anomaly_30d'] = abs(df['clicks_yesterday'] - df['avg_clicks_30d']) > threshold * df['avg_clicks_30d']

# 综合两种异常，判断是否存在波动异常
df['anomaly'] = df[['anomaly_7d', 'anomaly_30d']].any(axis=1)

# 筛选出存在异常的广告活动
anomaly_df = df[df['anomaly']]

# 构建输出DataFrame，包含指定列
output_df = anomaly_df[['campaignId', 'campaignName', 'placementClassification',
                        'clicks_yesterday', 'avg_clicks_7d', 'avg_clicks_30d']]

# 添加异常现象描述
output_df['Anomaly Description'] = "点击量波动超过30%"

# 重命名列以匹配所需的输出格式
output_df.columns = ['异常广告活动ID', '异常广告活动', '异常广告活动位置',
                     '昨天的点击量', '近七天日均点击量', '近30天日均点击量', '异常现象']

# 保存结果到CSV
output_df.to_csv(output_file_path, index=False)

print("异常检测完成并保存到:", output_file_path)