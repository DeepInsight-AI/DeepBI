# filename: handle_missing_values.py
import pandas as pd
import numpy as np

# 读取CSV文件
df = pd.read_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\商品\预处理1.csv')

# 检查并处理缺失值和零值
df['total_purchases7d_7d'] = df['total_purchases7d_7d'].replace(0, np.nan)
df['total_purchases7d_30d'] = df['total_purchases7d_30d'].replace(0, np.nan)

# 计算7天和30天的每单平均点击次数
df['average_clicks_7d'] = df['total_clicks_7d'] / df['total_purchases7d_7d']
df['average_clicks_30d'] = df['total_clicks_30d'] / df['total_purchases7d_30d']

# 输出处理后的数据
print("Data after handling missing values:")
print(df[['campaignName', 'adGroupName', 'advertisedSku', 'total_purchases7d_7d', 'total_purchases7d_30d', 'average_clicks_7d', 'average_clicks_30d']].head())