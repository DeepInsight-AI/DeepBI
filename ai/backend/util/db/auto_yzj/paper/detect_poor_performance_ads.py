# filename: detect_poor_performance_ads.py

import pandas as pd
import numpy as np
import os

# 定义文件路径
input_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\预处理.csv'
output_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\提问策略\劣质广告位_FR.csv'

# 读取CSV文件为DataFrame
df = pd.read_csv(input_file)

# 计算平均ACOS值
df['ACOS_7d_avg'] = df.groupby('placementClassification')['ACOS_7d'].transform('mean')
df['ACOS_3d_avg'] = df.groupby('placementClassification')['ACOS_3d'].transform('mean')

# 找到表现差的广告位（定义一）
condition_1 = (df['total_sales14d_7d'] == 0) & (df['total_clicks_7d'] > 0)
df_def1 = df[condition_1].copy()
df_def1['reason'] = '最近7天的总sales为0，但最近7天的总点击数大于0'

# 找到表现差的广告位（定义二）
df_def2 = pd.DataFrame()
for campaign, group in df.groupby('campaignName'):
    if len(group) >= 3:
        acos_diff = group['ACOS_7d'].max() - group['ACOS_7d'].min()
        if (24 < group['ACOS_7d_avg'].mean() < 50) and (acos_diff >= 0.2):
            worst_ad = group.loc[group['ACOS_7d'] == group['ACOS_7d'].max()]
            worst_ad['reason'] = '最近7天的平均ACOS值最大的广告位降低竞价3%'
            df_def2 = pd.concat([df_def2, worst_ad])

# 找到表现差的广告位（定义三）
condition_3 = (df['ACOS_7d_avg'] >= 50)
df_def3 = df[condition_3].copy()
df_def3['reason'] = '最近7天的平均ACOS值大于等于50%'

# 合并所有符合条件的数据
poor_performance_ads = pd.concat([df_def1, df_def2, df_def3])

# 选择输出的列
output_columns = ['campaignName', 'placementClassification', 'ACOS_7d_avg', 'ACOS_3d_avg', 'total_clicks_7d', 'total_clicks_3d', 'reason']
result_df = poor_performance_ads[output_columns]

# 保存为CSV文件
result_df.to_csv(output_file, index=False)

print("Success: The result has been saved to:", output_file)