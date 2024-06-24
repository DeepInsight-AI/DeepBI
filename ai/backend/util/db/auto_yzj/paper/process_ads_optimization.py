# filename: process_ads_optimization.py
import pandas as pd
from datetime import datetime, timedelta

# 加载数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\预处理.csv'
df = pd.read_csv(file_path)

# 给每个广告位假设一个初始竞价值，例如1.0
df['current_bid'] = 1.0

# 计算竞价操作结果，并添加到DataFrame
def evaluate_bidding(row):
    if (
        row['ACOS_7d'] > 0 and row['ACOS_7d'] <= 0.24 and
        row['ACOS_3d'] > 0 and row['ACOS_3d'] <= 0.24 and
        row['total_clicks_7d'] != row['max_clicks_7d'] and
        row['total_clicks_3d'] != row['max_clicks_3d']
    ):
        new_bid = row['current_bid'] * 1.05  # 提高5%
        if new_bid > row['current_bid'] * 1.5:  # 限制最高不超过50%
            new_bid = row['current_bid'] * 1.5
        return new_bid
    return row['current_bid']

today = datetime.strptime('2024-05-27', '%Y-%m-%d')
yesterday = (today - timedelta(days=1)).strftime('%Y-%m-%d')

# 加入辅助列，计算广告活动中广告位的最大点击量
df['max_clicks_7d'] = df.groupby('campaignName')['total_clicks_7d'].transform('max')
df['max_clicks_3d'] = df.groupby('campaignName')['total_clicks_3d'].transform('max')

# 计算新的竞价
df['new_bid'] = df.apply(evaluate_bidding, axis=1)

# 过滤出符合定义一条件的广告位
filtered_df = df[
    (df['ACOS_7d'] > 0) & (df['ACOS_7d'] <= 0.24) &
    (df['ACOS_3d'] > 0) & (df['ACOS_3d'] <= 0.24) &
    (df['total_clicks_7d'] != df['max_clicks_7d']) &
    (df['total_clicks_3d'] != df['max_clicks_3d'])
]

output_df = filtered_df[[
    'campaignName', 'placementClassification', 'ACOS_7d', 'ACOS_3d', 'total_clicks_7d', 'total_clicks_3d'
]].copy()

output_df['date'] = yesterday
output_df['竞价操作'] = filtered_df['new_bid']
output_df['原因'] = '最近7天和最近3天ACOS值符合要求且点击次数不是最大'

# 输出结果到CSV文件
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\提问策略\优质广告位_FR_2024-5-27.csv'
output_df.to_csv(output_path, index=False, encoding='utf-8-sig')

print(f'结果已输出到 {output_path}')