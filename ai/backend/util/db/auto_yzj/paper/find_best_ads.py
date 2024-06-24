# filename: find_best_ads.py
import pandas as pd
import os
from datetime import datetime, timedelta

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\预处理.csv'
df = pd.read_csv(file_path)

# 定义今天的日期
today = datetime(2024, 5, 27)
yesterday = today - timedelta(days=1)

# 过滤不在时间范围内的记录
# df = df[pd.to_datetime(df['date']) == yesterday]

def find_best_ads(df):
    # 创建一个空的DataFrame来保存结果
    result = pd.DataFrame(columns=[
        'campaignName', 'placementClassification', 'ACOS_7d', 'ACOS_3d', 'total_clicks_7d', 'total_clicks_3d', 'reason'
    ])
    
    # 按广告活动分组
    grouped = df.groupby('campaignName')
    
    for name, group in grouped:
        # 找到满足条件1的广告位
        condition1 = group[(group['ACOS_7d'] > 0) & (group['ACOS_7d'] <= 0.24)]
        if not condition1.empty:
            # 获取最小ACOS的广告位和最大点击次数的广告位
            min_acos_7d_placement = condition1.nsmallest(1, 'ACOS_7d')
            max_clicks_7d = group.nlargest(1, 'total_clicks_7d')
            if not min_acos_7d_placement.empty and not max_clicks_7d.empty and min_acos_7d_placement.iloc[0]['placementClassification'] != max_clicks_7d.iloc[0]['placementClassification']:
                reason = 'Condition1: 7d ACOS value is minimum and clicks are not maximum'
                result = pd.concat([result, pd.DataFrame({
                    'campaignName': [name],
                    'placementClassification': [min_acos_7d_placement.iloc[0]['placementClassification']],
                    'ACOS_7d': [min_acos_7d_placement.iloc[0]['ACOS_7d']],
                    'ACOS_3d': [min_acos_7d_placement.iloc[0]['ACOS_3d']],
                    'total_clicks_7d': [min_acos_7d_placement.iloc[0]['total_clicks_7d']],
                    'total_clicks_3d': [min_acos_7d_placement.iloc[0]['total_clicks_3d']],
                    'reason': [reason]
                })], ignore_index=True)
        
        # 找到满足条件2的广告位
        condition2 = group[(group['ACOS_3d'] > 0) & (group['ACOS_3d'] <= 0.24)]
        if not condition2.empty:
            # 获取最小ACOS的广告位和最大点击次数的广告位
            min_acos_3d_placement = condition2.nsmallest(1, 'ACOS_3d')
            max_clicks_3d = group.nlargest(1, 'total_clicks_3d')
            if not min_acos_3d_placement.empty and not max_clicks_3d.empty and min_acos_3d_placement.iloc[0]['placementClassification'] != max_clicks_3d.iloc[0]['placementClassification']:
                reason = 'Condition2: 3d ACOS value is minimum and clicks are not maximum'
                result = pd.concat([result, pd.DataFrame({
                    'campaignName': [name],
                    'placementClassification': [min_acos_3d_placement.iloc[0]['placementClassification']],
                    'ACOS_7d': [min_acos_3d_placement.iloc[0]['ACOS_7d']],
                    'ACOS_3d': [min_acos_3d_placement.iloc[0]['ACOS_3d']],
                    'total_clicks_7d': [min_acos_3d_placement.iloc[0]['total_clicks_7d']],
                    'total_clicks_3d': [min_acos_3d_placement.iloc[0]['total_clicks_3d']],
                    'reason': [reason]
                })], ignore_index=True)
    
    return result

# 查找符合条件的广告位
result_df = find_best_ads(df)

# 保存结果到新的CSV文件
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\提问策略\自动_优质广告位_IT_2024-06-06.csv'
result_df.to_csv(output_path, index=False)

print(f'Results have been saved to {output_path}')