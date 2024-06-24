# filename: 56743_bidding_adjustment.py

import pandas as pd

# 文件路径
input_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\预处理.csv'
output_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\提问策略\自动_优质广告位_IT_2024-06-08.csv'

# 读取csv数据
df = pd.read_csv(input_file)

# 筛选符合条件的广告位
results = []

for campaign, group in df.groupby('campaignName'):
    if len(group) < 3:
        continue
        
    # 找到最近7天ACOS最小且符合点击数条件的广告位
    min_acos_7d = group.loc[(group['ACOS_7d'] > 0) & (group['ACOS_7d'] <= 0.24)].nsmallest(1, 'ACOS_7d')
    max_clicks_7d = group['total_clicks_7d'].max()
    
    # 找到最近3天ACOS最小且符合点击数条件的广告位
    min_acos_3d = group.loc[(group['ACOS_3d'] > 0) & (group['ACOS_3d'] <= 0.24)].nsmallest(1, 'ACOS_3d')
    max_clicks_3d = group['total_clicks_3d'].max()
    
    if not min_acos_7d.empty and not min_acos_3d.empty:
        bid_7d = min_acos_7d.iloc[0]
        bid_3d = min_acos_3d.iloc[0]
        
        if (bid_7d['total_clicks_7d'] != max_clicks_7d) and (bid_3d['total_clicks_3d'] != max_clicks_3d):
            results.append({
                'campaignName': bid_7d['campaignName'],
                '广告位': bid_7d['placementClassification'],
                '最近7天的平均ACOS值': bid_7d['ACOS_7d'],
                '最近3天的平均ACOS值': bid_3d['ACOS_3d'],
                '最近7天的总点击次数': bid_7d['total_clicks_7d'],
                '最近3天的总点击次数': bid_3d['total_clicks_3d'],
                '对广告位进行竞价操作的原因': '提高竞价5%，最高50%'
            })

# 写入新的csv文件
result_df = pd.DataFrame(results)
result_df.to_csv(output_file, index=False)
print(f"结果已保存到 {output_file}")