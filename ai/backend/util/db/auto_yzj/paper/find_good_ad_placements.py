# filename: find_good_ad_placements.py

import pandas as pd
from datetime import date

# 测试日期
today = date(2024, 5, 27)

# 文件路径
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\预处理.csv'
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\提问策略\优质广告位_FR_2024-5-27.csv'

# 读取CSV文件
df = pd.read_csv(file_path)

# 筛选满足条件1和条件2的广告位
results = []
for campaign in df['campaignName'].unique():
    campaign_df = df[df['campaignName'] == campaign]
    
    for placement in campaign_df['placementClassification'].unique():
        placement_df = campaign_df[campaign_df['placementClassification'] == placement]
        
        if len(placement_df) >= 3:
            placement_df_sorted_7d = placement_df.sort_values('ACOS_7d')
            placement_df_sorted_3d = placement_df.sort_values('ACOS_3d')

            idx_7d_min = placement_df_sorted_7d.index[0]
            idx_3d_min = placement_df_sorted_3d.index[0]
            total_clicks_max_7d = placement_df['total_clicks_7d'].max()
            total_clicks_max_3d = placement_df['total_clicks_3d'].max()
            
            min_acos_7d = placement_df_sorted_7d.iloc[0]['ACOS_7d']
            min_acos_3d = placement_df_sorted_3d.iloc[0]['ACOS_3d']

            if (0 < min_acos_7d <= 0.24) and (placement_df_sorted_7d.iloc[0]['total_clicks_7d'] != total_clicks_max_7d):
                if (0 < min_acos_3d <= 0.24) and (placement_df_sorted_3d.iloc[0]['total_clicks_3d'] != total_clicks_max_3d):
                    result = {
                        "date": today,
                        "campaignName": campaign,
                        "广告位": placement,
                        "最近7天的平均ACOS值": min_acos_7d,
                        "最近3天的平均ACOS值": min_acos_3d,
                        "最近7天的总点击次数": placement_df_sorted_7d.iloc[0]['total_clicks_7d'],
                        "最近3天的总点击次数": placement_df_sorted_3d.iloc[0]['total_clicks_3d'],
                        "对广告位进行竞价操作的原因": "满足定义一：最近7天和最近3天ACOS值最小，点击次数不是最大"
                    }
                    results.append(result)

# 生成结果CSV文件
results_df = pd.DataFrame(results)
results_df.to_csv(output_path, index=False)

print(f"符合条件的广告位信息已保存到 {output_path}")