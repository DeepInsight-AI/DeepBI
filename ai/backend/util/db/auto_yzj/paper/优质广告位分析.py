# filename: 优质广告位分析.py
import pandas as pd

# 加载 CSV 数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\预处理.csv'
data = pd.read_csv(file_path)

def filter_good_placements(df):
    good_placements = []

    for campaign, group in df.groupby('campaignName'):
        group_acos_7d = group[group['ACOS_7d'].between(0, 0.24)]
        group_acos_3d = group[group['ACOS_3d'].between(0, 0.24)]

        if not group_acos_7d.empty:
            min_acos_7d = group_acos_7d.loc[group_acos_7d['ACOS_7d'].idxmin()]
            max_clicks_7d = group.loc[group['total_clicks_7d'].idxmax()]

            if min_acos_7d['placementClassification'] != max_clicks_7d['placementClassification']:
                reason = '最近7天的平均ACOS值最小，但点击次数不是最大的广告位'
                good_placements.append({
                    'campaignName': campaign,
                    'placementClassification': min_acos_7d['placementClassification'],
                    'ACOS_7d': min_acos_7d['ACOS_7d'],
                    'ACOS_3d': min_acos_7d['ACOS_3d'],
                    'total_clicks_7d': min_acos_7d['total_clicks_7d'],
                    'total_clicks_3d': min_acos_7d['total_clicks_3d'],
                    'reason': reason
                })
        
        if not group_acos_3d.empty:
            min_acos_3d = group_acos_3d.loc[group_acos_3d['ACOS_3d'].idxmin()]
            max_clicks_3d = group.loc[group['total_clicks_3d'].idxmax()]

            if min_acos_3d['placementClassification'] != max_clicks_3d['placementClassification']:
                reason = '最近3天的平均ACOS值最小，但点击次数不是最大的广告位'
                good_placements.append({
                    'campaignName': campaign,
                    'placementClassification': min_acos_3d['placementClassification'],
                    'ACOS_7d': min_acos_3d['ACOS_7d'],
                    'ACOS_3d': min_acos_3d['ACOS_3d'],
                    'total_clicks_7d': min_acos_3d['total_clicks_7d'],
                    'total_clicks_3d': min_acos_3d['total_clicks_3d'],
                    'reason': reason
                })

    return pd.DataFrame(good_placements)

filtered_data = filter_good_placements(data)

output_path = 'C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\自动sp广告\\广告位优化\\提问策略\\自动_优质广告位_ES_2024-06-121.csv'
filtered_data.to_csv(output_path, index=False)
print(f"结果已保存到 {output_path}")