# filename: optimize_ad_placements.py

import pandas as pd
from datetime import datetime

# 加载数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\预处理.csv'
data = pd.read_csv(file_path)

# 定义逻辑，寻找符合定义一的广告位
def find_good_placements(df):

    def get_min_acos_indices(group, days):
        acos_col = f'ACOS_{days}d'
        clicks_col = f'total_clicks_{days}d'
        min_acos_idx = group[group[acos_col] == group[acos_col].min()].index.tolist()
        
        if len(min_acos_idx) > 0 and group[acos_col].min() > 0 and group[acos_col].min() <= 0.24:
            max_clicks_idx = group[group[clicks_col] == group[clicks_col].max()].index.tolist()
            return [idx for idx in min_acos_idx if idx not in max_clicks_idx]
        return []

    good_places = []

    grouped = df.groupby('campaignId')
    
    for campaignId, group in grouped:
        min_acos_7d_idx = get_min_acos_indices(group, 7)
        min_acos_3d_idx = get_min_acos_indices(group, 3)
        
        good_idx = set(min_acos_7d_idx) & set(min_acos_3d_idx)
        if good_idx:
            for idx in good_idx:
                current_bid = group.loc[idx, 'bid']
                new_bid = min(current_bid * 1.05, current_bid * 1.5)
                good_places.append({
                    'campaignName': group.loc[idx, 'campaignName'],
                    'campaignId': campaignId,
                    'placementClassification': group.loc[idx, 'placementClassification'],
                    'ACOS_7d': group.loc[idx, 'ACOS_7d'],
                    'ACOS_3d': group.loc[idx, 'ACOS_3d'],
                    'total_clicks_7d': group.loc[idx, 'total_clicks_7d'],
                    'total_clicks_3d': group.loc[idx, 'total_clicks_3d'],
                    'bid': current_bid,
                    'new_bid': new_bid,
                    'reason': f'广告位 {group.loc[idx, "placementClassification"]} 最近7天和3天的ACOS值均最小，但点击数不是最大'
                })

    return good_places

# 找到符合条件的广告位
good_placements = find_good_placements(data)

# 将结果保存为CSV文件
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\提问策略\自动_优质广告位_v1_1_IT_' + str(datetime.now().strftime('%Y-%m-%d')) + '.csv'
good_df = pd.DataFrame(good_placements)
good_df.to_csv(output_path, index=False)

print(f'结果已保存到 {output_path}')