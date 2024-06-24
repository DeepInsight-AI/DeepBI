# filename: evaluate_placement_bids.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\预处理.csv'
data = pd.read_csv(file_path)

# 定义展示函数
def evaluate_and_adjust_bids(data):
    good_placements = []
    campaigns = data['campaignName'].unique()
    
    for campaign in campaigns:
        campaign_data = data[data['campaignName'] == campaign].dropna(subset=['ACOS_7d', 'ACOS_3d', 'total_clicks_7d', 'total_clicks_3d'])
        
        if campaign_data.shape[0] < 3:
            continue
        
        # 最近7天
        min_acos_7d_index = campaign_data['ACOS_7d'].idxmin()
        max_clicks_7d_index = campaign_data['total_clicks_7d'].idxmax()
        min_acos_7d_value = campaign_data.loc[min_acos_7d_index, 'ACOS_7d']
        
        if 0 < min_acos_7d_value <= 0.24 and min_acos_7d_index != max_clicks_7d_index:
            placement_7d = campaign_data.loc[min_acos_7d_index]
            good_placements.append({
                "date": "2024-05-27",
                "campaignName": campaign,
                "placement": placement_7d['placementClassification'],
                "ACOS_7d": min_acos_7d_value,
                "ACOS_3d": placement_7d['ACOS_3d'],
                "total_clicks_7d": placement_7d['total_clicks_7d'],
                "total_clicks_3d": placement_7d['total_clicks_3d'],
                "原因": "最近7天的平均ACOS值最低且满足条件，但最近7天点击次数不是最大"
            })
            
        # 最近3天
        min_acos_3d_index = campaign_data['ACOS_3d'].idxmin()
        max_clicks_3d_index = campaign_data['total_clicks_3d'].idxmax()
        min_acos_3d_value = campaign_data.loc[min_acos_3d_index, 'ACOS_3d']
        
        if 0 < min_acos_3d_value <= 0.24 and min_acos_3d_index != max_clicks_3d_index:
            placement_3d = campaign_data.loc[min_acos_3d_index]
            good_placements.append({
                "date": "2024-05-27",
                "campaignName": campaign,
                "placement": placement_3d['placementClassification'],
                "ACOS_7d": placement_3d['ACOS_7d'],
                "ACOS_3d": min_acos_3d_value,
                "total_clicks_7d": placement_3d['total_clicks_7d'],
                "total_clicks_3d": placement_3d['total_clicks_3d'],
                "原因": "最近3天的平均ACOS值最低且满足条件，但最近3天点击次数不是最大"
            })
    
    result_df = pd.DataFrame(good_placements)
    return result_df

# 应用函数并保存结果
result = evaluate_and_adjust_bids(data)
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\提问策略\手动_优质广告位_ES_2024-06-10.csv'
result.to_csv(output_file_path, index=False)

# 打印输出文件路径
print(f'Result saved to {output_file_path}')