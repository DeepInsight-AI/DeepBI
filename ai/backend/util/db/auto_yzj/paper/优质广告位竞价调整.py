# filename: 优质广告位竞价调整.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\预处理.csv'
df = pd.read_csv(file_path)

# 定义检测条件
def detect_ads(df):
    result = []

    # 按广告活动进行分组
    campaigns = df.groupby('campaignId')
    for campaign_id, campaign_data in campaigns:
        placements = ['Top of Search on-Amazon', 'Detail Page on-Amazon', 'Other on-Amazon']
        for placement in placements:
            # 获取相关数据
            placement_data = campaign_data[campaign_data['placementClassification'] == placement]
            
            # 按定义一的判断条件
            if not placement_data.empty:
                min_acos_3d = placement_data['ACOS_3d'].min()
                min_acos_7d = placement_data['ACOS_7d'].min()
                max_clicks_3d = placement_data['total_clicks_3d'].max()
                max_clicks_7d = placement_data['total_clicks_7d'].max()
                acos_3d_check = (placement_data['ACOS_3d'] > 0) & (placement_data['ACOS_3d'] <= 0.24)
                acos_7d_check = (placement_data['ACOS_7d'] > 0) & (placement_data['ACOS_7d'] <= 0.24)
                low_acos_3d = placement_data[placement_data['ACOS_3d'] == min_acos_3d]
                low_acos_7d = placement_data[placement_data['ACOS_7d'] == min_acos_7d]
                
                # 检查定义一
                if all(acos_3d_check) and all(acos_7d_check) and \
                   (low_acos_7d['total_clicks_7d'].values[0] != max_clicks_7d) and \
                   (low_acos_3d['total_clicks_3d'].values[0] != max_clicks_3d):
                    new_bid = min(low_acos_3d['bid'].values[0] + 5, 50)
                    reason = '满足定义一：ACOS和点击条件均符合'
                    result.append([low_acos_3d['campaignName'].values[0], campaign_id, placement, 
                                   low_acos_3d['ACOS_7d'].values[0], low_acos_3d['ACOS_3d'].values[0],
                                   low_acos_3d['total_clicks_7d'].values[0], low_acos_3d['total_clicks_3d'].values[0],
                                   low_acos_3d['bid'].values[0], new_bid, reason])
                    
                # 检查定义二
                if all(acos_7d_check) and (low_acos_7d['total_clicks_7d'].values[0] != max_clicks_7d) and \
                   (low_acos_3d['total_cost_3d'].values[0] < 4) and (low_acos_3d['total_clicks_3d'].values[0] != max_clicks_3d):
                    new_bid = min(low_acos_3d['bid'].values[0] + 5, 50)
                    reason = '满足定义二：ACOS和花费条件均符合'
                    result.append([low_acos_3d['campaignName'].values[0], campaign_id, placement, 
                                   low_acos_3d['ACOS_7d'].values[0], low_acos_3d['ACOS_3d'].values[0],
                                   low_acos_3d['total_clicks_7d'].values[0], low_acos_3d['total_clicks_3d'].values[0],
                                   low_acos_3d['bid'].values[0], new_bid, reason])

    return result

# 检测广告
results = detect_ads(df)

# 生成结果数据框并保存CSV
output_df = pd.DataFrame(results, columns=['campaignName', 'campaignId', '广告位', '最近7天的平均ACOS值', 
                                           '最近3天的平均ACOS值', '最近7天的总点击次数', '最近3天的总点击次数', 
                                           'bid', '调整后的竞价', '对广告位进行竞价操作的原因'])
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\提问策略\手动_优质广告位_v1_1_LAPASA_FR_2024-07-12.csv'
output_df.to_csv(output_path, index=False)
print(f"结果保存在 {output_path}")
