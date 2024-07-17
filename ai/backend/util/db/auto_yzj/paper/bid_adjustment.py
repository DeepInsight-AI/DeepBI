# filename: bid_adjustment.py
import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\预处理.csv'
df = pd.read_csv(file_path)

def adjust_bid(row):
    if row['bid'] + 5 <= 50:
        return row['bid'] + 5
    else:
        return 50

# 记录符合条件的广告位
qualified_ads = []

# 遍历广告活动
for campaign_id in df['campaignId'].unique():
    campaign_df = df[df['campaignId'] == campaign_id]
    
    for placement in ['Top of Search on-Amazon', 'Detail Page on-Amazon', 'Other on-Amazon']:
        placement_df = campaign_df[campaign_df['placementClassification'] == placement]
        
        if placement_df.empty:
            continue
        
        # 最近7天平均ACOS最小并满足条件
        min_acos_7d = placement_df[(placement_df['ACOS_7d'] > 0) & (placement_df['ACOS_7d'] <= 0.24)]
        min_acos_3d = placement_df[(placement_df['ACOS_3d'] > 0) & (placement_df['ACOS_3d'] <= 0.24)]
        
        if not min_acos_7d.empty and not min_acos_3d.empty:
            min_acos_7d_item = min_acos_7d.loc[min_acos_7d['ACOS_7d'].idxmin()]
            min_acos_3d_item = min_acos_3d.loc[min_acos_3d['ACOS_3d'].idxmin()]
            
            if (min_acos_7d_item['total_clicks_7d'] != placement_df['total_clicks_7d'].max()) and (min_acos_3d_item['total_clicks_3d'] != placement_df['total_clicks_3d'].max()):
                # 满足定义一的情况
                new_bid = adjust_bid(min_acos_7d_item)
                qualified_ads.append({
                    'campaignName': min_acos_7d_item['campaignName'],
                    'campaignId': min_acos_7d_item['campaignId'],
                    'placementClassification': min_acos_7d_item['placementClassification'],
                    'ACOS_7d': min_acos_7d_item['ACOS_7d'],
                    'ACOS_3d': min_acos_3d_item['ACOS_3d'],
                    'total_clicks_7d': min_acos_7d_item['total_clicks_7d'],
                    'total_clicks_3d': min_acos_3d_item['total_clicks_3d'],
                    'bid': min_acos_7d_item['bid'],
                    'new_bid': new_bid,
                    'reason': '定义一'
                })
                
            if (min_acos_7d_item['total_clicks_7d'] != placement_df['total_clicks_7d'].max()) and (min_acos_3d_item['total_cost_3d'] < 4) and (min_acos_3d_item['total_clicks_3d'] != placement_df['total_clicks_3d'].max()):
                # 满足定义二的情况
                new_bid = adjust_bid(min_acos_7d_item)
                qualified_ads.append({
                    'campaignName': min_acos_7d_item['campaignName'],
                    'campaignId': min_acos_7d_item['campaignId'],
                    'placementClassification': min_acos_7d_item['placementClassification'],
                    'ACOS_7d': min_acos_7d_item['ACOS_7d'],
                    'ACOS_3d': min_acos_3d_item['ACOS_3d'],
                    'total_clicks_7d': min_acos_7d_item['total_clicks_7d'],
                    'total_clicks_3d': min_acos_3d_item['total_clicks_3d'],
                    'bid': min_acos_7d_item['bid'],
                    'new_bid': new_bid,
                    'reason': '定义二'
                })

# 转换结果为DataFrame
result_df = pd.DataFrame(qualified_ads)

# 保存为CSV文件
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\提问策略\自动_优质广告位_v1_1_LAPASA_US_2024-07-12.csv'
result_df.to_csv(output_path, index=False)

print(f"结果已保存到 {output_path}")