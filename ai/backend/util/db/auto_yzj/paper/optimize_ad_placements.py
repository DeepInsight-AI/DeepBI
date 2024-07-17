# filename: optimize_ad_placements.py

import pandas as pd

# 读取数据集
file_path = "C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\手动sp广告\\广告位优化\\预处理.csv"
data = pd.read_csv(file_path)

# 初始化一个新的DataFrame用于存储符合条件的广告位
result = pd.DataFrame(columns=[
    "campaignName", "campaignId", "placementClassification",
    "ACOS_7d", "ACOS_3d", "total_clicks_7d", "total_clicks_3d", 
    "bid", "adjusted_bid", "reason"
])

# 遍历数据并筛选符合条件的广告位
def process_campaign(df, campaign_id):
    campaign_df = df[df['campaignId'] == campaign_id]
    placement_types = campaign_df['placementClassification'].unique()
    for placement in placement_types:
        placement_df = campaign_df[campaign_df['placementClassification'] == placement]
        
        if not placement_df.empty:
            # 定义一
            is_min_7d_acos = placement_df['ACOS_7d'].idxmin() == placement_df.index[0]
            is_not_max_7d_clicks = placement_df['total_clicks_7d'].idxmax() != placement_df.index[0]
            is_min_3d_acos = placement_df['ACOS_3d'].idxmin() == placement_df.index[0]
            is_not_max_3d_clicks = placement_df['total_clicks_3d'].idxmax() != placement_df.index[0]

            if (
                (0 < placement_df['ACOS_7d'].iloc[0] <= 0.24) and is_min_7d_acos and is_not_max_7d_clicks and 
                (0 < placement_df['ACOS_3d'].iloc[0] <= 0.24) and is_min_3d_acos and is_not_max_3d_clicks
            ):
                adjusted_bid = min(50, placement_df['bid'].iloc[0] + 5)
                result.loc[len(result)] = [
                    placement_df['campaignName'].iloc[0], placement_df['campaignId'].iloc[0],
                    placement_df['placementClassification'].iloc[0], placement_df['ACOS_7d'].iloc[0],
                    placement_df['ACOS_3d'].iloc[0], placement_df['total_clicks_7d'].iloc[0],
                    placement_df['total_clicks_3d'].iloc[0], placement_df['bid'].iloc[0],
                    adjusted_bid, "定义一"
                ]
                
            # 定义二
            if (
                (0 < placement_df['ACOS_7d'].iloc[0] <= 0.24) and is_min_7d_acos and is_not_max_7d_clicks and 
                (placement_df['total_cost_3d'].iloc[0] < 4) and is_not_max_3d_clicks
            ):
                adjusted_bid = min(50, placement_df['bid'].iloc[0] + 5)
                result.loc[len(result)] = [
                    placement_df['campaignName'].iloc[0], placement_df['campaignId'].iloc[0],
                    placement_df['placementClassification'].iloc[0], placement_df['ACOS_7d'].iloc[0],
                    placement_df['ACOS_3d'].iloc[0], placement_df['total_clicks_7d'].iloc[0],
                    placement_df['total_clicks_3d'].iloc[0], placement_df['bid'].iloc[0],
                    adjusted_bid, "定义二"
                ]

# 处理所有广告活动
campaign_ids = data['campaignId'].unique()
for campaign_id in campaign_ids:
    process_campaign(data, campaign_id)

# 保存结果
output_path = "C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\手动sp广告\\广告位优化\\提问策略\\手动_优质广告位_v1_1_LAPASA_ES_2024-07-13.csv"
result.to_csv(output_path, index=False)

print("优化后的广告位数据已经保存到:", output_path)