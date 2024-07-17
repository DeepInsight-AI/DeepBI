# filename: adjust_bid.py

import pandas as pd

# 读取 CSV 文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\预处理.csv'
data = pd.read_csv(file_path)

# 定义增加后的竞价上限
bid_increase = 5
max_bid = 50

# 保存判断条件原因
reasons = []

# 筛选符合定义一和定义二的广告位
qualified_ad_slots = []

# 遍历每一个广告活动
campaigns = data['campaignName'].unique()
for campaign in campaigns:
    campaign_data = data[data['campaignName'] == campaign]
    
    # 筛选广告活动的广告位
    for placement in campaign_data['placementClassification'].unique():
        placement_data = campaign_data[campaign_data['placementClassification'] == placement]
        
        # 定义最近七天和三天的特性
        acos_7d_min = placement_data[placement_data['ACOS_7d'] > 0]['ACOS_7d'].min()
        acos_3d_min = placement_data[placement_data['ACOS_3d'] > 0]['ACOS_3d'].min()
        clicks_7d_max = placement_data['total_clicks_7d'].max()
        clicks_3d_max = placement_data['total_clicks_3d'].max()
        cost_3d_threshold = 4

        for idx, row in placement_data.iterrows():
            if row['ACOS_7d'] == acos_7d_min and 0 < row['ACOS_7d'] <= 0.24 \
                    and row['total_clicks_7d'] < clicks_7d_max \
                    and row['ACOS_3d'] == acos_3d_min and 0 < row['ACOS_3d'] <= 0.24 \
                    and row['total_clicks_3d'] < clicks_3d_max:
                new_bid = min(row['bid'] + bid_increase, max_bid)
                qualified_ad_slots.append([row['campaignName'], row['campaignId'], row['placementClassification'],
                                           row['ACOS_7d'], row['ACOS_3d'], row['total_clicks_7d'], row['total_clicks_3d'],
                                           row['bid'], new_bid, '满足定义一'])
            elif row['ACOS_7d'] == acos_7d_min and 0 < row['ACOS_7d'] <= 0.24 \
                    and row['total_clicks_7d'] < clicks_7d_max \
                    and row['total_cost_3d'] < cost_3d_threshold \
                    and row['total_clicks_3d'] < clicks_3d_max:
                new_bid = min(row['bid'] + bid_increase, max_bid)
                qualified_ad_slots.append([row['campaignName'], row['campaignId'], row['placementClassification'],
                                           row['ACOS_7d'], row['ACOS_3d'], row['total_clicks_7d'], row['total_clicks_3d'],
                                           row['bid'], new_bid, '满足定义二'])

# 创建结果 DataFrame
columns = ['campaignName', 'campaignId', 'placementClassification', 'ACOS_7d', 'ACOS_3d',
           'total_clicks_7d', 'total_clicks_3d', 'bid', 'adjusted_bid', 'reason']
result_df = pd.DataFrame(qualified_ad_slots, columns=columns)

# 保存结果到CSV
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\提问策略\手动_优质广告位_v1_1_LAPASA_UK_2024-07-14.csv'
result_df.to_csv(output_path, index=False)

print(f"处理完成，结果已保存到 {output_path}")
