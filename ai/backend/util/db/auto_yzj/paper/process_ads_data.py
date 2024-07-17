# filename: process_ads_data.py

import pandas as pd

# 读取 CSV 文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\预处理.csv'
data = pd.read_csv(file_path)

# 定义判别广告位表现较好的函数
def check_and_adjust_bid(data):
    result = []
    
    for campaign in data['campaignId'].unique():
        campaign_data = data[data['campaignId'] == campaign]
        placements = campaign_data['placementClassification'].unique()
        
        if len(placements) > 2:
            min_acos_7d = campaign_data[campaign_data['ACOS_7d'] > 0]['ACOS_7d'].min()
            min_acos_3d = campaign_data[campaign_data['ACOS_3d'] > 0]['ACOS_3d'].min()

            conditions_def1 = (campaign_data['ACOS_7d'] > 0) & (campaign_data['ACOS_7d'] <= 0.24) & \
                              (campaign_data['ACOS_3d'] > 0) & (campaign_data['ACOS_3d'] <= 0.24) & \
                              (campaign_data['ACOS_7d'] == min_acos_7d) & (campaign_data['ACOS_3d'] == min_acos_3d) & \
                              (campaign_data['total_clicks_7d'] != campaign_data['total_clicks_7d'].max()) & \
                              (campaign_data['total_clicks_3d'] != campaign_data['total_clicks_3d'].max())
            
            conditions_def2 = (campaign_data['ACOS_7d'] > 0) & (campaign_data['ACOS_7d'] <= 0.24) & \
                              (campaign_data['total_cost_3d'] < 4) & \
                              (campaign_data['total_clicks_7d'] != campaign_data['total_clicks_7d'].max()) & \
                              (campaign_data['total_clicks_3d'] != campaign_data['total_clicks_3d'].max())
            
            qualified_ads_def1 = campaign_data[conditions_def1]
            qualified_ads_def2 = campaign_data[conditions_def2]
                        
            # 满足条件的广告位推进处理
            for _, row in qualified_ads_def1.iterrows():
                new_bid = min(row['bid'] + 5, 50)
                result.append({
                    'campaignName': row['campaignName'],
                    'campaignId': row['campaignId'],
                    'placementClassification': row['placementClassification'],
                    'ACOS_7d': row['ACOS_7d'],
                    'ACOS_3d': row['ACOS_3d'],
                    'total_clicks_7d': row['total_clicks_7d'],
                    'total_clicks_3d': row['total_clicks_3d'],
                    'current_bid': row['bid'],
                    'adjusted_bid': new_bid,
                    'reason': "定义一"
                })
                
            for _, row in qualified_ads_def2.iterrows():
                new_bid = min(row['bid'] + 5, 50)
                result.append({
                    'campaignName': row['campaignName'],
                    'campaignId': row['campaignId'],
                    'placementClassification': row['placementClassification'],
                    'ACOS_7d': row['ACOS_7d'],
                    'ACOS_3d': row['ACOS_3d'],
                    'total_clicks_7d': row['total_clicks_7d'],
                    'total_clicks_3d': row['total_clicks_3d'],
                    'current_bid': row['bid'],
                    'adjusted_bid': new_bid,
                    'reason': "定义二"
                })
                
    return pd.DataFrame(result)

# 调用函数进行处理
result_df = check_and_adjust_bid(data)

# 输出结果到新的 CSV 中
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\提问策略\手动_优质广告位_v1_1_LAPASA_US_2024-07-12.csv'
result_df.to_csv(output_path, index=False)

print('广告位表现优异的数据已经存储到目标路径下的 csv 文件中。')