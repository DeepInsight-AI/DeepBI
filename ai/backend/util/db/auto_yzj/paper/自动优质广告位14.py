# filename: analyze_ads_performance.py

import pandas as pd

# Step 1: Load the dataset
file_path = 'C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\自动sp广告\\广告位优化\\预处理.csv'
data = pd.read_csv(file_path)

# Step 2: Compute necessary metrics
data['avg_ACOS_7d'] = data['ACOS_7d']
data['avg_ACOS_3d'] = data['ACOS_3d']
data['total_clicks_7d'] = data['total_clicks_7d']
data['total_clicks_3d'] = data['total_clicks_3d']

# Step 3: Find the best-performing placement according to definition 1
result = []
campaigns = data['campaignId'].unique()

for campaign in campaigns:
    campaign_data = data[data['campaignId'] == campaign]
    
    min_acos_7d = campaign_data[campaign_data['avg_ACOS_7d'] <= 0.24]['avg_ACOS_7d'].min()
    min_acos_3d = campaign_data[campaign_data['avg_ACOS_3d'] <= 0.24]['avg_ACOS_3d'].min()
    
    best_7d = campaign_data[(campaign_data['avg_ACOS_7d'] == min_acos_7d) & (campaign_data['total_clicks_7d'] != campaign_data['total_clicks_7d'].max())]
    best_3d = campaign_data[(campaign_data['avg_ACOS_3d'] == min_acos_3d) & (campaign_data['total_clicks_3d'] != campaign_data['total_clicks_3d'].max())]

    best_performing_placement = pd.merge(best_7d, best_3d, how='inner')
    
    if not best_performing_placement.empty:
        for _, row in best_performing_placement.iterrows():
            result.append({
                'campaignName': row['campaignName'],
                'campaignId': row['campaignId'],
                'placementClassification': row['placementClassification'],
                'avg_ACOS_7d': row['avg_ACOS_7d'],
                'avg_ACOS_3d': row['avg_ACOS_3d'],
                'total_clicks_7d': row['total_clicks_7d'],
                'total_clicks_3d': row['total_clicks_3d'],
                '竞价调整': '提高5%',
                '原因': '最近7天和3天ACOS最小且点击次数不是最大'
            })

# Step 4: Output the result to a CSV file
result_df = pd.DataFrame(result)
output_path = 'C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\自动sp广告\\广告位优化\\提问策略\\自动_优质广告位_v1_1_ES_2024-06-14.csv'
result_df.to_csv(output_path, index=False)

print('Analysis completed and results saved.')