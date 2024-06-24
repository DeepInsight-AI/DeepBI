# filename: analyze_ad_performance.py

import pandas as pd

# 读取数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\预处理.csv'
data = pd.read_csv(file_path)

# Data Preparation
ads_performance = []

# 定义一过滤
definition1 = data[(data['total_sales14d_7d'] == 0) & (data['total_clicks_7d'] > 0)]
for index, row in definition1.iterrows():
    ads_performance.append({
        'campaignName': row['campaignName'],
        'campaignId': row['campaignId'],
        'placement': row['placementClassification'],
        'avg_ACOS_7d': row['ACOS_7d'],
        'avg_ACOS_3d': row['ACOS_3d'],
        'total_clicks_7d': row['total_clicks_7d'],
        'total_clicks_3d': row['total_clicks_3d'],
        'bid_adjustment': '竞价变为0',
        'reason': '最近7天的总sales为0，但最近7天的总点击数大于0'
    })

# 定义二过滤
campaigns = data['campaignId'].unique()
for campaign in campaigns:
    campaign_data = data[data['campaignId'] == campaign]
    if len(campaign_data) == 3:
        avg_ACOS_7d = campaign_data['ACOS_7d']
        if (avg_ACOS_7d > 0.24).all() and (avg_ACOS_7d < 0.5).all() and ((avg_ACOS_7d.max() - avg_ACOS_7d.min()) >= 0.2):
            ad_to_adjust = campaign_data.loc[avg_ACOS_7d.idxmax()]
            ads_performance.append({
                'campaignName': ad_to_adjust['campaignName'],
                'campaignId': ad_to_adjust['campaignId'],
                'placement': ad_to_adjust['placementClassification'],
                'avg_ACOS_7d': ad_to_adjust['ACOS_7d'],
                'avg_ACOS_3d': ad_to_adjust['ACOS_3d'],
                'total_clicks_7d': ad_to_adjust['total_clicks_7d'],
                'total_clicks_3d': ad_to_adjust['total_clicks_3d'],
                'bid_adjustment': '降低竞价3%',
                'reason': '最近7天的平均ACOS值最大的广告位与最近7天的平均ACOS值最小的广告位ACOS值相差大于等于0.2'
            })

# 定义三过滤
definition3 = data[(data['ACOS_7d'] >= 0.5)]
for index, row in definition3.iterrows():
    ads_performance.append({
        'campaignName': row['campaignName'],
        'campaignId': row['campaignId'],
        'placement': row['placementClassification'],
        'avg_ACOS_7d': row['ACOS_7d'],
        'avg_ACOS_3d': row['ACOS_3d'],
        'total_clicks_7d': row['total_clicks_7d'],
        'total_clicks_3d': row['total_clicks_3d'],
        'bid_adjustment': '竞价变为0',
        'reason': '最近7天的平均ACOS值大于等于0.5'
    })

# 输出结果
output_df = pd.DataFrame(ads_performance)
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\提问策略\手动_劣质广告位_v1_1_IT_2024-06-13.csv'
output_df.to_csv(output_file_path, index=False)

print("分析完成，结果已经输出至CSV文件。")