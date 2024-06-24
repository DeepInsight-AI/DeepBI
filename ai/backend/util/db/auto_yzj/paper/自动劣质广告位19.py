# filename: analyze_ad_slots.py

import pandas as pd

# Load the data from the CSV file
file_path = 'C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\自动sp广告\\广告位优化\\预处理.csv'
data = pd.read_csv(file_path)

# Initialize a list to store the results
results = []

# Iterate through each campaign
for campaign_id, campaign_group in data.groupby('campaignId'):
    # Apply Definition 1
    df_def1 = campaign_group[(campaign_group['total_sales14d_7d'] == 0) & (campaign_group['total_clicks_7d'] > 0)]
    for _, row in df_def1.iterrows():
        results.append({
            "campaignName": row['campaignName'],
            "campaignId": row['campaignId'],
            "广告位": row['placementClassification'],
            "最近7天的平均ACOS值": row['ACOS_7d'],
            "最近3天的平均ACOS值": row['ACOS_3d'],
            "最近7天的总点击次数": row['total_clicks_7d'],
            "最近3天的总点击次数": row['total_clicks_3d'],
            "bid": row['bid'],
            "new_bid": 0,
            "原因": "最近7天的总sales为0，但最近7天的总点击数大于0"
        })

    # Apply Definition 2
    campaign_acos = campaign_group['ACOS_7d']
    if len(campaign_acos) == 3:
        acos_max = campaign_acos.max()
        acos_min = campaign_acos.min()
        if 24 < acos_max < 50 and (acos_max - acos_min) >= 0.2:
            idx_max = campaign_group['ACOS_7d'].idxmax()
            row = campaign_group.loc[idx_max]
            new_bid = max(row['bid'] * 0.97, 0)  # reduce by 3%
            results.append({
                "campaignName": row['campaignName'],
                "campaignId": row['campaignId'],
                "广告位": row['placementClassification'],
                "最近7天的平均ACOS值": row['ACOS_7d'],
                "最近3天的平均ACOS值": row['ACOS_3d'],
                "最近7天的总点击次数": row['total_clicks_7d'],
                "最近3天的总点击次数": row['total_clicks_3d'],
                "bid": row['bid'],
                "new_bid": new_bid,
                "原因": "三个广告位最近7天的平均ACOS值均大于24%小于50%，并且最大与最小值差距>=0.2"
            })

    # Apply Definition 3
    df_def3 = campaign_group[campaign_group['ACOS_7d'] >= 50]
    for _, row in df_def3.iterrows():
        results.append({
            "campaignName": row['campaignName'],
            "campaignId": row['campaignId'],
            "广告位": row['placementClassification'],
            "最近7天的平均ACOS值": row['ACOS_7d'],
            "最近3天的平均ACOS值": row['ACOS_3d'],
            "最近7天的总点击次数": row['total_clicks_7d'],
            "最近3天的总点击次数": row['total_clicks_3d'],
            "bid": row['bid'],
            "new_bid": 0,
            "原因": "最近7天的平均ACOS值>=50%"
        })

# Convert results to a DataFrame
results_df = pd.DataFrame(results)

# Save the results to a CSV file
output_file_path = 'C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\自动sp广告\\广告位优化\\提问策略\\自动_劣质广告位_v1_1_IT_2024-06-19.csv'
results_df.to_csv(output_file_path, index=False)

# Print the results
print(results_df)