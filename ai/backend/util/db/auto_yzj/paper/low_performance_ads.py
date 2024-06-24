# filename: low_performance_ads.py

import pandas as pd

# 载入数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\预处理.csv'
df = pd.read_csv(file_path)

# 定义不良广告位的判定
reasons = []

# 定义一：最近7天的总Sales为0，但最近7天的总点击数大于0
condition1 = (df['total_sales14d_7d'] == 0) & (df['total_clicks_7d'] > 0)
condition1_df = df[condition1]
condition1_df['reason'] = '最近7天的总Sales为0，但最近7天的总点击数大于0'
reasons.append(condition1_df)

# 定义二：对于同一广告活动中的广告位，满足特定的ACOS条件
condition2_df = pd.DataFrame()
for campaign in df['campaignName'].unique():
    campaign_df = df[df['campaignName'] == campaign]
    condition2_campaign_df = campaign_df[
        (campaign_df['ACOS_7d'] > 0.24) & 
        (campaign_df['ACOS_7d'] < 0.5)
    ]
    
    if len(condition2_campaign_df) >= 3:
        acos_sorted = condition2_campaign_df.sort_values(by='ACOS_7d')
        acos_diff = acos_sorted['ACOS_7d'].iloc[-1] - acos_sorted['ACOS_7d'].iloc[0]
        
        if acos_diff >= 0.2:
            acos_sorted['reason'] = '同一广告活动中的三个广告位满足特定的ACOS条件'
            condition2_df = pd.concat([condition2_df, acos_sorted.iloc[[-1]]])
reasons.append(condition2_df)

# 定义三：最近7天的平均ACOS值大于等于0.5
condition3 = df['ACOS_7d'] >= 0.5
condition3_df = df[condition3]
condition3_df['reason'] = '最近7天的平均ACOS值大于等于0.5'
reasons.append(condition3_df)

# 合并所有不良广告位记录
low_performance_ads = pd.concat(reasons).drop_duplicates()

# 保留需要的字段
result_df = low_performance_ads[[
    'campaignName', 'placementClassification', 'ACOS_7d', 'ACOS_3d', 'total_clicks_7d', 'total_clicks_3d', 'reason'
]]

# 保存结果到CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\提问策略\手动_劣质广告位_ES_2024-06-07.csv'
result_df.to_csv(output_file_path, index=False)

print(f'文件已保存到: {output_file_path}')