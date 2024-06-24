# filename: identify_and_bid_ads.py

import pandas as pd

# 读取数据
file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\预处理.csv"
data = pd.read_csv(file_path)

# 数据准备
columns_needed = [
    'campaignName', 'placementClassification',
    'ACOS_7d', 'ACOS_3d',
    'total_clicks_7d', 'total_clicks_3d'
]
df = data[columns_needed]

# 判断表现较好的广告位
grouped = df.groupby('campaignName')

results = []

for campaign, group in grouped:
    good_ads_set = set()
    
    # 找到最近7天的最小ACOS值的广告位，并且ACOS在0到24%之间
    min_acos_7d = group[(group['ACOS_7d'] > 0) & (group['ACOS_7d'] <= 24)]['ACOS_7d'].min()
    min_acos_7d_ads = group[(group['ACOS_7d'] == min_acos_7d) & (group['ACOS_7d'] > 0) & (group['ACOS_7d'] <= 24)].copy()
    
    for idx, ad in min_acos_7d_ads.iterrows():
        if ad['total_clicks_7d'] != group['total_clicks_7d'].max():
            good_ads_set.add(ad['placementClassification'])
            ad['竞价操作'] = '提高竞价5%，最高50%'
            ad['对广告位进行竞价操作的原因'] = '最近7天的平均ACOS值最小且点击次数不是最大'
            results.append(ad.to_dict())
    
    # 找到最近3天的最小ACOS值的广告位，并且ACOS在0到24%之间
    min_acos_3d = group[(group['ACOS_3d'] > 0) & (group['ACOS_3d'] <= 24)]['ACOS_3d'].min()
    min_acos_3d_ads = group[(group['ACOS_3d'] == min_acos_3d) & (group['ACOS_3d'] > 0) & (group['ACOS_3d'] <= 24)].copy()
    
    for idx, ad in min_acos_3d_ads.iterrows():
        if ad['total_clicks_7d'] != group['total_clicks_7d'].max():
            if ad['placementClassification'] not in good_ads_set:
                ad['竞价操作'] = '提高竞价5%，最高50%'
                ad['对广告位进行竞价操作的原因'] = '最近3天的平均ACOS值最小且点击次数不是最大'
                results.append(ad.to_dict())

result_df = pd.DataFrame(results)
result_df = result_df[['campaignName', 'placementClassification', 'ACOS_7d', 'ACOS_3d', 'total_clicks_7d', 'total_clicks_3d', '竞价操作', '对广告位进行竞价操作的原因']]

# 保存结果
output_file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\提问策略\优质广告位_FR.csv"
result_df.to_csv(output_file_path, index=False)

print(f"结果已保存到 {output_file_path}")