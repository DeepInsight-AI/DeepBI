# filename: ad_bid_optimization.py

import pandas as pd

# 读取数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\预处理.csv'
data = pd.read_csv(file_path)

# 过滤符合条件的广告位
result = []

for campaign_name, group in data.groupby('campaignName'):
    # 条件1
    min_acos_7d = group[(group['ACOS_7d'] > 0) & (group['ACOS_7d'] <= 0.24)]['ACOS_7d'].min()
    condition1 = group[(group['ACOS_7d'] == min_acos_7d) & (group['total_clicks_7d'] != group['total_clicks_7d'].max())]

    # 条件2
    min_acos_3d = group[(group['ACOS_3d'] > 0) & (group['ACOS_3d'] <= 0.24)]['ACOS_3d'].min()
    condition2 = group[(group['ACOS_3d'] == min_acos_3d) & (group['total_clicks_3d'] != group['total_clicks_3d'].max())]

    # 找到满足两个条件的广告位
    good_ads = pd.merge(condition1, condition2, on=['placementClassification', 'campaignName', 
                                                    'total_clicks_3d', 'total_clicks_7d', 'ACOS_3d', 'ACOS_7d'])
    for _, row in good_ads.iterrows():
        result.append({
            'date': '2024-05-27',
            'campaignName': row['campaignName'],
            'placementClassification': row['placementClassification'],
            'ACOS_7d': row['ACOS_7d'],
            'ACOS_3d': row['ACOS_3d'],
            'total_clicks_7d': row['total_clicks_7d'],
            'total_clicks_3d': row['total_clicks_3d'],
            'reason': '满足定义一中的条件1和条件2，建议提高竞价5%'
        })

# 转换为DataFrame
result_df = pd.DataFrame(result)

# 保存结果到CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\提问策略\手动_优质广告位_IT_2024-06-06.csv'
result_df.to_csv(output_file_path, index=False, encoding='utf-8-sig')

print(f"结果已保存到 {output_file_path}")