# filename: find_good_performance_ads.py
import pandas as pd
import os
from datetime import date

# 1. 读取数据
input_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\预处理.csv'
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\提问策略\优质广告位_FR_2024-5-27.csv'

# Load the CSV file into a DataFrame
data = pd.read_csv(input_file_path)

# 2. 寻找满足定义一的广告位
good_ad_placements = []

for campaign in data['campaignName'].unique():
    campaign_data = data[data['campaignName'] == campaign]

    # 找到最近7天平均ACOS值最小的广告位，但最近7天点击次数不是最大的广告位，并且0 < ACOS <= 0.24
    min_acos_7d = campaign_data[(campaign_data['ACOS_7d'] > 0) & (campaign_data['ACOS_7d'] <= 0.24)]
    if min_acos_7d.empty:
        continue

    min_acos_placement_7d = min_acos_7d.loc[min_acos_7d['ACOS_7d'].idxmin()]
    if min_acos_placement_7d['total_clicks_7d'] == campaign_data['total_clicks_7d'].max():
        continue

    # 再次筛选，找到最近3天平均ACOS值最小的广告位，并且0 < ACOS <= 0.24，最近3天点击次数不是最大的广告位
    min_acos_3d = campaign_data[(campaign_data['ACOS_3d'] > 0) & (campaign_data['ACOS_3d'] <= 0.24)]
    if min_acos_3d.empty:
        continue

    min_acos_placement_3d = min_acos_3d.loc[min_acos_3d['ACOS_3d'].idxmin()]
    if min_acos_placement_3d['total_clicks_3d'] == campaign_data['total_clicks_3d'].max():
        continue

    # 满足定义一的广告位，添加到结果列表
    good_ad_placements.append({
        'date': '2024-05-27',
        'campaignName': min_acos_placement_3d['campaignName'],
        '广告位': min_acos_placement_3d['placementClassification'],
        '最近7天的平均ACOS值': min_acos_placement_3d['ACOS_7d'],
        '最近3天的平均ACOS值': min_acos_placement_3d['ACOS_3d'],
        '最近7天的总点击次数': min_acos_placement_3d['total_clicks_7d'],
        '最近3天的总点击次数': min_acos_placement_3d['total_clicks_3d'],
        '竞价操作': '提高竞价5%，最高50%',
        '原因': '满足定义一：最近7天和最近3天的平均ACOS值都最小且点击次数不是最大的广告位'
    })

# 转换为DataFrame
output_df = pd.DataFrame(good_ad_placements)

# 3. 将结果保存到CSV文件
os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
output_df.to_csv(output_file_path, index=False)

print("结果已保存到:", output_file_path)