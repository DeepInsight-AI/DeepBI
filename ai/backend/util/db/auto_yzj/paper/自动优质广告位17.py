# filename: ad_placement_analysis.py
import pandas as pd

# 读取文件路径
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\预处理.csv'

# 读取CSV文件
data = pd.read_csv(file_path)

# 数据准备：计算最近7天和3天的平均ACOS值和总点击次数
data['ACOS_7d_avg'] = data['ACOS_7d']
data['ACOS_3d_avg'] = data['ACOS_3d']
data['total_clicks_7d_sum'] = data['total_clicks_7d']
data['total_clicks_3d_sum'] = data['total_clicks_3d']

# 定义空列表存储符合条件的广告位的记录
filtered_results = []

# 按campaignName分组
grouped = data.groupby('campaignName')

# 遍历每个广告活动的广告位
for name, group in grouped:
    best_7d_ACOS = group[(group['ACOS_7d_avg'] > 0) & (group['ACOS_7d_avg'] <= 0.24)]['ACOS_7d_avg'].min()
    best_3d_ACOS = group[(group['ACOS_3d_avg'] > 0) & (group['ACOS_3d_avg'] <= 0.24)]['ACOS_3d_avg'].min()

    for index, row in group.iterrows():
        if (row['ACOS_7d_avg'] == best_7d_ACOS and
                row['total_clicks_7d_sum'] != group['total_clicks_7d_sum'].max() and
                row['ACOS_3d_avg'] == best_3d_ACOS and
                row['total_clicks_3d_sum'] != group['total_clicks_3d_sum'].max()):

            # 竞价调整
            adjusted_bid = round(row['ACOS_7d_avg'] * 1.05, 2)
            if adjusted_bid > 0.50:
                adjusted_bid = 0.50

            placement_info = {
                'campaignName': row['campaignName'],
                'campaignId': row['campaignId'],
                'placementClassification': row['placementClassification'],
                'ACOS_7d_avg': row['ACOS_7d_avg'],
                'ACOS_3d_avg': row['ACOS_3d_avg'],
                'total_clicks_7d_sum': row['total_clicks_7d_sum'],
                'total_clicks_3d_sum': row['total_clicks_3d_sum'],
                'adjusted_bid': adjusted_bid,
                'reason': '符合定义一的广告位，提高竞价5%'
            }
            filtered_results.append(placement_info)

# 将结果存储到新的CSV文件中
output_df = pd.DataFrame(filtered_results)
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\提问策略\自动_优质广告位_v1_1_IT_2024-06-17.csv'
output_df.to_csv(output_file_path, index=False)

print(f"符合条件的广告位已保存到 {output_file_path}")