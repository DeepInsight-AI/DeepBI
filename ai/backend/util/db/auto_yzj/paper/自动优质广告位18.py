# filename: update_bid.py

import pandas as pd

# 读取CSV数据
file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\预处理.csv"
data = pd.read_csv(file_path)

# 找到满足条件的广告位
filtered_data = []
grouped = data.groupby('campaignId')

for campaignId, group in grouped:
    group['ACOS_7d_min'] = group['ACOS_7d'].min()
    group['ACOS_3d_min'] = group['ACOS_3d'].min()
    
    if len(group[(group['ACOS_7d'] == group['ACOS_7d_min']) & (group['ACOS_7d'] > 0) & (group['ACOS_7d'] <= 0.24) & (group['total_clicks_7d'] != group['total_clicks_7d'].max())]) > 0 and \
       len(group[(group['ACOS_3d'] == group['ACOS_3d_min']) & (group['ACOS_3d'] > 0) & (group['ACOS_3d'] <= 0.24) & (group['total_clicks_3d'] != group['total_clicks_3d'].max())]) > 0:
        
        for idx, row in group.iterrows():
            if row['ACOS_7d'] == row['ACOS_7d_min'] and row['ACOS_3d'] == row['ACOS_3d_min'] and \
               row['total_clicks_7d'] != group['total_clicks_7d'].max() and row['total_clicks_3d'] != group['total_clicks_3d'].max() and \
               0 < row['ACOS_7d'] <= 0.24 and 0 < row['ACOS_3d'] <= 0.24:
                
                new_bid = min(row['bid'] * 1.05, row['bid'] * 1.50)
                
                filtered_data.append({
                    'campaignName': row['campaignName'],
                    'campaignId': row['campaignId'],
                    'placementClassification': row['placementClassification'],
                    'ACOS_7d': row['ACOS_7d'],
                    'ACOS_3d': row['ACOS_3d'],
                    'total_clicks_7d': row['total_clicks_7d'],
                    'total_clicks_3d': row['total_clicks_3d'],
                    'bid': row['bid'],
                    'new_bid': new_bid,
                    'reason': '最近7天和3天的平均ACOS值最小，且分别在0到0.24之间，且点击次数不是最大'
                })

# 转换为DataFrame并保存为新的CSV文件
result_df = pd.DataFrame(filtered_data)
result_file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\提问策略\自动_优质广告位_v1_1_ES_2024-06-18.csv"
result_df.to_csv(result_file_path, index=False)

print(f"结果已成功保存到: {result_file_path}")