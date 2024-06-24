# filename: optimize_ads.py
import pandas as pd

# 读取CSV文件
file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\预处理.csv"
data = pd.read_csv(file_path)

# 定义条件
condition_7d = (data['ACOS_7d'] > 0) & (data['ACOS_7d'] <= 0.24)
condition_3d = (data['ACOS_3d'] > 0) & (data['ACOS_3d'] <= 0.24)

# 分组按广告活动
grouped = data.groupby('campaignId')

result = []
for name, group in grouped:
    # 最近7天的最小ACOS广告位，排除NaN值来寻找最小值
    group_7d_clean = group.dropna(subset=['ACOS_7d'])
    group_3d_clean = group.dropna(subset=['ACOS_3d'])
    
    if group_7d_clean.empty or group_3d_clean.empty:
        continue
    
    min_acos_7d_row = group_7d_clean.loc[group_7d_clean['ACOS_7d'].idxmin()]
    min_acos_3d_row = group_3d_clean.loc[group_3d_clean['ACOS_3d'].idxmin()]
    
    if (condition_7d[min_acos_7d_row.name] and condition_3d[min_acos_3d_row.name]):
        max_clicks_7d = group['total_clicks_7d'].max()
        max_clicks_3d = group['total_clicks_3d'].max()
        
        if (min_acos_7d_row['total_clicks_7d'] != max_clicks_7d and 
            min_acos_3d_row['total_clicks_3d'] != max_clicks_3d):
            
            reason = "ACOS符合条件，平均ACOS值最低, 并且最近总点击次数不是最大"
            new_bid = min(min_acos_7d_row['bid'] + 5, 50)
            
            # 把需要的数据收集起来
            result.append({
                'campaignName': min_acos_7d_row['campaignName'],
                'campaignId': name,
                'placementClassification': min_acos_7d_row['placementClassification'],
                'ACOS_7d': min_acos_7d_row['ACOS_7d'],
                'ACOS_3d': min_acos_7d_row['ACOS_3d'],
                'total_clicks_7d': min_acos_7d_row['total_clicks_7d'],
                'total_clicks_3d': min_acos_7d_row['total_clicks_3d'],
                'bid': min_acos_7d_row['bid'],
                'new_bid': new_bid,
                'reason': reason
            })

# 转换为DataFrame
result_df = pd.DataFrame(result)

# 保存结果到CSV
output_file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\提问策略\手动_优质广告位_v1_1_IT_2024-06-21.csv"
result_df.to_csv(output_file_path, index=False)

print(f"Result saved to {output_file_path}")
