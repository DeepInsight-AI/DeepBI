# filename: find_good_placements.py
import pandas as pd

# 定义文件路径
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\预处理.csv'
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\提问策略\自动_优质广告位_v1_1_ES_2024-06-20.csv'

# 读取CSV文件
df = pd.read_csv(file_path)

# 筛选条件定义
condition_1 = (df['ACOS_7d'] > 0) & (df['ACOS_7d'] <= 0.24)
condition_2 = (df.groupby('campaignName')['ACOS_7d'].transform('min') == df['ACOS_7d'])
condition_3 = (df['total_clicks_7d'] < df.groupby('campaignName')['total_clicks_7d'].transform('max'))
condition_4 = (df['ACOS_3d'] > 0) & (df['ACOS_3d'] <= 0.24)
condition_5 = (df.groupby('campaignName')['ACOS_3d'].transform('min') == df['ACOS_3d'])
condition_6 = (df['total_clicks_3d'] < df.groupby('campaignName')['total_clicks_3d'].transform('max'))

# 综合所有条件
filtered_df = df[condition_1 & condition_2 & condition_3 & condition_4 & condition_5 & condition_6].copy()

# 竞价提高，确保新的竞价不超过原竞价的50%
filtered_df['new_bid'] = filtered_df['bid'] * 1.05
filtered_df['new_bid'] = filtered_df.apply(lambda row: min(row['new_bid'], 1.5 * row['bid']), axis=1)

# 生成原因
filtered_df['reason'] = '最近7天和最近3天的ACOS值均最小且点击数不是最大的广告位'

# 输出我们感兴趣的列
output_df = filtered_df[['campaignName', 'campaignId', 'placementClassification', 'ACOS_7d', 'ACOS_3d', 'total_clicks_7d', 'total_clicks_3d', 'bid', 'new_bid', 'reason']]

# 将数据保存为CSV文件
output_df.to_csv(output_path, index=False)

print(f'Results saved to {output_path}')