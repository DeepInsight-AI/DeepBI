# filename: optimal_ad_placements.py

import pandas as pd
import os

# 读取CSV文件
file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\预处理.csv"
df = pd.read_csv(file_path)

# 数据准备
df['avg_ACOS_7d'] = df.groupby('placementClassification')['ACOS_7d'].transform('mean')
df['avg_ACOS_3d'] = df.groupby('placementClassification')['ACOS_3d'].transform('mean')

# 满足条件1
condition1 = (df['avg_ACOS_7d'] == df.groupby('campaignName')['avg_ACOS_7d'].transform('min')) & \
             (df['total_clicks_7d'] != df.groupby('campaignName')['total_clicks_7d'].transform('max')) & \
             (df['avg_ACOS_7d'] > 0) & (df['avg_ACOS_7d'] <= 24)

# 满足条件2
condition2 = (df['avg_ACOS_3d'] == df.groupby('campaignName')['avg_ACOS_3d'].transform('min')) & \
             (df['total_clicks_3d'] != df.groupby('campaignName')['total_clicks_3d'].transform('max')) & \
             (df['avg_ACOS_3d'] > 0) & (df['avg_ACOS_3d'] <= 24)

# 选择满足条件1和条件2的广告位
selected_df = df[condition1 & condition2].copy()

# 增加竞价操作的原因
selected_df['reason'] = "竞价提高5%，直到50%"

# 输出结果到新的CSV文件
output_folder = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\提问策略"
os.makedirs(output_folder, exist_ok=True)
output_file = os.path.join(output_folder, "优质广告位_FR.csv")
selected_df[['campaignName', 'placementClassification', 'avg_ACOS_7d', 'avg_ACOS_3d', 'total_clicks_7d', 'total_clicks_3d', 'reason']].to_csv(output_file, index=False)

print("CSV文件已经成功生成并保存至：", output_file)