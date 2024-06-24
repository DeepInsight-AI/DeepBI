# filename: process_ad_placements.py

import pandas as pd

# 读取CSV文件的位置
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\预处理.csv'

# 读取CSV文件到DataFrame
df = pd.read_csv(file_path)

# 确保所有必要的字段都在数据集中
required_columns = [
    'placementClassification', 'campaignName', 'campaignId', 'total_clicks_3d',
    'total_clicks_7d', 'ACOS_3d', 'ACOS_7d'
]

# 检查是否缺少必须的字段
missing_columns = [col for col in required_columns if col not in df.columns]
if missing_columns:
    raise ValueError(f"缺少必要的字段: {', '.join(missing_columns)}")

# 筛选符合定义一条件的广告位
filtered_df = df.groupby('campaignId').apply(
    lambda x: x[
        (x['ACOS_7d'] > 0) & (x['ACOS_7d'] <= 0.24) & 
        (x['ACOS_3d'] > 0) & (x['ACOS_3d'] <= 0.24) & 
        (x['ACOS_7d'] == x['ACOS_7d'].min()) & 
        (x['ACOS_3d'] == x['ACOS_3d'].min()) & 
        (x['total_clicks_7d'] != x['total_clicks_7d'].max()) & 
        (x['total_clicks_3d'] != x['total_clicks_3d'].max())
    ]
).reset_index(drop=True)

# 增加竞价调整列和原因列
filtered_df['bid_adjustment'] = "增加5%"
filtered_df['reason'] = "ACOS值在范围内且点击次数不最大"

# 选择需要的列
output_df = filtered_df[[
    'campaignName', 'campaignId', 'placementClassification', 'ACOS_7d', 'ACOS_3d',
    'total_clicks_7d', 'total_clicks_3d', 'bid_adjustment', 'reason'
]]

# 保存结果到新的CSV文件
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\提问策略\手动_优质广告位_v1_1_IT_2024-06-13.csv'
output_df.to_csv(output_path, index=False, encoding='utf-8-sig')

print("CSV 文件已保存到:", output_path)