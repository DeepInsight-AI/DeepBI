# filename: process_ads_placement.py

import pandas as pd
import os

# 加载 CSV 文件
file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\预处理.csv"
data = pd.read_csv(file_path)

# 确保数据包含所需的字段
required_fields = ["placementClassification", "campaignName", "ACOS_7d", "ACOS_3d", "total_clicks_7d", "total_clicks_3d"]
missing_fields = [field for field in required_fields if field not in data.columns]
if missing_fields:
    raise ValueError(f"缺少以下字段: {', '.join(missing_fields)}")

# 按广告活动分组
grouped = data.groupby('campaignName')

# 筛选出符合条件的广告位
output_data = []
for name, group in grouped:
    # 获取最近7天和3天的平均ACOS值和点击次数
    group = group[group["ACOS_7d"].between(0, 0.24) & group["ACOS_3d"].between(0, 0.24)]
    if len(group) < 3:
        continue
    
    group_sorted_by_acos_7d = group.sort_values("ACOS_7d")
    group_sorted_by_acos_3d = group.sort_values("ACOS_3d")
    
    lowest_acos_7d = group_sorted_by_acos_7d.iloc[0]
    lowest_acos_3d = group_sorted_by_acos_3d.iloc[0]

    max_clicks_7d = group["total_clicks_7d"].max()
    max_clicks_3d = group["total_clicks_3d"].max()

    if lowest_acos_7d["total_clicks_7d"] != max_clicks_7d and lowest_acos_3d["total_clicks_3d"] != max_clicks_3d:
        output_data.append({
            "campaignName": name,
            "广告位": lowest_acos_7d["placementClassification"],
            "最近7天的平均ACOS值": lowest_acos_7d["ACOS_7d"],
            "最近3天的平均ACOS值": lowest_acos_3d["ACOS_3d"],
            "最近7天的总点击次数": lowest_acos_7d["total_clicks_7d"],
            "最近3天的总点击次数": lowest_acos_3d["total_clicks_3d"],
            "原因": "符合定义一，竞价提高5%"
        })

# 保存结果
output_df = pd.DataFrame(output_data)
output_file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\提问策略\自动_优质广告位_ES_2024-06-05.csv"
os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
output_df.to_csv(output_file_path, index=False)

print(f"结果已保存到: {output_file_path}")