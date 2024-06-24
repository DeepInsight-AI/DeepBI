# filename: 优质广告位_ES_2024-6-02.py

import pandas as pd

# 读取CSV文件
df = pd.read_csv("C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\自动sp广告\\广告位优化\\预处理.csv")

# 条件1: 最近7天的平均ACOS值最小，但最近7天点击次数不是最大的广告位
df7 = df[(df['ACOS_7d'] > 0) & (df['ACOS_7d'] <= 0.24)]
df7 = df7.sort_values(['campaignName', 'campaignName', 'total_clicks_7d'])
df7_group = df7.groupby('campaignName').agg({
    'ACOS_7d': 'min',
    'total_clicks_7d': 'max',
}).reset_index()

# 获取符合条件的广告位
condition1 = df7.merge(df7_group, on='campaignName')
condition1 = condition1[(condition1['ACOS_7d_x'] == condition1['ACOS_7d_y']) & (condition1['total_clicks_7d_x'] != condition1['total_clicks_7d_y'])]

# 条件2: 最近3天的平均ACOS值最小，但最近3天点击次数不是最大的广告位
df3 = df[(df['ACOS_3d'] > 0) & (df['ACOS_3d'] <= 0.24)]
df3 = df3.sort_values(['campaignName', 'campaignName', 'total_clicks_3d'])
df3_group = df3.groupby('campaignName').agg({
    'ACOS_3d': 'min',
    'total_clicks_3d': 'max',
}).reset_index()

# 获取符合条件的广告位
condition2 = df3.merge(df3_group, on='campaignName')
condition2 = condition2[(condition2['ACOS_3d_x'] == condition2['ACOS_3d_y']) & (condition2['total_clicks_3d_x'] != condition2['total_clicks_3d_y'])]

# 设定竞价操作并合并条件
result = pd.merge(condition1, condition2, on=['campaignName', 'placementClassification'])

result['bid_increase'] = (result['total_clicks_7d_x'] * 1.05).apply(lambda x: x if x <= 1.5 else 1.5)
result['reason'] = "符合定义一（满足最近7天和最近3天点击次数的条件）"

# CSV文件保存路径
output_file = "C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\自动sp广告\\广告位优化\\提问策略\\优质广告位_ES_2024-6-02.csv"

# 保存结果至CSV文件
result[['campaignName', 'placementClassification', 'ACOS_7d_x', 'ACOS_3d_x', 'total_clicks_7d_x', 'total_clicks_3d_x', 'bid_increase', 'reason']].to_csv(output_file, index=False)

print(f"结果已保存至 {output_file}")