# filename: analyze_and_bid_adjustment.py

import pandas as pd

# Step 1: 读取数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\预处理.csv'
data = pd.read_csv(file_path)

# 定义今天的日期为2024年5月27日
current_date = pd.to_datetime('2024-05-27')

# 初始化结果数据框
results = []

# Step 2: 执行定义一检查
for index, row in data.iterrows():
    if row['total_sales14d_7d'] == 0 and row['total_clicks_7d'] > 0:
        results.append({
            "campaignName": row['campaignName'],
            "广告位": row['placementClassification'],
            "最近7天的平均ACOS值": row['ACOS_7d'],
            "最近3天的平均ACOS值": row['ACOS_3d'],
            "最近7天的总点击次数": row['total_clicks_7d'],
            "最近3天的总点击次数": row['total_clicks_3d'],
            "竞价操作": "竞价变为0",
            "定义": "定义一"
        })

# Step 3: 执行定义二检查
campaign_groups = data.groupby('campaignName')
for campaign_name, group in campaign_groups:
    acos_values_7d = group['ACOS_7d']
    if acos_values_7d.min() > 24 and acos_values_7d.max() < 50 and (acos_values_7d.max() - acos_values_7d.min() >= 0.2):
        max_acos_row = group.loc[acos_values_7d.idxmax()]
        results.append({
            "campaignName": max_acos_row['campaignName'],
            "广告位": max_acos_row['placementClassification'],
            "最近7天的平均ACOS值": max_acos_row['ACOS_7d'],
            "最近3天的平均ACOS值": max_acos_row['ACOS_3d'],
            "最近7天的总点击次数": max_acos_row['total_clicks_7d'],
            "最近3天的总点击次数": max_acos_row['total_clicks_3d'],
            "竞价操作": "竞价降低3%",
            "定义": "定义二"
        })

# Step 4: 执行定义三检查
for index, row in data.iterrows():
    if row['ACOS_7d'] >= 50:
        results.append({
            "campaignName": row['campaignName'],
            "广告位": row['placementClassification'],
            "最近7天的平均ACOS值": row['ACOS_7d'],
            "最近3天的平均ACOS值": row['ACOS_3d'],
            "最近7天的总点击次数": row['total_clicks_7d'],
            "最近3天的总点击次数": row['total_clicks_3d'],
            "竞价操作": "竞价变为0",
            "定义": "定义三"
        })

# Step 5: 输出结果到CSV文件
results_df = pd.DataFrame(results)
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\提问策略\劣质广告位_ES_2024-6-02.csv'
results_df.to_csv(output_file_path, index=False, encoding='utf-8-sig')

print(f"结果已保存到 {output_file_path}")