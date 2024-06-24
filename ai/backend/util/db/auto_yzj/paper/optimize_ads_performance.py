# filename: optimize_ads_performance.py

import pandas as pd

# Step 1: 读取数据集
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\预处理.csv'
df = pd.read_csv(file_path)

# Step 2: 定义筛选条件和标记符合条件的广告位
def get_best_placements(sub_df):
    result = []
    reasons = []
    
    # 最近7天条件
    min_acos_7d = sub_df['ACOS_7d'].min()
    min_acos_7d_row = sub_df[(sub_df['ACOS_7d'] == min_acos_7d) & (sub_df['ACOS_7d'] > 0) & (sub_df['ACOS_7d'] <= 0.24)]
    max_clicks_7d_row = sub_df[sub_df['total_clicks_7d'] == sub_df['total_clicks_7d'].max()]
    
    if not min_acos_7d_row.empty and not min_acos_7d_row.index.equals(max_clicks_7d_row.index):
        result.append(min_acos_7d_row)
        reasons.append("最近7天平均ACOS值最小，但点击次数不是最多")

    # 最近3天条件
    min_acos_3d = sub_df['ACOS_3d'].min()
    min_acos_3d_row = sub_df[(sub_df['ACOS_3d'] == min_acos_3d) & (sub_df['ACOS_3d'] > 0) & (sub_df['ACOS_3d'] <= 0.24)]
    max_clicks_3d_row = sub_df[sub_df['total_clicks_3d'] == sub_df['total_clicks_3d'].max()]
    
    if not min_acos_3d_row.empty and not min_acos_3d_row.index.equals(max_clicks_3d_row.index):
        result.append(min_acos_3d_row)
        reasons.append("最近3天平均ACOS值最小，但点击次数不是最多")
    
    return result, reasons

grouped = df.groupby('campaignName')
results = []
    
# 筛选符合条件的广告位
for name, group in grouped:
    placements, reasons = get_best_placements(group)
    for i in range(len(placements)):
        placement = placements[i]
        placement['reason'] = reasons[i]
        results.append(placement)

result_df = pd.concat(results)

# Step 3: 生成结果 DataFrame
result_df = result_df[['campaignName', 'placementClassification', 'ACOS_7d', 'ACOS_3d', 'total_clicks_7d', 'total_clicks_3d', 'reason']]

# Step 4: 保存结果
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\提问策略\自动_优质广告位_ES_2024-06-10.csv'
result_df.to_csv(output_file_path, index=False)

print("Results have been saved successfully.")