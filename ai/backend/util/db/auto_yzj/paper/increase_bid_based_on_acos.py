# filename: increase_bid_based_on_acos.py

import pandas as pd

# 读取数据集
input_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\预处理.csv'
df = pd.read_csv(input_file_path)

output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\提问策略\自动_优质广告位_v1_1_LAPASA_FR_2024-07-12.csv'

# 初始化 adjusted_bid 列和 reason 列
df['adjusted_bid'] = df['bid']
df['reason'] = ''

# 定义竞价调整逻辑
def adjust_bid(row):
    """
    对满足条件的广告位提高竞价5，直到50
    """
    if row['bid'] < 50:
        row['adjusted_bid'] = min(row['bid'] + 5, 50)
    return row

# 判断是否符合定义一或定义二
def identified_ad_placement(row):
    max_clicks_7d = df[df['campaignId'] == row['campaignId']]['total_clicks_7d'].max()
    max_clicks_3d = df[df['campaignId'] == row['campaignId']]['total_clicks_3d'].max()
    min_acos_3d = df[(df['campaignId'] == row['campaignId']) & (df['placementClassification'] == row['placementClassification'])]['ACOS_3d'].min()

    if (row['ACOS_3d'] > 0 and row['ACOS_3d'] <= 0.24 and
        row['ACOS_7d'] > 0 and row['ACOS_7d'] <= 0.24 and
        row['total_clicks_7d'] < max_clicks_7d and
        row['total_clicks_3d'] < max_clicks_3d
    ):
        if row['total_cost_3d'] < 4:
            row['reason'] = "定义二"
            row = adjust_bid(row)
        elif row['ACOS_3d'] == min_acos_3d and row['total_clicks_3d'] < max_clicks_3d:
            row['reason'] = "定义一"
            row = adjust_bid(row)
        return row

    row['reason'] = ''
    return row

# 筛选匹配的广告位并进行调整
filtered_df = df.apply(identified_ad_placement, axis=1)
result_df = filtered_df[filtered_df['reason'] != '']

# 保持所需列和命名
columns = ['campaignName', 'campaignId', 'placementClassification', 'ACOS_7d', 'ACOS_3d', 'total_clicks_7d', 'total_clicks_3d', 'bid', 'adjusted_bid', 'reason']
result_df = result_df[columns]

# 确保 result_df 包含所有所需的列
missing_columns = set(columns) - set(result_df.columns)
if missing_columns:
    raise ValueError(f"缺少的列: {missing_columns}")

# 保存结果到新的CSV文件
result_df.to_csv(output_file_path, index=False)

print(f"数据已经处理完毕并保存到文件: {output_file_path}")