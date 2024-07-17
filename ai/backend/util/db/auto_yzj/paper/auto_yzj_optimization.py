# filename: auto_yzj_optimization.py

import pandas as pd

# 读取CSV数据
data_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv'
df = pd.read_csv(data_path)

# 定义需要处理的字段
fields_to_include = [
    "keyword", "keywordId", "campaignName", "adGroupName",
    "matchType", "keywordBid", "total_clicks_7d", "total_clicks_30d",
    "total_sales14d_7d", "total_sales14d_30d", "total_cost_7d", "total_cost_30d",
    "ACOS_7d", "ACOS_30d"
]

filtered_data = []

# 定义满足条件的商品投放
for _, row in df.iterrows():
    if 0.24 < row['ACOS_7d'] <= 0.5 and 0 < row['ACOS_30d'] <= 0.5:
        new_keywordBid = row['keywordBid'] / ((row['ACOS_7d'] - 0.24) / 0.24 + 1)
        reason = "定义一：调整竞价"
    elif row['ACOS_7d'] > 0.5 and row['ACOS_30d'] <= 0.36:
        new_keywordBid = row['keywordBid'] / ((row['ACOS_7d'] - 0.24) / 0.24 + 1)
        reason = "定义二：调整竞价"
    elif row['total_clicks_7d'] >= 10 and row['total_sales14d_7d'] == 0 and row['ACOS_30d'] <= 0.36:
        new_keywordBid = row['keywordBid'] - 0.04
        reason = "定义三：降低竞价"
    elif row['total_clicks_7d'] >= 10 and row['total_sales14d_7d'] == 0 and row['ACOS_30d'] > 0.5:
        new_keywordBid = "关闭"
        reason = "定义四：关闭该词"
    elif row['ACOS_7d'] > 0.5 and row['ACOS_30d'] > 0.36:
        new_keywordBid = "关闭"
        reason = "定义五：关闭该词"
    elif row['total_sales14d_30d'] == 0 and row['total_cost_30d'] >= 5:
        new_keywordBid = "关闭"
        reason = "定义六：关闭该词"
    elif row['total_sales14d_30d'] == 0 and row['total_clicks_30d'] >= 15 and row['total_clicks_7d'] > 0:
        new_keywordBid = "关闭"
        reason = "定义七：关闭该词"
    else:
        continue

    filtered_data.append({
        **{field: row[field] for field in fields_to_include},
        "New_keywordBid": new_keywordBid,
        "reason": reason
    })

# 转换为DataFrame并保存为CSV
output_df = pd.DataFrame(filtered_data)
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_劣质商品投放_v1_1_OutdoorMaster_FR_2024-07-09.csv'
output_df.to_csv(output_path, index=False)

print("处理完成，结果保存在: " + output_path)