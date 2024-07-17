# filename: manual_asin_poor_performance_v1_1_LAPASA_DE.py

import pandas as pd

# 数据路径
csv_file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv"

# 结果输出路径
output_file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_劣质商品投放_v1_1_LAPASA_DE_2024-07-10.csv"

# 读取数据
data = pd.read_csv(csv_file_path)

# 创建一个空的DataFrame来存储结果
columns = [
    "keyword", "keywordId", "campaignName", "adGroupName", "matchType", "keywordBid", 
    "new_keywordBid", "targeting", "total_cost_7d", "total_sales14d_7d", "total_cost_30d", 
    "ACOS_7d", "ACOS_30d", "total_clicks_30d", "action", "reason"
]
result = pd.DataFrame(columns=columns)

# 应用规则
rows_list = []
for index, row in data.iterrows():
    reason = None
    action = None
    new_keywordBid = None

    if row['ACOS_7d'] > 0.24 and row['ACOS_7d'] <= 0.5 and row['ACOS_30d'] > 0 and row['ACOS_30d'] <= 0.5:
        new_keywordBid = row['keywordBid'] / ((row['ACOS_7d'] - 0.24) / 0.24 + 1)
        reason = "定义一"
        action = "Adjust Bid"
    elif row['ACOS_7d'] > 0.5 and row['ACOS_30d'] <= 0.36:
        new_keywordBid = row['keywordBid'] / ((row['ACOS_7d'] - 0.24) / 0.24 + 1)
        reason = "定义二"
        action = "Adjust Bid"
    elif row['total_clicks_7d'] >= 10 and row['total_sales14d_7d'] == 0 and row['ACOS_30d'] <= 0.36:
        new_keywordBid = max(row['keywordBid'] - 0.04, 0)  # 确保竞价不为负值
        reason = "定义三"
        action = "Lower Bid"
    elif row['total_clicks_7d'] >= 10 and row['total_sales14d_7d'] == 0 and row['ACOS_30d'] > 0.5:
        new_keywordBid = "关闭"
        reason = "定义四"
        action = "Pause"
    elif row['ACOS_7d'] > 0.5 and row['ACOS_30d'] > 0.36:
        new_keywordBid = "关闭"
        reason = "定义五"
        action = "Pause"
    elif row['total_sales14d_30d'] == 0 and row['total_cost_30d'] >= 5:
        new_keywordBid = "关闭"
        reason = "定义六"
        action = "Pause"
    elif row['total_sales14d_30d'] == 0 and row['total_clicks_30d'] >= 15 and row['total_clicks_7d'] > 0:
        new_keywordBid = "关闭"
        reason = "定义七"
        action = "Pause"
    
    if reason is not None and action is not None:
        rows_list.append({
            "keyword": row["keyword"], "keywordId": row["keywordId"], "campaignName": row["campaignName"], 
            "adGroupName": row["adGroupName"], "matchType": row["matchType"], "keywordBid": row["keywordBid"], 
            "new_keywordBid": new_keywordBid, "targeting": row["targeting"], "total_cost_7d": row["total_cost_7d"], 
            "total_sales14d_7d": row["total_sales14d_7d"], "total_cost_30d": row["total_cost_30d"], 
            "ACOS_7d": row["ACOS_7d"], "ACOS_30d": row["ACOS_30d"], "total_clicks_30d": row["total_clicks_30d"], 
            "action": action, "reason": reason
        })

# 将所有行加入结果 DataFrame
if rows_list:
    result = pd.concat([result, pd.DataFrame(rows_list)])

# 保存结果到CSV文件
result.to_csv(output_file_path, index=False, encoding='utf-8-sig')

print("结果已保存到", output_file_path)