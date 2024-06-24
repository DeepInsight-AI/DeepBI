# filename: keyword_analysis.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\关键词优化\预处理.csv'
data = pd.read_csv(file_path)

# 定义关键词提价策略函数
def increase_bid(row):
    if (0 < row['ACOS_7d'] <= 0.1 and 0 < row['ACOS_30d'] <= 0.1 and row['ORDER_1m'] >= 2):
        return row['keywordBid'] + 0.05, "定义一"
    elif (0 < row['ACOS_7d'] <= 0.1 and 0.1 < row['ACOS_30d'] <= 0.24 and row['ORDER_1m'] >= 2):
        return row['keywordBid'] + 0.03, "定义二"
    elif (0.1 < row['ACOS_7d'] <= 0.2 and 0 < row['ACOS_30d'] <= 0.1 and row['ORDER_1m'] >= 2):
        return row['keywordBid'] + 0.04, "定义三"
    elif (0.1 < row['ACOS_7d'] <= 0.2 and 0.1 < row['ACOS_30d'] <= 0.24 and row['ORDER_1m'] >= 2):
        return row['keywordBid'] + 0.02, "定义四"
    elif (0.2 < row['ACOS_7d'] <= 0.24 and 0 < row['ACOS_30d'] <= 0.1 and row['ORDER_1m'] >= 2):
        return row['keywordBid'] + 0.02, "定义五"
    elif (0.2 < row['ACOS_7d'] <= 0.24 and 0.1 < row['ACOS_30d'] <= 0.24 and row['ORDER_1m'] >= 2):
        return row['keywordBid'] + 0.01, "定义六"
    else:
        return row['keywordBid'], None

# 应用关键词提价策略
result_data = []
for _, row in data.iterrows():
    new_bid, reason = increase_bid(row)
    if reason:
        result_row = {
            "keyword": row["keyword"],
            "keywordId": row["keywordId"],
            "campaignName": row["campaignName"],
            "adGroupName": row["adGroupName"],
            "匹配类型": row["matchType"],
            "关键词出价": row["keywordBid"],
            "targeting": row["targeting"],
            "cost": row["total_cost_30d"],
            "clicks": row["total_clicks_30d"],
            "最近7天的平均ACOS值": row["ACOS_7d"],
            "最近一个月的平均ACOS值": row["ACOS_30d"],
            "最近一个月的订单数": row["ORDER_1m"],
            "new_keywordBid": new_bid,
            "提价原因": reason
        }
        result_data.append(result_row)

# 将结果保存到CSV文件
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\关键词优化\提问策略\手动_优质关键词_ES_2024-06-11.csv'
result_df = pd.DataFrame(result_data)
result_df.to_csv(output_path, index=False, encoding='utf-8-sig')

print("结果已保存到：", output_path)