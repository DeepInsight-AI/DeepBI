# filename: simulate_csv_with_data.py
import pandas as pd

# 设置文件路径
input_filepath = "C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/手动sp广告/特殊商品投放/预处理.csv"

# 创建模拟数据
data = {
    "campaignName": ["Campaign1", "Campaign2", "Campaign1", "Campaign3", "Campaign2"],
    "adGroupName": ["Group1", "Group1", "Group2", "Group1", "Group2"],
    "total_sales_15d": [0, 0, 10, 0, 0],
    "total_clicks_7d": [5, 11, 15, 12, 1],
    "keyword": ["Keyword1", "Keyword2", "Keyword3", "Keyword4", "Keyword5"],
    "matchType": ["Type1", "Type2", "Type3", "Type4", "Type5"],
    "keywordBid": [0.5, 0.3, 0.4, 0.7, 0.6],
    "keywordId": [1001, 1002, 1003, 1004, 1005]
}

# 创建DataFrame并保存为CSV文件
df = pd.DataFrame(data)
df.to_csv(input_filepath, index=False)

print("模拟数据已写入到CSV文件。")