# filename: campaign_bid_adjustment.py

import pandas as pd

# 读取CSV文件
file_path = "C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/手动sp广告/商品投放优化/预处理.csv"
data = pd.read_csv(file_path)

# 定义符合条件的数据处理函数
def adjust_bids(row):
    if row['ACOS_7d'] > 0 and row['ACOS_7d'] <= 0.1:
        if row['ACOS_30d'] > 0 and row['ACOS_30d'] <= 0.1 and row['ORDER_1m'] >= 2:
            return row['keywordBid'] + 0.05, "定义一: 提价0.05"
        elif row['ACOS_30d'] > 0.1 and row['ACOS_30d'] <= 0.24 and row['ORDER_1m'] >= 2:
            return row['keywordBid'] + 0.03, "定义二: 提价0.03"
    elif row['ACOS_7d'] > 0.1 and row['ACOS_7d'] <= 0.2:
        if row['ACOS_30d'] > 0 and row['ACOS_30d'] <= 0.1 and row['ORDER_1m'] >= 2:
            return row['keywordBid'] + 0.04, "定义三: 提价0.04"
        elif row['ACOS_30d'] > 0.1 and row['ACOS_30d'] <= 0.24 and row['ORDER_1m'] >= 2:
            return row['keywordBid'] + 0.02, "定义四: 提价0.02"
    elif row['ACOS_7d'] > 0.2 and row['ACOS_7d'] <= 0.24:
        if row['ACOS_30d'] > 0 and row['ACOS_30d'] <= 0.1 and row['ORDER_1m'] >= 2:
            return row['keywordBid'] + 0.02, "定义五: 提价0.02"
        elif row['ACOS_30d'] > 0.1 and row['ACOS_30d'] <= 0.24 and row['ORDER_1m'] >= 2:
            return row['keywordBid'] + 0.01, "定义六: 提价0.01"
    return row['keywordBid'], None

# 对数据进行处理
data[['New_keywordBid', '调价原因']] = data.apply(adjust_bids, axis=1, result_type="expand")

# 过滤出符合条件的记录
filtered_data = data[data['调价原因'].notna()]

# 输出结果到新CSV文件
output_file_path = "C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/手动sp广告/商品投放优化/提问策略/手动_ASIN_优质商品投放_v1_1_DE_2024-06-30.csv"
filtered_data.to_csv(output_file_path, index=False)

print(f"调整后的数据已保存至 {output_file_path}")