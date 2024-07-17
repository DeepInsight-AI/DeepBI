# filename: process_product_bids.py
import pandas as pd

# 读取数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv'
data = pd.read_csv(file_path)

# 定义提价标准
def adjust_bid(row):
    new_bid = row['keywordBid']
    reason = ""

    if 0 < row['ACOS_7d'] <= 0.1 and 0 < row['ACOS_30d'] <= 0.1 and row['ORDER_1m'] >= 2:
        new_bid += 0.05
        reason = "定义一"
    elif 0 < row['ACOS_7d'] <= 0.1 and 0.1 < row['ACOS_30d'] <= 0.24 and row['ORDER_1m'] >= 2:
        new_bid += 0.03
        reason = "定义二"
    elif 0.1 < row['ACOS_7d'] <= 0.2 and 0 < row['ACOS_30d'] <= 0.1 and row['ORDER_1m'] >= 2:
        new_bid += 0.04
        reason = "定义三"
    elif 0.1 < row['ACOS_7d'] <= 0.2 and 0.1 < row['ACOS_30d'] <= 0.24 and row['ORDER_1m'] >= 2:
        new_bid += 0.02
        reason = "定义四"
    elif 0.2 < row['ACOS_7d'] <= 0.24 and 0 < row['ACOS_30d'] < 0.1 and row['ORDER_1m'] >= 2:
        new_bid += 0.02
        reason = "定义五"
    elif 0.2 < row['ACOS_7d'] <= 0.24 and 0.1 < row['ACOS_30d'] <= 0.24 and row['ORDER_1m'] >= 2:
        new_bid += 0.01
        reason = "定义六"

    return pd.Series([new_bid, reason])

# 筛选并添加字段
data[['New_keywordBid', '提价原因']] = data.apply(adjust_bid, axis=1)
filtered_data = data[data['提价原因'] != ""]

# 输出结果到CSV文件
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_优质商品投放_v1_1_LAPASA_ES_2024-07-03.csv'
filtered_data.to_csv(output_path, index=False)

print(f"Data processing complete. Results saved to {output_path}.")