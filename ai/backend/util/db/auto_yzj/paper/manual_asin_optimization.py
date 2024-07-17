# filename: manual_asin_optimization.py

import pandas as pd

# 读取数据集
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv'
data = pd.read_csv(file_path)

# 筛选和计算新出价
def determine_new_bid(row):
    if (0 < row['ACOS_7d'] <= 0.1) and (0 < row['ACOS_30d'] <= 0.1) and (row['ORDER_1m'] >= 2):
        return row['keywordBid'] + 0.05, "Definition 1"
    elif (0 < row['ACOS_7d'] <= 0.1) and (0.1 < row['ACOS_30d'] <= 0.24) and (row['ORDER_1m'] >= 2):
        return row['keywordBid'] + 0.03, "Definition 2"
    elif (0.1 < row['ACOS_7d'] <= 0.2) and (0 < row['ACOS_30d'] <= 0.1) and (row['ORDER_1m'] >= 2):
        return row['keywordBid'] + 0.04, "Definition 3"
    elif (0.1 < row['ACOS_7d'] <= 0.2) and (0.1 < row['ACOS_30d'] <= 0.24) and (row['ORDER_1m'] >= 2):
        return row['keywordBid'] + 0.02, "Definition 4"
    elif (0.2 < row['ACOS_7d'] <= 0.24) and (0 < row['ACOS_30d'] <= 0.1) and (row['ORDER_1m'] >= 2):
        return row['keywordBid'] + 0.02, "Definition 5"
    elif (0.2 < row['ACOS_7d'] <= 0.24) and (0.1 < row['ACOS_30d'] <= 0.24) and (row['ORDER_1m'] >= 2):
        return row['keywordBid'] + 0.01, "Definition 6"
    return row['keywordBid'], None

data[['New Bid', 'Reason']] = data.apply(lambda row: pd.Series(determine_new_bid(row)), axis=1)
filtered_data = data[data['Reason'].notnull()]

# 输出结果到CSV
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_优质商品投放_v1_1_DE_2024-06-27.csv'
filtered_data.to_csv(output_file_path, index=False, encoding='utf-8-sig')

print("筛选并保存完成，输出文件路径:", output_file_path)