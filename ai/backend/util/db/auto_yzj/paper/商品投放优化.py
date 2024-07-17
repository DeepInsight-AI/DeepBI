# filename: 商品投放优化.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv'
data = pd.read_csv(file_path)

# 定义提价规则
def apply_bid_increase(row):
    reason = ""
    if 0 < row['ACOS_7d'] <= 0.1 and 0 < row['ACOS_30d'] <= 0.1 and row['ORDER_1m'] >= 2:
        row['New_keywordBid'] = row['keywordBid'] + 0.05
        reason = "定义一"
    elif 0 < row['ACOS_7d'] <= 0.1 and 0.1 < row['ACOS_30d'] <= 0.24 and row['ORDER_1m'] >= 2:
        row['New_keywordBid'] = row['keywordBid'] + 0.03
        reason = "定义二"
    elif 0.1 < row['ACOS_7d'] <= 0.2 and 0 < row['ACOS_30d'] <= 0.1 and row['ORDER_1m'] >= 2:
        row['New_keywordBid'] = row['keywordBid'] + 0.04
        reason = "定义三"
    elif 0.1 < row['ACOS_7d'] <= 0.2 and 0.1 < row['ACOS_30d'] <= 0.24 and row['ORDER_1m'] >= 2:
        row['New_keywordBid'] = row['keywordBid'] + 0.02
        reason = "定义四"
    elif 0.2 < row['ACOS_7d'] <= 0.24 and row['ACOS_30d'] <= 0.1 and row['ORDER_1m'] >= 2:
        row['New_keywordBid'] = row['keywordBid'] + 0.02
        reason = "定义五"
    elif 0.2 < row['ACOS_7d'] <= 0.24 and 0.1 < row['ACOS_30d'] <= 0.24 and row['ORDER_1m'] >= 2:
        row['New_keywordBid'] = row['keywordBid'] + 0.01
        reason = "定义六"

    return row, reason

# 处理数据
result = []
for idx, row in data.iterrows():
    processed_row, reason = apply_bid_increase(row)
    if reason:
        processed_row['提价原因'] = reason
        result.append(processed_row)

# 转换为DataFrame
result_df = pd.DataFrame(result)

# 选择最终需要的字段
result_columns = [
    'keyword', 'keywordId', 'campaignName', 'adGroupName', 'matchType', 'keywordBid', 'New_keywordBid', 'targeting',
    'total_cost_30d', 'total_clicks_7d', 'ACOS_7d', 'ACOS_30d', 'ORDER_1m', '提价原因'
]
output_df = result_df[result_columns]

# 保存到新的CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_优质商品投放_v1_1_LAPASA_UK_2024-07-03.csv'
output_df.to_csv(output_file_path, index=False)

print(f"数据已保存到 {output_file_path}")