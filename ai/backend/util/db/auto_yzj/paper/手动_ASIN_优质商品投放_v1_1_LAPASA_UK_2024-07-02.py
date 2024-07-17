# filename: 手动_ASIN_优质商品投放_v1_1_LAPASA_UK_2024-07-02.py

import pandas as pd

# 读取数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv'
data = pd.read_csv(file_path)

# 定义提价策略
def adjust_bid(row):
    # 获取所需字段值
    acos_7d = row['ACOS_7d']
    acos_30d = row['ACOS_30d']
    orders_1m = row['ORDER_1m']
    current_bid = row['keywordBid']
    increase = 0
    reason = ""

    # 定义一
    if 0 < acos_7d <= 0.1 and 0 < acos_30d <= 0.1 and orders_1m >= 2:
        increase = 0.05
        reason = "定义一"
    # 定义二
    elif 0 < acos_7d <= 0.1 and 0.1 < acos_30d <= 0.24 and orders_1m >= 2:
        increase = 0.03
        reason = "定义二"
    # 定义三
    elif 0.1 < acos_7d <= 0.2 and acos_30d <= 0.1 and orders_1m >= 2:
        increase = 0.04
        reason = "定义三"
    # 定义四
    elif 0.1 < acos_7d <= 0.2 and 0.1 < acos_30d <= 0.24 and orders_1m >= 2:
        increase = 0.02
        reason = "定义四"
    # 定义五
    elif 0.2 < acos_7d <= 0.24 and acos_30d <= 0.1 and orders_1m >= 2:
        increase = 0.02
        reason = "定义五"
    # 定义六
    elif 0.2 < acos_7d <= 0.24 and 0.1 < acos_30d <= 0.24 and orders_1m >= 2:
        increase = 0.01
        reason = "定义六"

    if increase > 0:
        new_bid = current_bid + increase
    else:
        new_bid = current_bid
    
    return pd.Series([new_bid, increase, reason])

# 调整竞价并筛选出符合条件的商品投放
data[['New_keywordBid', 'Increase', 'Reason']] = data.apply(adjust_bid, axis=1)

# 筛选出竞价提高的记录
filtered_data = data[data['Increase'] > 0]

# 保存结果
output_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_优质商品投放_v1_1_LAPASA_UK_2024-07-02.csv'
filtered_data.to_csv(output_file, index=False, encoding='utf-8-sig')

print(f"结果已保存到：{output_file}")