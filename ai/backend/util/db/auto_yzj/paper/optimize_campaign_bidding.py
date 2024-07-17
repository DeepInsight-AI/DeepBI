# filename: optimize_campaign_bidding.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv'
data = pd.read_csv(file_path)

# 定义提价理由以及提价金额
def classify_and_bid(row):
    reasons = []
    new_bid = row['keywordBid']
    
    # 定义一
    if 0 < row['ACOS_7d'] <= 0.1 and 0 < row['ACOS_30d'] <= 0.1 and row['ORDER_1m'] >= 2:
        reasons.append("满足定义一")
        new_bid += 0.05
    
    # 定义二
    if 0 < row['ACOS_7d'] <= 0.1 and 0.1 < row['ACOS_30d'] <= 0.24 and row['ORDER_1m'] >= 2:
        reasons.append("满足定义二")
        new_bid += 0.03
    
    # 定义三
    if 0.1 < row['ACOS_7d'] <= 0.2 and row['ACOS_30d'] <= 0.1 and row['ORDER_1m'] >= 2:
        reasons.append("满足定义三")
        new_bid += 0.04
    
    # 定义四
    if 0.1 < row['ACOS_7d'] <= 0.2 and 0.1 < row['ACOS_30d'] <= 0.24 and row['ORDER_1m'] >= 2:
        reasons.append("满足定义四")
        new_bid += 0.02
    
    # 定义五
    if 0.2 < row['ACOS_7d'] <= 0.24 and row['ACOS_30d'] <= 0.1 and row['ORDER_1m'] >= 2:
        reasons.append("满足定义五")
        new_bid += 0.02
    
    # 定义六
    if 0.2 < row['ACOS_7d'] <= 0.24 and 0.1 < row['ACOS_30d'] <= 0.24 and row['ORDER_1m'] >= 2:
        reasons.append("满足定义六")
        new_bid += 0.01

    return new_bid, reasons

# 应用分类函数到数据集
data['New_keywordBid'], data['提价原因'] = zip(*data.apply(classify_and_bid, axis=1))

# 过滤并选择需要的列
filtered_data = data[
    (data['New_keywordBid'] != data['keywordBid']) &
    (data['提价原因'].map(len) > 0)
]

filtered_data['提价原因'] = filtered_data['提价原因'].map(lambda x: ', '.join(x))

# 保存到指定路径
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_优质商品投放_v1_1_LAPASA_US_2024-07-04.csv'
filtered_data.to_csv(output_file_path, index=False)

print(f"处理完成，结果已保存至 {output_file_path}")