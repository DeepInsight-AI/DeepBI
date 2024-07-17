# filename: process_campaign_data.py

import pandas as pd

# 读取 CSV 数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv'
df = pd.read_csv(file_path)

# 增加新的竞价列、新的提价值和提价原因
df['New_keywordBid'] = df['keywordBid']
df['提价值'] = 0
df['提价原因'] = ""

# 定义提价逻辑
def adjust_bid(row):
    if 0 < row['ACOS_7d'] <= 0.1 and 0 < row['ACOS_30d'] <= 0.1 and row['ORDER_1m'] >= 2:
        return 0.05, "定义一"
    elif 0 < row['ACOS_7d'] <= 0.1 and 0.1 < row['ACOS_30d'] <= 0.24 and row['ORDER_1m'] >= 2:
        return 0.03, "定义二"
    elif 0.1 < row['ACOS_7d'] <= 0.2 and row['ACOS_30d'] <= 0.1 and row['ORDER_1m'] >= 2:
        return 0.04, "定义三"
    elif 0.1 < row['ACOS_7d'] <= 0.2 and 0.1 < row['ACOS_30d'] <= 0.24 and row['ORDER_1m'] >= 2:
        return 0.02, "定义四"
    elif 0.2 < row['ACOS_7d'] <= 0.24 and row['ACOS_30d'] <= 0.1 and row['ORDER_1m'] >= 2:
        return 0.02, "定义五"
    elif 0.2 < row['ACOS_7d'] <= 0.24 and 0.1 < row['ACOS_30d'] <= 0.24 and row['ORDER_1m'] >= 2:
        return 0.01, "定义六"
    else:
        return 0, ""

# 应用提价逻辑
for index, row in df.iterrows():
    increment, reason = adjust_bid(row)
    if increment > 0:
        df.at[index, 'New_keywordBid'] = row['keywordBid'] + increment
        df.at[index, '提价值'] = increment
        df.at[index, '提价原因'] = reason

# 筛选出需要输出的列
output_df = df[df['提价值'] > 0][[
    'keyword', 'keywordId', 'campaignName', 'adGroupName', 'matchType', 
    'keywordBid', 'New_keywordBid', 'targeting', 'total_cost_30d', 'total_clicks_7d', 
    'ACOS_7d', 'ACOS_30d', 'ORDER_1m', '提价值', '提价原因'
]]

# 保存结果到新的CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_优质商品投放_v1_1_LAPASA_US_2024-07-09.csv'
output_df.to_csv(output_file_path, index=False, encoding='utf-8-sig')

print(f"处理完成，输出文件保存在 {output_file_path}")