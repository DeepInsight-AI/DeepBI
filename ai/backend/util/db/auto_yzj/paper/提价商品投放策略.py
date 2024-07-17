# filename: 提价商品投放策略.py

import pandas as pd

# 读取数据文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv'
data = pd.read_csv(file_path)

# 定义条件
def condition_1(row):
    return (0 < row['ACOS_7d'] <= 0.1 and 
            0 < row['ACOS_30d'] <= 0.1 and 
            row['ORDER_1m'] >= 2 and 
            0 < row['ACOS_3d'] <= 0.2)

def condition_2(row):
    return (0 < row['ACOS_7d'] <= 0.1 and 
            0.1 < row['ACOS_30d'] <= 0.24 and 
            row['ORDER_1m'] >= 2 and 
            0 < row['ACOS_3d'] <= 0.2)

def condition_3(row):
    return (0.1 < row['ACOS_7d'] <= 0.2 and 
            0 < row['ACOS_30d'] <= 0.1 and 
            row['ORDER_1m'] >= 2 and 
            0 < row['ACOS_3d'] <= 0.2)

def condition_4(row):
    return (0.1 < row['ACOS_7d'] <= 0.2 and 
            0.1 < row['ACOS_30d'] <= 0.24 and 
            row['ORDER_1m'] >= 2 and 
            0 < row['ACOS_3d'] <= 0.2)

def condition_5(row):
    return (0.2 < row['ACOS_7d'] <= 0.24 and 
            0 < row['ACOS_30d'] <= 0.1 and 
            row['ORDER_1m'] >= 2 and 
            0 < row['ACOS_3d'] <= 0.2)

def condition_6(row):
    return (0.2 < row['ACOS_7d'] <= 0.24 and 
            0.1 < row['ACOS_30d'] <= 0.24 and 
            row['ORDER_1m'] >= 2 and 
            0 < row['ACOS_3d'] <= 0.2)

# 增加新的竞价和提价原因列
data['New_keywordBid'] = data['keywordBid']
data['提价原因'] = ""

# 定义提价策略
def adjust_bid(row):
    if condition_1(row):
        row['New_keywordBid'] += 0.05
        row['提价原因'] = '定义一'
    elif condition_2(row):
        row['New_keywordBid'] += 0.03
        row['提价原因'] = '定义二'
    elif condition_3(row):
        row['New_keywordBid'] += 0.04
        row['提价原因'] = '定义三'
    elif condition_4(row):
        row['New_keywordBid'] += 0.02
        row['提价原因'] = '定义四'
    elif condition_5(row):
        row['New_keywordBid'] += 0.02
        row['提价原因'] = '定义五'
    elif condition_6(row):
        row['New_keywordBid'] += 0.01
        row['提价原因'] = '定义六'
    return row

# 应用提价策略
data = data.apply(adjust_bid, axis=1)

# 筛选被提价的商品投放
result_data = data[data['提价原因'] != ""]

# 选择输出的列
output_columns = ['keyword', 'keywordId', 'campaignName', 'adGroupName', 'matchType', 'keywordBid', 
                  'New_keywordBid', 'targeting', 'total_cost_30d', 'total_clicks_30d', 'ACOS_7d', 
                  'ACOS_30d', 'ORDER_1m', '提价原因']

result_data = result_data[output_columns]

# 保存结果到新的CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_优质商品投放_v1_1_LAPASA_DE_2024-07-11.csv'
result_data.to_csv(output_file_path, index=False)

print(f"新文件保存至: {output_file_path}")