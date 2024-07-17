# filename: update_bids.py

import pandas as pd

# 读取数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv'
df = pd.read_csv(file_path)

# 定义提价函数
def adjust_bid(row):
    if (0 < row['ACOS_7d'] <= 0.1 and 
        0 < row['ACOS_30d'] <= 0.1 and 
        row['ORDER_1m'] >= 2 and 
        0 < row['ACOS_3d'] <= 0.2):
        return 0.05, '定义一'
    elif (0 < row['ACOS_7d'] <= 0.1 and 
          0.1 < row['ACOS_30d'] <= 0.24 and 
          row['ORDER_1m'] >= 2 and 
          0 < row['ACOS_3d'] <= 0.2):
        return 0.03, '定义二'
    elif (0.1 < row['ACOS_7d'] <= 0.2 and 
          0 < row['ACOS_30d'] <= 0.1 and 
          row['ORDER_1m'] >= 2 and 
          0 < row['ACOS_3d'] <= 0.2):
        return 0.04, '定义三'
    elif (0.1 < row['ACOS_7d'] <= 0.2 and 
          0.1 < row['ACOS_30d'] <= 0.24 and 
          row['ORDER_1m'] >= 2 and 
          0 < row['ACOS_3d'] <= 0.2):
        return 0.02, '定义四'
    elif (0.2 < row['ACOS_7d'] <= 0.24 and 
          0 < row['ACOS_30d'] <= 0.1 and 
          row['ORDER_1m'] >= 2 and 
          0 < row['ACOS_3d'] <= 0.2):
        return 0.02, '定义五'
    elif (0.2 < row['ACOS_7d'] <= 0.24 and 
          0.1 < row['ACOS_30d'] <= 0.24 and 
          row['ORDER_1m'] >= 2 and 
          0 < row['ACOS_3d'] <= 0.2):
        return 0.01, '定义六'
    else:
        return 0, ''

# 应用提价规则
df['increase_bid'], df['reason'] = zip(*df.apply(adjust_bid, axis=1))
df['New_keywordBid'] = df['keywordBid'] + df['increase_bid']

# 筛选得到表现较好商品投放
df_good_performance = df[df['increase_bid'] > 0]

# 选择需要输出的列
output_columns = [
    'keyword', 'keywordId', 'campaignName', 'adGroupName', 'matchType', 
    'keywordBid', 'New_keywordBid', 'targeting', 'total_cost_30d', 
    'total_clicks_30d', 'ACOS_7d', 'ACOS_30d', 'ORDER_1m', 'increase_bid', 'reason'
]

# 导出数据
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_优质商品投放_v1_1_LAPASA_DE_2024-07-14.csv'
df_good_performance.to_csv(output_file_path, index=False, columns=output_columns)

print(f"Data successfully saved to {output_file_path}")