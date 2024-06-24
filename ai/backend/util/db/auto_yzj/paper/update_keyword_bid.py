# filename: update_keyword_bid.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\关键词优化\预处理.csv'
df = pd.read_csv(file_path)

# 定义条件函数
def categorize_and_update(row):
    new_bid = None
    reason = None
    
    if 0 < row['ACOS_7d'] <= 0.1 and 0 < row['ACOS_30d'] <= 0.1 and row['ORDER_1m'] >= 2:
        new_bid = row['keywordBid'] + 0.05
        reason = "满足定义一"
    elif 0 < row['ACOS_7d'] <= 0.1 and 0.1 < row['ACOS_30d'] <= 0.24 and row['ORDER_1m'] >= 2:
        new_bid = row['keywordBid'] + 0.03
        reason = "满足定义二"
    elif 0.1 < row['ACOS_7d'] <= 0.2 and row['ACOS_30d'] <= 0.1 and row['ORDER_1m'] >= 2:
        new_bid = row['keywordBid'] + 0.04
        reason = "满足定义三"
    elif 0.1 < row['ACOS_7d'] <= 0.2 and 0.1 < row['ACOS_30d'] <= 0.24 and row['ORDER_1m'] >= 2:
        new_bid = row['keywordBid'] + 0.02
        reason = "满足定义四"
    elif 0.2 < row['ACOS_7d'] <= 0.24 and row['ACOS_30d'] <= 0.1 and row['ORDER_1m'] >= 2:
        new_bid = row['keywordBid'] + 0.02
        reason = "满足定义五"
    elif 0.2 < row['ACOS_7d'] <= 0.24 and 0.1 < row['ACOS_30d'] <= 0.24 and row['ORDER_1m'] >= 2:
        new_bid = row['keywordBid'] + 0.01
        reason = "满足定义六"
    
    return new_bid, reason

# 应用条件函数
df[['new_keywordBid', '提价原因']] = df.apply(lambda row: pd.Series(categorize_and_update(row)), axis=1)

# 过滤出满足条件的关键词
filtered_df = df.dropna(subset=['new_keywordBid'])

# 选择数据列
output_columns = [
    'keyword', 'keywordId', 'campaignName', 'adGroupName', 'matchType',
    'keywordBid', 'new_keywordBid', 'targeting', 'total_cost_30d', 
    'total_clicks_30d', 'ACOS_7d', 'ACOS_30d', 'ORDER_1m', '提价原因'
]
output_df = filtered_df[output_columns]

# 保存为CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\关键词优化\提问策略\手动_优质关键词_v1_1_ES_2024-06-121.csv'
output_df.to_csv(output_file_path, index=False)

print(f"输出结果已保存至 {output_file_path}")