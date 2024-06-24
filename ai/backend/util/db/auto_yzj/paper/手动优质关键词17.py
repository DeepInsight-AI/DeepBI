# filename: process_keywords.py
import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\关键词优化\预处理.csv'
df = pd.read_csv(file_path)

# 定义条件函数
def get_raise_amount(row):
    ACOS_7d = row['ACOS_7d']
    ACOS_30d = row['ACOS_30d']
    orders_1m = row['ORDER_1m']
    
    if 0 < ACOS_7d <= 0.1 and 0 < ACOS_30d <= 0.1 and orders_1m >= 2:
        return 0.05, '定义一'
    elif 0 < ACOS_7d <= 0.1 and 0.1 < ACOS_30d <= 0.24 and orders_1m >= 2:
        return 0.03, '定义二'
    elif 0.1 < ACOS_7d <= 0.2 and 0 < ACOS_30d <= 0.1 and orders_1m >= 2:
        return 0.04, '定义三'
    elif 0.1 < ACOS_7d <= 0.2 and 0.1 < ACOS_30d <= 0.24 and orders_1m >= 2:
        return 0.02, '定义四'
    elif 0.2 < ACOS_7d <= 0.24 and 0 < ACOS_30d <= 0.1 and orders_1m >= 2:
        return 0.02, '定义五'
    elif 0.2 < ACOS_7d <= 0.24 and 0.1 < ACOS_30d <= 0.24 and orders_1m >= 2:
        return 0.01, '定义六'
    else:
        return 0, ''

# 应用条件函数
df['raise_amount'], df['reason'] = zip(*df.apply(get_raise_amount, axis=1))

# 筛选出符合条件的关键词并计算新竞价
df['new_keywordBid'] = df['keywordBid'] + df['raise_amount']
result_df = df[df['raise_amount'] > 0][[
    'keyword', 'keywordId', 'campaignName', 'adGroupName', 'matchType',
    'keywordBid', 'new_keywordBid', 'targeting', 'total_cost_30d', 'total_clicks_7d',
    'ACOS_7d', 'ACOS_30d', 'ORDER_1m', 'raise_amount', 'reason'
]]

# 输出结果到指定文件
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\关键词优化\提问策略\手动_优质关键词_v1_1_IT_2024-06-17.csv'
result_df.to_csv(output_path, index=False)

print(f"结果已保存至 {output_path}")