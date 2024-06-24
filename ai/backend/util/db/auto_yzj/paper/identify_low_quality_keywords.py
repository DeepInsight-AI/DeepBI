# filename: identify_low_quality_keywords.py

import pandas as pd
from datetime import datetime

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\关键词优化\预处理.csv'
df = pd.read_csv(file_path)

# 计算广告组最近7天的总花费
adgroup_costs_7d = df.groupby('adGroupName')['total_cost_7d'].sum().reset_index()
adgroup_costs_7d.columns = ['adGroupName', 'adGroup_total_cost_7d']

# 将计算结果合并回原数据框
df = df.merge(adgroup_costs_7d, on='adGroupName', how='left')

# 定义新的字段
df['new_keywordBid'] = df['keywordBid']
df['operation_reason'] = ''

# 列出所有可能返回的情况
def apply_conditions(row):
    # 定义一
    if 0.24 < row['ACOS_7d'] <= 0.5 and 0 < row['ACOS_30d'] <= 0.5:
        new_bid = row['keywordBid'] / (((row['ACOS_7d'] - 0.24) / 0.24) + 1)
        return new_bid, 'Adjusted bid according to Definition 1'
    # 定义二
    elif row['ACOS_7d'] > 0.5 and row['ACOS_30d'] <= 0.36:
        new_bid = row['keywordBid'] / (((row['ACOS_7d'] - 0.24) / 0.24) + 1)
        return new_bid, 'Adjusted bid according to Definition 2'
    # 定义三
    elif row['total_clicks_7d'] >= 10 and row['total_sales14d_7d'] == 0 and row['ACOS_30d'] <= 0.36:
        new_bid = row['keywordBid'] - 0.04
        return new_bid, 'Reduced bid by 0.04 as per Definition 3'
    # 定义四
    elif row['total_clicks_7d'] >= 10 and row['total_sales14d_7d'] == 0 and row['ACOS_30d'] > 0.5:
        return 0, 'Closed as per Definition 4'
    # 定义五
    elif row['ACOS_7d'] > 0.5 and row['ACOS_30d'] > 0.36:
        return 0, 'Closed as per Definition 5'
    # 定义六
    elif row['total_sales14d_30d'] == 0 and row['total_cost_7d'] > (row['adGroup_total_cost_7d'] / 5):
        return 0, 'Closed as per Definition 6'
    # 定义七
    elif row['total_sales14d_30d'] == 0 and row['total_clicks_30d'] >= 15:
        return 0, 'Closed as per Definition 7'
    
    # 检查条目并返回默认值
    return row['keywordBid'], ''

# 应用条件，并添加调试信息
applied_conditions = df.apply(lambda row: pd.Series(apply_conditions(row)), axis=1)

# 检查行数是否正确
if len(df) != len(applied_conditions):
    print(f"Mismatch in length! Original dataframe length: {len(df)}, Applied conditions length: {len(applied_conditions)}")
else:
    print(f"Length match! Original dataframe length: {len(df)}, Applied conditions length: {len(applied_conditions)}")

# 检查列数是否正确
if len(applied_conditions.columns) != 2:
    print(f"Expected 2 columns, got {len(applied_conditions.columns)} columns")

# 打印前几行
print(applied_conditions.head())

# 保存并检查临时内容
applied_conditions.to_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\关键词优化\applied_conditions_temp.csv', index=False)

# 确保df的行数和applied_conditions的行数一致
assert len(df) == len(applied_conditions), "Mismatch in row count"

df[['new_keywordBid', 'operation_reason']] = applied_conditions

# 仅保留被识别的关键词
df_filtered = df[df['operation_reason'] != '']

# 添加当前日期
df_filtered['date'] = datetime.today().strftime('%Y-%m-%d')

# 选择需要输出的字段并重命名
output_columns = [
    'date', 'keyword', 'keywordId', 'campaignName', 'adGroupName', 'matchType', 'keywordBid', 'targeting',
    'total_cost_yesterday', 'total_clicks_yesterday', 'total_cost_7d', 'total_sales14d_7d', 
    'total_sales14d_30d', 'adGroup_total_cost_7d', 'ACOS_7d', 'ACOS_30d', 'new_keywordBid', 'operation_reason'
]
df_output = df_filtered[output_columns]

# 输出到CSV文件
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\关键词优化\提问策略\手动_劣质关键词_ES_2024-06-07.csv'
df_output.to_csv(output_path, index=False)

print(f"劣质关键词信息已输出至 {output_path}")