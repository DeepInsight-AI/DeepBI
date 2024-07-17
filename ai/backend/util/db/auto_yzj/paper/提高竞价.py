# filename: 提高竞价.py

import pandas as pd

# Step 1: 读取数据
data_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv'
df = pd.read_csv(data_path)

# Step 2: 定义提高竞价规则
def increase_bid(df):
    conditions = [
        # 定义一
        (df['ACOS_7d'].between(0, 0.1) & df['ACOS_30d'].between(0, 0.1) & (df['ORDER_1m'] >= 2)),
        # 定义二
        (df['ACOS_7d'].between(0, 0.1) & df['ACOS_30d'].between(0.1, 0.24) & (df['ORDER_1m'] >= 2)),
        # 定义三
        (df['ACOS_7d'].between(0.1, 0.2) & df['ACOS_30d'].between(0, 0.1) & (df['ORDER_1m'] >= 2)),
        # 定义四
        (df['ACOS_7d'].between(0.1, 0.2) & df['ACOS_30d'].between(0.1, 0.24) & (df['ORDER_1m'] >= 2)),
        # 定义五
        (df['ACOS_7d'].between(0.2, 0.24) & df['ACOS_30d'].between(0, 0.1) & (df['ORDER_1m'] >= 2)),
        # 定义六
        (df['ACOS_7d'].between(0.2, 0.24) & df['ACOS_30d'].between(0.1, 0.24) & (df['ORDER_1m'] >= 2))
    ]
    
    increases = [0.05, 0.03, 0.04, 0.02, 0.02, 0.01]
    reasons = ['definition_1', 'definition_2', 'definition_3', 'definition_4', 'definition_5', 'definition_6']
    
    for i, condition in enumerate(conditions):
        df.loc[condition, 'increase'] = increases[i]
        df.loc[condition, 'reason'] = reasons[i]
        
    df = df[df['increase'].notnull()]
    return df

# Step 3: 根据定义规则增加商品投放竞价
df = increase_bid(df)
df['New_keywordBid'] = df['keywordBid'] + df['increase']

# Step 4: 选择需要输出的列
columns_needed = [
    'keyword', 'keywordId', 'campaignName', 'adGroupName',
    'matchType', 'keywordBid', 'New_keywordBid', 'targeting',
    'total_cost_30d', 'total_clicks_30d', 'ACOS_7d', 'ACOS_30d',
    'ORDER_1m', 'increase', 'reason'
]
df_filtered = df[columns_needed]

# Step 5: 输出到CSV文件
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_优质商品投放_v1_1_LAPASA_FR_2024-07-10.csv'
df_filtered.to_csv(output_path, index=False)
print("CSV文件已成功保存到: ", output_path)