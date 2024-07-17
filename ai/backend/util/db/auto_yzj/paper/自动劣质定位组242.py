# filename: update_keywords.py

import pandas as pd

# 读取 CSV 文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\自动定位组优化\预处理.csv'
data = pd.read_csv(file_path)

# 初始化一个新的 DataFrame 用于存储需要修改的关键词信息
results = pd.DataFrame(columns=['campaignName', 'adGroupName', 'keyword', 'keywordBid', 'New_Bid', 'ACOS_30d', 'ACOS_7d', 'total_clicks_7d', '降价/关闭幅度', '原因'])

# 定义条件判断函数
def add_to_results(row, new_bid, reduction_reason):
    if new_bid == '关闭':
        reduction = '关闭'
    else:
        reduction = round(row['keywordBid'] - new_bid, 2)
        
    results.loc[len(results)] = [row['campaignName'], row['adGroupName'], row['keyword'], row['keywordBid'], new_bid, 
                                 row['ACOS_30d'], row['ACOS_7d'], row['total_clicks_7d'], reduction, reduction_reason]

# 遍历每一行数据，根据定义的条件修改竞价或关闭关键词
for index, row in data.iterrows():
    keywordBid = row['keywordBid']
    
    # 定义一
    if 0 < row['ACOS_30d'] < 0.24 and 0.24 < row['ACOS_7d'] < 0.5:
        new_bid = max(0, keywordBid - 0.03)
        add_to_results(row, new_bid, '定义一：降低竞价0.03')

    # 定义二
    elif 0.24 < row['ACOS_30d'] < 0.5 and 0.24 < row['ACOS_7d'] < 0.5:
        new_bid = max(0, keywordBid - 0.04)
        add_to_results(row, new_bid, '定义二：降低竞价0.04')
        
    # 定义三
    elif row['total_sales14d_7d'] == 0 and row['total_clicks_7d'] > 0 and 0.24 < row['ACOS_30d'] < 0.5:
        new_bid = max(0, keywordBid - 0.04)
        add_to_results(row, new_bid, '定义三：降低竞价0.04')

    # 定义四
    elif row['ACOS_30d'] > 0.5 and 0.24 < row['ACOS_7d'] < 0.5:
        new_bid = max(0, keywordBid - 0.05)
        add_to_results(row, new_bid, '定义四：降低竞价0.05')

    # 定义五
    elif 0 < row['ACOS_30d'] < 0.24 and row['ACOS_7d'] > 0.5:
        new_bid = max(0, keywordBid - 0.05)
        add_to_results(row, new_bid, '定义五：降低竞价0.05')

    # 定义六
    elif row['ORDER_1m'] == 0 and row['total_clicks_30d'] > 13 and row['total_clicks_7d'] > 0:
        new_bid = '关闭'
        add_to_results(row, new_bid, '定义六：关闭此关键词')

    # 定义七
    elif row['total_sales14d_7d'] == 0 and row['total_clicks_7d'] > 0 and row['ACOS_30d'] > 0.5:
        new_bid = '关闭'
        add_to_results(row, new_bid, '定义七：关闭此关键词')

    # 定义八
    elif row['ACOS_30d'] > 0.24 and row['ACOS_7d'] > 0.5:
        new_bid = '关闭'
        add_to_results(row, new_bid, '定义八：关闭此关键词')

# 保存结果到新的 CSV 文件
results.to_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\自动定位组优化\提问策略\自动_劣质自动定位组_v1_1_ES_2024-06-24.csv', index=False)

print("处理完成，结果已保存至目标文件。")