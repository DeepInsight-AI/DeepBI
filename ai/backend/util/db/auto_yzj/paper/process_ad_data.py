# filename: process_ad_data.py
import pandas as pd

# 定义文件路径
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv'
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_劣质商品投放_v1_1_LAPASA_US_2024-07-10.csv'

# 读取CSV文件
data = pd.read_csv(file_path)

# 定义新的DataFrame保存结果
columns = [
    'keyword', 'keywordId', 'campaignName', 'adGroupName', 'matchType', 'keywordBid', 'new_keywordBid', 
    'targeting', 'total_cost_7d', 'total_sales14d_7d', 'total_cost_30d', 'ACOS_7d', 'ACOS_30d', 
    'total_clicks_30d', 'action_reason'
]
result = pd.DataFrame(columns=columns)

# 遍历每一行数据
for index, row in data.iterrows():
    action_reason = ""
    new_keyword_bid = None
    
    if 0.24 < row['ACOS_7d'] <= 0.5 and 0 < row['ACOS_30d'] <= 0.5:
        new_keyword_bid = row['keywordBid'] / ((row['ACOS_7d'] - 0.24) / 0.24 + 1)
        action_reason = "定义一：调整竞价"
    elif row['ACOS_7d'] > 0.5 and row['ACOS_30d'] <= 0.36:
        new_keyword_bid = row['keywordBid'] / ((row['ACOS_7d'] - 0.24) / 0.24 + 1)
        action_reason = "定义二：调整竞价"
    elif row['total_clicks_7d'] >= 10 and row['total_sales14d_7d'] == 0 and row['ACOS_30d'] <= 0.36:
        new_keyword_bid = row['keywordBid'] - 0.04
        action_reason = "定义三：降低竞价"
    elif row['total_clicks_7d'] >= 10 and row['total_sales14d_7d'] == 0 and row['ACOS_30d'] > 0.5:
        new_keyword_bid = '关闭'
        action_reason = "定义四：关闭关键词"
    elif row['ACOS_7d'] > 0.5 and row['ACOS_30d'] > 0.36:
        new_keyword_bid = '关闭'
        action_reason = "定义五：关闭关键词"
    elif row['total_sales14d_30d'] == 0 and row['total_cost_30d'] >= 5:
        new_keyword_bid = '关闭'
        action_reason = "定义六：关闭关键词"
    elif row['total_sales14d_30d'] == 0 and row['total_clicks_30d'] >= 15 and row['total_clicks_7d'] > 0:
        new_keyword_bid = '关闭'
        action_reason = "定义七：关闭关键词"

    if action_reason:
        new_row = pd.DataFrame({
            'keyword': [row['keyword']],
            'keywordId': [row['keywordId']],
            'campaignName': [row['campaignName']],
            'adGroupName': [row['adGroupName']],
            'matchType': [row['matchType']],
            'keywordBid': [row['keywordBid']],
            'new_keywordBid': [new_keyword_bid],
            'targeting': [row['targeting']],
            'total_cost_7d': [row['total_cost_7d']],
            'total_sales14d_7d': [row['total_sales14d_7d']],
            'total_cost_30d': [row['total_cost_30d']],
            'ACOS_7d': [row['ACOS_7d']],
            'ACOS_30d': [row['ACOS_30d']],
            'total_clicks_30d': [row['total_clicks_30d']],
            'action_reason': [action_reason]
        })
        result = pd.concat([result, new_row], ignore_index=True)

# 输出结果到CSV
result.to_csv(output_file_path, index=False)

print("处理完成，结果已输出到文件。")