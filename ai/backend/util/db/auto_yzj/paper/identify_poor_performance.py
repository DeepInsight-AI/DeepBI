# filename: identify_poor_performance.py
import pandas as pd

# 加载数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv'
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_劣质商品投放_v1_1_LAPASA_ES_2024-07-09.csv'

df = pd.read_csv(file_path)

# 定义ACOS和竞价字段的最低值
ACOS_7d_lower_limit = 0.24
ACOS_30d_upper_limit = 0.5

# 初始化结果DataFrame
columns = ['keyword', 'keywordId', 'campaignName', 'adGroupName', 'matchType', 'keywordBid', 'new_keywordBid', 'targeting',
           'total_cost_7d', 'total_sales14d_7d', 'total_cost_30d', 'total_sales14d_30d', 'ACOS_7d', 'ACOS_30d', 
           'total_clicks_30d', 'action_reason']
result_df = pd.DataFrame(columns=columns)

# 定义函数：根据定义一调整竞价
def adjust_bid_definition_1(row):
    return row['keywordBid'] / ((row['ACOS_7d'] - ACOS_7d_lower_limit) / ACOS_7d_lower_limit + 1)

# 定义函数：关闭操作
def close_keyword():
    return "关闭"

# 处理数据，判定劣质商品投放并调整竞价
for idx, row in df.iterrows():
    action_reason = ""
    new_keywordBid = None
    
    if ACOS_7d_lower_limit < row['ACOS_7d'] <= ACOS_30d_upper_limit and 0 < row['ACOS_30d'] <= ACOS_30d_upper_limit:
        new_keywordBid = adjust_bid_definition_1(row)
        action_reason = "定义一：调整竞价"
    
    elif row['ACOS_7d'] > 0.5 and row['ACOS_30d'] <= 0.36:
        new_keywordBid = adjust_bid_definition_1(row)
        action_reason = "定义二：调整竞价"
        
    elif row['total_clicks_7d'] >= 10 and row['total_sales14d_7d'] == 0 and row['ACOS_30d'] <= 0.36:
        new_keywordBid = max(row['keywordBid'] - 0.04, 0)
        action_reason = "定义三：降低竞价"
        
    elif row['total_clicks_7d'] >= 10 and row['total_sales14d_7d'] == 0 and row['ACOS_30d'] > 0.5:
        new_keywordBid = close_keyword()
        action_reason = "定义四：关闭"
        
    elif row['ACOS_7d'] > 0.5 and row['ACOS_30d'] > 0.36:
        new_keywordBid = close_keyword()
        action_reason = "定义五：关闭"
        
    elif row['total_sales14d_30d'] == 0 and row['total_cost_30d'] >= 5:
        new_keywordBid = close_keyword()
        action_reason = "定义六：关闭"
        
    elif row['total_sales14d_30d'] == 0 and row['total_clicks_30d'] >= 15 and row['total_clicks_7d'] > 0:
        new_keywordBid = close_keyword()
        action_reason = "定义七：关闭"
    
    if action_reason:
        row_data = {
            'keyword': row['keyword'],
            'keywordId': row['keywordId'],
            'campaignName': row['campaignName'],
            'adGroupName': row['adGroupName'],
            'matchType': row['matchType'],
            'keywordBid': row['keywordBid'],
            'new_keywordBid': new_keywordBid,
            'targeting': row['targeting'],
            'total_cost_7d': row['total_cost_7d'],
            'total_sales14d_7d': row['total_sales14d_7d'],
            'total_cost_30d': row['total_cost_30d'],
            'total_sales14d_30d': row['total_sales14d_30d'],
            'ACOS_7d': row['ACOS_7d'],
            'ACOS_30d': row['ACOS_30d'],
            'total_clicks_30d': row['total_clicks_30d'],
            'action_reason': action_reason
        }
        result_df = pd.concat([result_df, pd.DataFrame([row_data])], ignore_index=True)

# 将结果保存到CSV文件
result_df.to_csv(output_path, index=False)
print("结果已保存到:", output_path)