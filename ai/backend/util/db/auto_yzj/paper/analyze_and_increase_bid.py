# filename: analyze_and_increase_bid.py
import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv'
data = pd.read_csv(file_path)

# 定义条件
conditions = [
    {
        "condition": (data['ACOS_7d'] > 0) & (data['ACOS_7d'] <= 0.1) & 
                     (data['ACOS_30d'] > 0) & (data['ACOS_30d'] <= 0.1) & 
                     (data['ORDER_1m'] >= 2),
        "bid_increase": 0.05,
        "reason": "定义一"
    },
    {
        "condition": (data['ACOS_7d'] > 0) & (data['ACOS_7d'] <= 0.1) & 
                     (data['ACOS_30d'] > 0.1) & (data['ACOS_30d'] <= 0.24) & 
                     (data['ORDER_1m'] >= 2),
        "bid_increase": 0.03,
        "reason": "定义二"
    },
    {
        "condition": (data['ACOS_7d'] > 0.1) & (data['ACOS_7d'] <= 0.2) & 
                     (data['ACOS_30d'] <= 0.1) & 
                     (data['ORDER_1m'] >= 2),
        "bid_increase": 0.04,
        "reason": "定义三"
    },
    {
        "condition": (data['ACOS_7d'] > 0.1) & (data['ACOS_7d'] <= 0.2) & 
                     (data['ACOS_30d'] > 0.1) & (data['ACOS_30d'] <= 0.24) & 
                     (data['ORDER_1m'] >= 2),
        "bid_increase": 0.02,
        "reason": "定义四"
    },
    {
        "condition": (data['ACOS_7d'] > 0.2) & (data['ACOS_7d'] <= 0.24) & 
                     (data['ACOS_30d'] <= 0.1) & 
                     (data['ORDER_1m'] >= 2),
        "bid_increase": 0.02,
        "reason": "定义五"
    },
    {
        "condition": (data['ACOS_7d'] > 0.2) & (data['ACOS_7d'] <= 0.24) & 
                     (data['ACOS_30d'] > 0.1) & (data['ACOS_30d'] <= 0.24) & 
                     (data['ORDER_1m'] >= 2),
        "bid_increase": 0.01,
        "reason": "定义六"
    }
]

# 筛选并计算新的竞价
results = []
for condition in conditions:
    filtered_data = data[condition["condition"]]
    filtered_data = filtered_data.copy()  # deep copy to avoid modifying original dataset
    filtered_data['New_keywordBid'] = filtered_data['keywordBid'] + condition["bid_increase"]
    filtered_data['Bid_Increase'] = condition["bid_increase"]
    filtered_data['Increasing_Reason'] = condition["reason"]
    results.append(filtered_data)

# 合并所有结果
results_df = pd.concat(results)

# 准备输出列
output_columns = [
    'keyword', 'keywordId', 'campaignName', 'adGroupName', 'matchType', 'keywordBid', 
    'New_keywordBid', 'targeting', 'total_cost_30d', 'total_clicks_30d', 
    'ACOS_7d', 'ACOS_30d', 'ORDER_1m', 'Bid_Increase', 'Increasing_Reason'
]

# 输出结果到CSV文件
output_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_优质商品投放_v1_1_LAPASA_US_2024-07-10.csv'
results_df.to_csv(output_file, columns=output_columns, index=False)

print("分析完毕并保存到CSV文件。")