# filename: 提价策略_v1_1.py

import pandas as pd
import os

# 读取数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv'
data = pd.read_csv(file_path)

# 定义条件
conditions = [
    # 定义一
    ((data['ACOS_7d'] > 0) & (data['ACOS_7d'] <= 0.1) &
     (data['ACOS_30d'] > 0) & (data['ACOS_30d'] <= 0.1) &
     (data['ORDER_1m'] >= 2) &
     (data['ACOS_3d'] > 0) & (data['ACOS_3d'] <= 0.2), 0.05, "definition_1"),
    
    # 定义二
    ((data['ACOS_7d'] > 0) & (data['ACOS_7d'] <= 0.1) &
     (data['ACOS_30d'] > 0.1) & (data['ACOS_30d'] <= 0.24) &
     (data['ORDER_1m'] >= 2) &
     (data['ACOS_3d'] > 0) & (data['ACOS_3d'] <= 0.2), 0.03, "definition_2"),
    
    # 定义三
    ((data['ACOS_7d'] > 0.1) & (data['ACOS_7d'] <= 0.2) &
     (data['ACOS_30d'] <= 0.1) &
     (data['ORDER_1m'] >= 2) &
     (data['ACOS_3d'] > 0) & (data['ACOS_3d'] <= 0.2), 0.04, "definition_3"),
    
    # 定义四
    ((data['ACOS_7d'] > 0.1) & (data['ACOS_7d'] <= 0.2) &
     (data['ACOS_30d'] > 0.1) & (data['ACOS_30d'] <= 0.24) &
     (data['ORDER_1m'] >= 2) &
     (data['ACOS_3d'] > 0) & (data['ACOS_3d'] <= 0.2), 0.02, "definition_4"),
    
    # 定义五
    ((data['ACOS_7d'] > 0.2) & (data['ACOS_7d'] <= 0.24) &
     (data['ACOS_30d'] <= 0.1) &
     (data['ORDER_1m'] >= 2) &
     (data['ACOS_3d'] > 0) & (data['ACOS_3d'] <= 0.2), 0.02, "definition_5"),
    
    # 定义六
    ((data['ACOS_7d'] > 0.2) & (data['ACOS_7d'] <= 0.24) &
     (data['ACOS_30d'] > 0.1) & (data['ACOS_30d'] <= 0.24) &
     (data['ORDER_1m'] >= 2) &
     (data['ACOS_3d'] > 0) & (data['ACOS_3d'] <= 0.2), 0.01, "definition_6")
]

# 创建一个新的DataFrame来存储符合条件的记录
result = pd.DataFrame()

# 处理每个条件
for condition, bid_increment, reason in conditions:
    temp_data = data[condition].copy()
    temp_data['New_keywordBid'] = temp_data['keywordBid'] + bid_increment
    temp_data['Bid_Increment'] = bid_increment
    temp_data['Reason'] = reason
    result = pd.concat([result, temp_data], ignore_index=True)

# 选择并重命名列
result = result[['keyword', 'keywordId', 'campaignName', 'adGroupName', 'matchType', 'keywordBid', 'New_keywordBid', 
                 'targeting', 'total_cost_30d', 'total_clicks_30d', 'ACOS_7d', 'ACOS_30d', 'ORDER_1m', 
                 'Bid_Increment', 'Reason']]
result.columns = ['keyword', 'keywordId', 'campaignName', 'adGroupName', 'matchType', 'keywordBid', 'New_keywordBid', 
                  'targeting', 'cost', 'clicks', 'ACOS_7d', 'ACOS_30d', 'ORDER_1m', 'Bid_Increment', 'Reason']

# 输出结果到CSV文件
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略'
if not os.path.exists(output_path):
    os.makedirs(output_path)
output_file = os.path.join(output_path, '手动_ASIN_优质商品投放_v1_1_LAPASA_UK_2024-07-11.csv')
result.to_csv(output_file, index=False)

print(f"结果已保存到 {output_file}")