# filename: process_product_optimization.py
import pandas as pd

# 读取数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv'
data = pd.read_csv(file_path)

# 创建一个新的DataFrame来存储结果
results = []

# 定义一
condition1 = (data['ACOS_7d'] > 0.24) & (data['ACOS_7d'] <= 0.5) & (data['ACOS_30d'] > 0) & (data['ACOS_30d'] <= 0.5)
data.loc[condition1, 'New_keywordBid'] = data['keywordBid'] / (((data['ACOS_7d'] - 0.24) / 0.24) + 1)
data.loc[condition1, 'ActionReason'] = '竞价调整: 定义一'

# 定义二
condition2 = (data['ACOS_7d'] > 0.5) & (data['ACOS_30d'] <= 0.36)
data.loc[condition2, 'New_keywordBid'] = data['keywordBid'] / (((data['ACOS_7d'] - 0.24) / 0.24) + 1)
data.loc[condition2, 'ActionReason'] = '竞价调整: 定义二'

# 定义三
condition3 = (data['total_clicks_7d'] >= 10) & (data['total_sales14d_7d'] == 0) & (data['ACOS_30d'] <= 0.36)
data.loc[condition3, 'New_keywordBid'] = data['keywordBid'] - 0.04
data.loc[condition3, 'ActionReason'] = '竞价调整: 定义三'

# 定义四
condition4 = (data['total_clicks_7d'] >= 10) & (data['total_sales14d_7d'] == 0) & (data['ACOS_30d'] > 0.5)
data.loc[condition4, 'New_keywordBid'] = '关闭'
data.loc[condition4, 'ActionReason'] = '关闭广告: 定义四'

# 定义五
condition5 = (data['ACOS_7d'] > 0.5) & (data['ACOS_30d'] > 0.36)
data.loc[condition5, 'New_keywordBid'] = '关闭'
data.loc[condition5, 'ActionReason'] = '关闭广告: 定义五'

# 定义六
condition6 = (data['total_sales14d_30d'] == 0) & (data['total_cost_30d'] >= 5)
data.loc[condition6, 'New_keywordBid'] = '关闭'
data.loc[condition6, 'ActionReason'] = '关闭广告: 定义六'

# 定义七
condition7 = (data['total_sales14d_30d'] == 0) & (data['total_clicks_30d'] >= 15) & (data['total_clicks_7d'] > 0)
data.loc[condition7, 'New_keywordBid'] = '关闭'
data.loc[condition7, 'ActionReason'] = '关闭广告: 定义七'

# 将筛选后的数据保存到新的CSV文件
result_data = data.loc[condition1 | condition2 | condition3 | condition4 | condition5 | condition6 | condition7, [
    'keywordId', 'keyword', 'targeting', 'matchType', 'adGroupName', 'campaignName', 
    'keywordBid', 'New_keywordBid', 'total_clicks_7d', 'total_sales14d_7d', 
    'total_cost_30d', 'total_sales14d_30d', 'total_clicks_30d', 'ACOS_7d', 'ACOS_30d',
    'ActionReason']]

output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_劣质商品投放_v1_1_LAPASA_UK_2024-07-10.csv'
result_data.to_csv(output_file_path, index=False)

print("结果已成功保存到文件:", output_file_path)