# filename: output_to_csv.py
import pandas as pd

# 读取CSV文件
data = pd.read_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\关键词优化\预处理.csv')

# 填充ACOS相关的NaN值
acos_columns = ['ACOS_7d', 'ACOS_30d', 'ACOS_yesterday']
for column in acos_columns:
    mean_value = data[column].mean()
    data[column].fillna(mean_value, inplace=True)

# 定义函数来计算新的竞价
def calculate_new_bid(row):
    if (row['ACOS_7d'] > 0.24 and row['ACOS_7d'] <= 0.5) and (row['ACOS_30d'] > 0 and row['ACOS_30d'] <= 0.5):
        return row['keywordBid'] / ((row['ACOS_7d'] - 0.24) / 0.24 + 1)
    elif (row['ACOS_7d'] > 0.5) and (row['ACOS_30d'] <= 0.36):
        return row['keywordBid'] / ((row['ACOS_7d'] - 0.24) / 0.24 + 1)
    else:
        return row['keywordBid']

# 应用定义一到定义七的规则
data['new_keywordBid'] = data.apply(calculate_new_bid, axis=1)

# 应用定义三到定义七的规则
data['action'] = 'keep'
data['action_reason'] = ''

# 定义三
data.loc[(data['total_clicks_7d'] >= 10) & (data['total_sales14d_7d'] == 0) & (data['ACOS_30d'] <= 0.36), 'new_keywordBid'] = data['new_keywordBid'] - 0.04
data.loc[(data['total_clicks_7d'] >= 10) & (data['total_sales14d_7d'] == 0) & (data['ACOS_30d'] <= 0.36), 'action'] = 'reduce bid'
data.loc[(data['total_clicks_7d'] >= 10) & (data['total_sales14d_7d'] == 0) & (data['ACOS_30d'] <= 0.36), 'action_reason'] = 'Definiton 3: High clicks but no sales, ACOS <= 0.36'

# 定义四
data.loc[(data['total_clicks_7d'] >= 10) & (data['total_sales14d_7d'] == 0) & (data['ACOS_30d'] > 0.5), 'action'] = 'close'
data.loc[(data['total_clicks_7d'] >= 10) & (data['total_sales14d_7d'] == 0) & (data['ACOS_30d'] > 0.5), 'action_reason'] = 'Definiton 4: High clicks but no sales, ACOS > 0.5'

# 定义五
data.loc[(data['ACOS_7d'] > 0.5) & (data['ACOS_30d'] > 0.36), 'action'] = 'close'
data.loc[(data['ACOS_7d'] > 0.5) & (data['ACOS_30d'] > 0.36), 'action_reason'] = 'Definiton 5: High ACOS, ACOS_30d > 0.36'

# 定义六
data.loc[(data['total_sales14d_30d'] == 0) & (data['total_cost_7d'] > data['total_cost_7d'].quantile(0.2)), 'action'] = 'close'
data.loc[(data['total_sales14d_30d'] == 0) & (data['total_cost_7d'] > data['total_cost_7d'].quantile(0.2)), 'action_reason'] = 'Definiton 6: No sales, high cost'

# 定义七
data.loc[(data['total_sales14d_30d'] == 0) & (data['total_clicks_30d'] >= 15), 'action'] = 'close'
data.loc[(data['total_sales14d_30d'] == 0) & (data['total_clicks_30d'] >= 15), 'action_reason'] = 'Definiton 7: No sales, high clicks'

# 输出结果到CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\关键词优化\提问策略\劣质关键词_FR_2024-5-28_deepseek.csv'
data.to_csv(output_file_path, index=False)

print("Output saved to:", output_file_path)