# filename: search_term_analysis.py

import pandas as pd

# 读取CSV文件
data = pd.read_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\搜索词优化\预处理.csv')

# 定义筛选条件
conditions = (
    ((data['ACOS_30d'] > 0.24) & (data['ACOS_30d'] < 0.36) & (data['ORDER_1m'] <= 5)) |  # 定义一
    ((data['ACOS_30d'] >= 0.36) & (data['ORDER_1m'] <= 8)) |                           # 定义二
    ((data['total_clicks_30d'] > 13) & (data['ORDER_1m'] == 0)) |                       # 定义三
    ((data['ACOS_7d'] > 0.24) & (data['ACOS_7d'] < 0.36) & (data['ORDER_7d'] <= 3)) |   # 定义四
    ((data['ACOS_7d'] >= 0.36) & (data['ORDER_7d'] <= 5)) |                             # 定义五
    ((data['total_clicks_7d'] > 10) & (data['ORDER_7d'] == 0))                          # 定义六
)

# 筛选数据
filtered_data = data[conditions].copy()

# 确定满足的定义原因
reasons = []
for idx, row in filtered_data.iterrows():
    reason = []
    if (0.24 < row['ACOS_30d'] < 0.36) and (row['ORDER_1m'] <= 5):
        reason.append('定义一')
    if (row['ACOS_30d'] >= 0.36) and (row['ORDER_1m'] <= 8):
        reason.append('定义二')
    if (row['total_clicks_30d'] > 13) and (row['ORDER_1m'] == 0):
        reason.append('定义三')
    if (0.24 < row['ACOS_7d'] < 0.36) and (row['ORDER_7d'] <= 3):
        reason.append('定义四')
    if (row['ACOS_7d'] >= 0.36) and (row['ORDER_7d'] <= 5):
        reason.append('定义五')
    if (row['total_clicks_7d'] > 10) and (row['ORDER_7d'] == 0):
        reason.append('定义六')
    reasons.append(','.join(reason))

# 添加原因列
filtered_data['reason'] = reasons

# 选择需要的列
output_cols = ['campaignName', 'campaignId', 'adGroupName', 'adGroupId', 'total_clicks_7d', 
               'ACOS_7d', 'ORDER_7d', 'total_clicks_30d', 'ORDER_1m', 'ACOS_30d', 'searchTerm', 'reason']
output_data = filtered_data[output_cols]

# 保存结果到新的CSV文件
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\搜索词优化\提问策略\手动_劣质搜索词_v1_1_ES_2024-06-20.csv'
output_data.to_csv(output_path, index=False)

print(f"结果已保存到 {output_path}")