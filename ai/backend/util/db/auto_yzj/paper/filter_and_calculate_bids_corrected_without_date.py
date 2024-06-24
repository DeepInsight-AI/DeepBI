# filename: filter_and_calculate_bids_corrected_without_date.py
import pandas as pd

# 读取CSV文件
data = pd.read_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\关键词优化\预处理.csv')

# 定义筛选条件
conditions = [
    (data['ACOS_7d'] > 0) & (data['ACOS_7d'] <= 0.1) &
    (data['ACOS_30d'] > 0) & (data['ACOS_30d'] <= 0.1) &
    (data['ORDER_1m'] >= 2),

    (data['ACOS_7d'] > 0) & (data['ACOS_7d'] <= 0.1) &
    (data['ACOS_30d'] > 0.1) & (data['ACOS_30d'] <= 0.24) &
    (data['ORDER_1m'] >= 2),

    (data['ACOS_7d'] > 0.1) & (data['ACOS_7d'] <= 0.2) &
    (data['ACOS_30d'] <= 0.1) &
    (data['ORDER_1m'] >= 2),

    (data['ACOS_7d'] > 0.1) & (data['ACOS_7d'] <= 0.2) &
    (data['ACOS_30d'] > 0.1) & (data['ACOS_30d'] <= 0.24) &
    (data['ORDER_1m'] >= 2),

    (data['ACOS_7d'] > 0.2) & (data['ACOS_7d'] <= 0.24) &
    (data['ACOS_30d'] <= 0.1) &
    (data['ORDER_1m'] >= 2),

    (data['ACOS_7d'] > 0.2) & (data['ACOS_7d'] <= 0.24) &
    (data['ACOS_30d'] > 0.1) & (data['ACOS_30d'] <= 0.24) &
    (data['ORDER_1m'] >= 2)
]

# 定义对应的提价值
values = [0.05, 0.03, 0.04, 0.02, 0.02, 0.01]

# 应用筛选条件并计算新的竞价
data['new_bid'] = None
data['reason'] = None
for i, condition in enumerate(conditions):
    selected = data[condition]
    data.loc[condition, 'new_bid'] = data.loc[condition, 'keywordBid'] + values[i]
    data.loc[condition, 'reason'] = f"提价{values[i]}，满足定义{i + 1}"

# 输出结果到CSV文件
output_data = data[['keyword', 'keywordId', 'campaignName', 'adGroupName', 'matchType', 'keywordBid', 'targeting', 'total_cost_7d', 'total_clicks_7d', 'ACOS_7d', 'ACOS_30d', 'ORDER_1m', 'new_bid', 'reason']]
output_data.to_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\关键词优化\提问策略\优质关键词_FR_2024-5-28_deepseek.csv', index=False)

print("结果已保存到CSV文件。")