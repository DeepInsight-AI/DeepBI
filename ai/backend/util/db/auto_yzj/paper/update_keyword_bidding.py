# filename: update_keyword_bidding.py
import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\关键词优化\预处理.csv'
data = pd.read_csv(file_path)

# 定义筛选条件和提价策略
def update_bid(cond1, cond2, cond3, increase, reason):
    condition_met = cond1 & cond2 & cond3
    data.loc[condition_met, 'new_keywordBid'] = data.loc[condition_met, 'keywordBid'] + increase
    data.loc[condition_met, '提价原因'] = reason

# 定义一
condition_1_1 = (data['ACOS_7d'] > 0) & (data['ACOS_7d'] <= 0.1)
condition_1_2 = (data['ACOS_30d'] > 0) & (data['ACOS_30d'] <= 0.1)
condition_1_3 = (data['ORDER_1m'] >= 2)
update_bid(condition_1_1, condition_1_2, condition_1_3, 0.05, '定义一')

# 定义二
condition_2_1 = (data['ACOS_7d'] > 0) & (data['ACOS_7d'] <= 0.1)
condition_2_2 = (data['ACOS_30d'] > 0.1) & (data['ACOS_30d'] <= 0.24)
condition_2_3 = (data['ORDER_1m'] >= 2)
update_bid(condition_2_1, condition_2_2, condition_2_3, 0.03, '定义二')

# 定义三
condition_3_1 = (data['ACOS_7d'] > 0.1) & (data['ACOS_7d'] <= 0.2)
condition_3_2 = (data['ACOS_30d'] <= 0.1)
condition_3_3 = (data['ORDER_1m'] >= 2)
update_bid(condition_3_1, condition_3_2, condition_3_3, 0.04, '定义三')

# 定义四
condition_4_1 = (data['ACOS_7d'] > 0.1) & (data['ACOS_7d'] <= 0.2)
condition_4_2 = (data['ACOS_30d'] > 0.1) & (data['ACOS_30d'] <= 0.24)
condition_4_3 = (data['ORDER_1m'] >= 2)
update_bid(condition_4_1, condition_4_2, condition_4_3, 0.02, '定义四')

# 定义五
condition_5_1 = (data['ACOS_7d'] > 0.2) & (data['ACOS_7d'] <= 0.24)
condition_5_2 = (data['ACOS_30d'] <= 0.1)
condition_5_3 = (data['ORDER_1m'] >= 2)
update_bid(condition_5_1, condition_5_2, condition_5_3, 0.02, '定义五')

# 定义六
condition_6_1 = (data['ACOS_7d'] > 0.2) & (data['ACOS_7d'] <= 0.24)
condition_6_2 = (data['ACOS_30d'] > 0.1) & (data['ACOS_30d'] <= 0.24)
condition_6_3 = (data['ORDER_1m'] >= 2)
update_bid(condition_6_1, condition_6_2, condition_6_3, 0.01, '定义六')

# 筛选出被识别的关键词
filtered_data = data.dropna(subset=['new_keywordBid'])

# 选择需要输出的列
output_cols = [
    "keyword", "keywordId", "campaignName", "adGroupName", "matchType",
    "keywordBid", "targeting", "total_cost_7d", "total_clicks_7d",
    "ACOS_7d", "ACOS_30d", "ORDER_1m", "new_keywordBid", "提价原因"
]
output_data = filtered_data[output_cols]

# 保存结果到指定的CSV文件
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\关键词优化\提问策略\手动_优质关键词_IT_2024-06-11.csv'
output_data.to_csv(output_path, index=False)

print(f"结果已保存到 {output_path}")