# filename: filter_and_calculate.py
import pandas as pd

# 假设数据已经加载到名为 'data' 的 DataFrame 中
data = pd.read_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\预算优化\预处理.csv')

# 筛选条件
# 1. 最近7天的平均ACOS值在0.24以下
# 2. 昨天的ACOS值在0.24以下
# 3. 昨天花费超过了昨天预算的80%
filtered_data = data[(data['ACOS_7d'] <= 0.24) & (data['ACOS_yesterday'] <= 0.24) & (data['cost_yesterday'] > data['Budget'] * 0.8)]

# 计算新的预算
filtered_data['New_Budget'] = filtered_data['Budget'] + filtered_data['Budget'] / 5

# 确保新的预算不超过50
filtered_data['New_Budget'] = filtered_data['New_Budget'].apply(lambda x: 50 if x > 50 else x)

# 输出结果
output_data = filtered_data[['campaignId', 'campaignName', 'Budget', 'New_Budget', 'cost_yesterday', 'clicks_yesterday', 'ACOS_yesterday', 'ACOS_7d', 'country_avg_ACOS_1m', 'total_clicks_30d', 'total_sales14d_30d', 'total_cost_30d']]
output_data['对广告活动进行增加预算的原因'] = '满足所有定义条件，包括最近7天的平均ACOS值在0.24以下，昨天的ACOS值在0.24以下，昨天花费超过了昨天预算的80%'

# 保存到CSV文件
output_data.to_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\预算优化\提问策略\自动_优质广告活动_v1_1_IT_2024-06-13_deepseek.csv', index=False)

# 打印结果
print(output_data)