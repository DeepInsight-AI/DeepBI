# filename: filter_and_calculate_corrected.py
import pandas as pd

# 读取CSV文件
data = pd.read_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\预处理.csv')

# 筛选条件
conditions = (
    data['ACOS_7d'] <= 0.24,
    data['ACOS_yesterday'] <= 0.24,
    data['cost_yesterday'] >= 0.8 * data['Budget']
)

# 如果所有条件都满足，则该广告活动符合条件
data['is_qualified'] = conditions[0] & conditions[1] & conditions[2]

# 计算新的预算
data['New_Budget'] = data['Budget'] * 1.2

# 限制预算不超过50
data.loc[data['New_Budget'] > 50, 'New_Budget'] = 50

# 输出符合条件的广告活动及其详细信息
qualified_data = data[data['is_qualified']]

# 添加增加预算的原因
qualified_data['Reason'] = '满足最近7天的平均ACOS值在0.24以下，昨天的ACOS值在0.24以下，且昨天花费超过了昨天预算的80%的条件'

# 选择需要输出的列
output_columns = [
    'campaignId',
    'campaignName',
    'Budget',
    'New_Budget',
    'cost_yesterday',
    'clicks_yesterday',
    'ACOS_yesterday',
    'ACOS_7d',
    'country_avg_ACOS_1m',
    'total_clicks_30d',
    'total_sales14d_30d',
    'Reason'
]

# 准备输出数据
output_data = qualified_data[output_columns]

# 输出到CSV文件
output_data.to_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\提问策略\手动_优质广告活动_v1_1_IT_2024-06-13_deepseek.csv', index=False)

# 打印输出数据
print(output_data)