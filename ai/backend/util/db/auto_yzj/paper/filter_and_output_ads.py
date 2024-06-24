# filename: filter_and_output_ads.py
import pandas as pd

# 读取数据
data = pd.read_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\预处理.csv')

# 筛选出符合定义一的广告活动
filtered_ads = data[(data['sales_1m'] == 0) & (data['clicks_1m'] >= 75)]

# 准备输出数据
output_data = filtered_ads[['date', 'campaignName', 'Budget', 'clicks', 'ACOS', 'avg_ACOS_7d', 'avg_ACOS_1m', 'clicks_1m', 'sales_1m']]
output_data['对广告活动进行关闭的原因'] = '最近一个月的总销售为0且最近一个月的总点击次数大于等于75'

# 输出到CSV文件
output_data.to_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\提问策略\关闭的广告活动_FR_2024-5-28_deepseek.csv', index=False)