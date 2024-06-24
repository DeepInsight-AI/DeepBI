# filename: combined_script.py
import pandas as pd

# 读取CSV文件
data = pd.read_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\预处理.csv')

# 筛选出昨天的数据
yesterday = '2024-05-27'
data_yesterday = data[data['date'] == yesterday]

# 筛选出符合条件的广告活动
selected_campaigns = data_yesterday[
    (data_yesterday['avg_ACOS_7d'] <= 0.24) &
    (data_yesterday['ACOS'] <= 0.24) &
    (data_yesterday['cost'] >= data_yesterday['Budget'] * 0.8)
]

# 计算新的预算
selected_campaigns['new_Budget'] = selected_campaigns['Budget'] * 1.2
selected_campaigns.loc[selected_campaigns['new_Budget'] > 50, 'new_Budget'] = 50

# 输出结果到CSV文件
output_columns = ['date', 'campaignName', 'Budget', 'cost', 'clicks', 'ACOS', 'avg_ACOS_7d', 'avg_ACOS_1m', 'clicks_1m', 'sales_1m', 'new_Budget']
selected_campaigns[output_columns].to_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\提问策略\优质广告活动_FR_2024-5-28_deepseek.csv', index=False)

print(selected_campaigns)