# filename: filter_and_update_budget.py
import pandas as pd

# 读取数据
data = pd.read_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\test\预处理.csv')

# 筛选条件
today = pd.to_datetime('2024-05-28')
yesterday = today - pd.Timedelta(days=1)
yesterday_str = yesterday.strftime('%Y-%m-%d')

# 筛选出符合条件的广告活动
filtered_data = data[(data['date'] == yesterday_str) &
                     (data['avg_ACOS_7d'] > 0.24) &
                     (data['ACOS'] > 0.24) &
                     (data['clicks'] >= 10) &
                     (data['avg_ACOS_1m'] > data['country_avg_ACOS_1m'])]

# 更新预算
filtered_data.loc[:, 'Budget'] = filtered_data['Budget'].apply(lambda x: max(8, x - 5))

# 添加新列
filtered_data['new_budget'] = filtered_data['Budget']
filtered_data['reason'] = '符合定义一的条件，进行预算降低'

# 输出结果
output_columns = ['date', 'campaignName', 'Budget', 'clicks', 'ACOS', 'avg_ACOS_7d', 'clicks_7d', 'sales_1m', 'avg_ACOS_1m', 'clicks_1m', 'sales_1m', 'country_avg_ACOS_1m', 'new_budget', 'reason']
output_data = filtered_data[output_columns]
output_data.columns = ['date', 'campaignName', 'Budget', 'clicks', 'ACOS', '最近7天的平均ACOS值', '最近7天的总点击次数', '最近7天的总销售', '最近一个月的平均ACOS值', '最近一个月的总点击数', '最近一个月的总销售', '该国家广告活动最近一个月整体的平均ACOS值', '新的预算', '对广告活动进行降低预算的原因']
output_data.to_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\test\提问策略\测试_FR_2024-05-15_deepseek.csv', index=False)

# 确认执行成功
print("数据处理完成，结果已保存到CSV文件。")