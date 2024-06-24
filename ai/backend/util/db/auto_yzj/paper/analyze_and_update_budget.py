# filename: analyze_and_update_budget.py
import pandas as pd

# 读取数据
data = pd.read_csv('/backend/util/db/auto_yzj/日常优化/手动sp广告/test/预处理1.csv')

# 筛选符合条件的广告活动
filtered_data = data[(data['avg_ACOS_7d'] > 0.24) & (data['ACOS'] > 0.24) & (data['clicks'] >= 10) & (data['avg_ACOS_1m'] > data['country_avg_ACOS_1m'])]

# 更新预算
filtered_data['Budget'] = filtered_data['Budget'].apply(lambda x: x - 5 if x > 8 else 8)

# 输出结果到CSV文件
output_columns = ['date', 'campaignName', 'Budget', 'clicks', 'ACOS', 'avg_ACOS_7d', 'clicks_7d', 'sales_1m', 'avg_ACOS_1m', 'clicks_1m', 'sales_1m', 'country_avg_ACOS_1m', 'new_Budget', 'reason']
output_data = filtered_data[output_columns]
output_data.columns = ['日期', '广告活动', '预算', '点击数', 'ACOS', '最近7天的平均ACOS值', '最近7天的总点击次数', '最近7天的总线销售', '最近一个月的平均ACOS值', '最近一个月的总点击数', '最近一个月的总销售', '该国家广告活动最近一个月整体的平均ACOS值', '新的预算', '对广告活动进行降低预算的原因']
output_data.to_csv('C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\手动sp广告\\test\\提问策略\\测试_FR_2024-05-17_deepseek.csv', index=False)

# 确认执行成功
print("数据处理和预算更新完成，结果已保存到CSV文件。")
