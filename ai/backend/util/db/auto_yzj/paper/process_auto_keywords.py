# filename: process_auto_keywords.py

import pandas as pd

# 读取CSV文件
file_path = 'C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\自动sp广告\\自动定位组优化\\预处理.csv'
data = pd.read_csv(file_path)

# 筛选满足定义的自动定位词并记录降价原因
conditions = [
    (data['ACOS_7d'] > 0.24) & (data['ACOS_7d'] < 0.5) & (data['ACOS_30d'] > 0) & (data['ACOS_30d'] < 0.24), 
    (data['ACOS_7d'] > 0.24) & (data['ACOS_7d'] < 0.5) & (data['ACOS_30d'] > 0.24) & (data['ACOS_30d'] < 0.5),
    (data['total_sales14d_7d'] == 0) & (data['total_clicks_7d'] > 0) & (data['ACOS_30d'] > 0.24) & (data['ACOS_30d'] < 0.5),
    (data['ACOS_7d'] > 0.24) & (data['ACOS_7d'] < 0.5) & (data['ACOS_30d'] > 0.5),
    (data['ACOS_7d'] > 0.5) & (data['ACOS_30d'] > 0) & (data['ACOS_30d'] < 0.24)
]

discounts = [-0.03, -0.04, -0.04, -0.05, -0.05]
reasons = [
    '定义一：自动定位词最近7天的平均ACOS值大于0.24小于0.5，并且自动定位词最近30天的平均ACOS值大于0小于0.24。降价0.03',
    '定义二：自动定位词最近7天的平均ACOS值大于0.24小于0.5，并且自动定位词最近30天的平均ACOS值大于0.24小于0.5。降价0.04',
    '定义三：自动定位词最近7天没有销售额并且近七天总点击数大于0，并且自动定位词最近30天的平均acos值大于0.24小于0.5。降价0.04',
    '定义四：自动定位词最近7天的平均ACOS值大于0.24小于0.5，并且自动定位词最近30天的平均ACOS值大于0.5。降价0.05',
    '定义五：自动定位词最近7天的平均ACOS值大于0.5，并且自动定位词最近30天的平均ACOS值大于0小于0.24。降价0.05'
]

results = []

for condition, discount, reason in zip(conditions, discounts, reasons):
    filtered_data = data[condition]
    filtered_data['降价的原因'] = reason
    results.append(filtered_data)

# 合并所有结果
final_results = pd.concat(results)

# 选择需要的列
final_results = final_results[['campaignName', 'adGroupName', 'keyword', 'ACOS_30d', 'ACOS_7d', '降价的原因']]

# 保存到新的CSV文件
output_file_path = 'C:\\DeepBI\\678904545.csv'
final_results.to_csv(output_file_path, index=False)

print("分析完成，结果保存在:", output_file_path)