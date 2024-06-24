# filename: filter_auto_keywords.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\自动定位组优化\预处理.csv'
data = pd.read_csv(file_path, encoding='utf-8')

# 定义条件一
condition1 = (
    (data['total_sales14d_7d'] == 0) &
    (data['total_clicks_7d'] > 0) &
    (data['total_sales14d_30d'] == 0) &
    (data['total_clicks_30d'] > 10)
)

# 定义条件二
condition2 = (
    (data['total_sales14d_7d'] == 0) &
    (data['total_clicks_7d'] > 0) &
    (data['ACOS_30d'] > 0.5)
)

# 定义条件三
condition3 = (
    (data['ACOS_7d'] > 0.5) &
    (data['ACOS_30d'] > 0.24)
)

# 过滤符合条件的自动定位词
filtered_data_1 = data[condition1].copy()
filtered_data_1['reason'] = '定义一'

filtered_data_2 = data[condition2].copy()
filtered_data_2['reason'] = '定义二'

filtered_data_3 = data[condition3].copy()
filtered_data_3['reason'] = '定义三'

# 合并所有符合条件的数据
result_data = pd.concat([filtered_data_1, filtered_data_2, filtered_data_3])

# 保留所需的列
result_data = result_data[['campaignName', 'adGroupName', 'keyword', 'ACOS_30d', 'ACOS_7d', 'reason']]

# 保存到新的CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\自动定位组优化\提问策略\自动_关闭自动定位组_IT_2024-06-05.csv'
result_data.to_csv(output_file_path, index=False, encoding='utf-8')

print("结果已保存到", output_file_path)