# filename: process_skus.py

import pandas as pd

# 读取csv文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\关闭SKU\预处理.csv'
data = pd.read_csv(file_path)

# 定义筛选条件
conditions = [
    # 定义一
    (data['ORDER_1m'] < 8) & (data['ACOS_7d'] > 0.24) & (data['total_cost_7d'] > 5),
    # 定义二
    (data['ORDER_1m'] < 8) & (data['ACOS_30d'] > 0.24) & (data['total_sales_7d'] == 0) & (data['total_cost_7d'] > 5),
    # 定义三
    (data['ORDER_1m'] < 8) & (data['ACOS_7d'] > 0.24) & (data['ACOS_7d'] < 0.5) & (data['ACOS_30d'] > 0) & (data['ACOS_30d'] < 0.24) & (data['total_cost_7d'] > 5),
    # 定义四
    (data['ORDER_1m'] < 8) & (data['ACOS_7d'] > 0.24) & (data['ACOS_30d'] > 0.24),
    # 定义五
    (data['ACOS_7d'] > 0.5),
    # 定义六
    (data['total_cost_30d'] > 5) & (data['total_sales_30d'] == 0),
    # 定义七
    (data['ORDER_1m'] < 8) & (data['total_cost_7d'] >= 5) & (data['total_sales_7d'] == 0),
    # 定义八
    (data['ORDER_1m'] >= 8) & (data['total_cost_7d'] >= 10) & (data['total_sales_7d'] == 0)
]

# 合并所有条件
data['定义'] = '不符合'
for i, condition in enumerate(conditions, 1):
    data.loc[condition, '定义'] = f'定义{i}'

# 筛选符合条件的数据
result = data[data['定义'] != '不符合']

# 构造输出的DataFrame
output_columns = ['campaignName', 'adId', 'adGroupName', 'ACOS_30d', 'ACOS_7d', 'total_clicks_7d', 'advertisedSku', 'ORDER_1m', '定义']
output_data = result[output_columns]

# 保存结果
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\关闭SKU\提问策略\SD_关闭SKU_v1_1_LAPASA_FR_2024-07-15.csv'
output_data.to_csv(output_file_path, index=False)

print(f'任务完成，结果已保存到 {output_file_path}')