# filename: filter_sku_data.py
import pandas as pd

# 数据源路径
csv_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\关闭SKU\预处理.csv'
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\关闭SKU\提问策略\SD_关闭SKU_v1_1_LAPASA_DE_2024-07-11.csv'

# 读取CSV数据
data = pd.read_csv(csv_file_path)

# 计算平均ACOS值（7天和30天）
data['ACOS_7d_avg'] = data['total_cost_7d'] / data['total_sales_7d']
data['ACOS_30d_avg'] = data['total_cost_30d'] / data['total_sales_30d']

# 定义各个条件
definitions = [
    # 定义一
    (data['ORDER_1m'] < 8) & (data['ACOS_7d_avg'] > 0.24) & (data['total_cost_7d'] > 5),
    # 定义二
    (data['ORDER_1m'] < 8) & (data['ACOS_30d_avg'] > 0.24) & (data['total_sales_7d'] == 0) & (data['total_cost_7d'] > 5),
    # 定义三
    (data['ORDER_1m'] < 8) & (data['ACOS_7d_avg'] > 0.24) & (data['ACOS_7d_avg'] < 0.5) & (data['ACOS_30d_avg'] > 0) & (data['ACOS_30d_avg'] < 0.24) & (data['total_cost_7d'] > 5),
    # 定义四
    (data['ORDER_1m'] < 8) & (data['ACOS_7d_avg'] > 0.24) & (data['ACOS_30d_avg'] > 0.24),
    # 定义五
    (data['ACOS_7d_avg'] > 0.5),
    # 定义六
    (data['total_cost_30d'] > 5) & (data['total_sales_30d'] == 0),
    # 定义七
    (data['ORDER_1m'] < 8) & (data['total_cost_7d'] >= 5) & (data['total_sales_7d'] == 0),
    # 定义八
    (data['ORDER_1m'] >= 8) & (data['total_cost_7d'] >= 10) & (data['total_sales_7d'] == 0)
]

combined_condition = definitions[0]
for definition in definitions[1:]:
    combined_condition |= definition

# 筛选出符合条件的记录
filtered_data = data[combined_condition]

# 添加满足的定义列（根据定义的条件）
filtered_data['definition'] = 'N/A'
filtered_data.loc[definitions[0], 'definition'] = '定义一'
filtered_data.loc[definitions[1], 'definition'] = '定义二'
filtered_data.loc[definitions[2], 'definition'] = '定义三'
filtered_data.loc[definitions[3], 'definition'] = '定义四'
filtered_data.loc[definitions[4], 'definition'] = '定义五'
filtered_data.loc[definitions[5], 'definition'] = '定义六'
filtered_data.loc[definitions[6], 'definition'] = '定义七'
filtered_data.loc[definitions[7], 'definition'] = '定义八'

# 选择需要的列
output_columns = [
    'campaignName',
    'adId',
    'adGroupName',
    'ACOS_30d_avg',
    'ACOS_7d_avg',
    'total_clicks_7d',
    'advertisedSku',
    'ORDER_1m',
    'definition'
]

# 保存结果到新的CSV文件
filtered_data[output_columns].to_csv(output_file_path, index=False)

print(f"筛选结果已保存到 {output_file_path}")