# filename: filter_keywords.py

import pandas as pd

# 加载数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\自动定位组优化\预处理.csv'
data = pd.read_csv(file_path)

# 定义一：最近7天没有销售额且近7天总点击数大于0，且最近30天没有销售额，且近30天点击数大于10
def_def_1 = (data['total_sales14d_7d'] == 0) & (data['total_clicks_7d'] > 0) & (data['total_sales14d_30d'] == 0) & (data['total_clicks_30d'] > 10)

# 定义二：最近7天没有销售额且近7天总点击数大于0，且最近30天的ACOS值大于0.5
def_def_2 = (data['total_sales14d_7d'] == 0) & (data['total_clicks_7d'] > 0) & (data['ACOS_30d'] > 0.5)

# 定义三：最近7天ACOS值大于0.5，最近30天ACOS值大于0.24
def_def_3 = (data['ACOS_7d'] > 0.5) & (data['ACOS_30d'] > 0.24)

# 筛选满足定义一、定义二或定义三的记录
filtered_data = data[def_def_1 | def_def_2 | def_def_3].copy()

# 确定提价的原因
filtered_data['提价的原因'] = ""
filtered_data.loc[def_def_1, '提价的原因'] = "定义一"
filtered_data.loc[def_def_2, '提价的原因'] = "定义二"
filtered_data.loc[def_def_3, '提价的原因'] = "定义三"

# 选择需要输出的字段
output_data = filtered_data[['campaignName', 'adGroupName', 'keyword', 'ACOS_30d', 'ACOS_7d', '提价的原因']]

# 保存到新的CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\自动定位组优化\提问策略\自动_关闭自动定位组_IT_2024-06-08.csv'
output_data.to_csv(output_file_path, index=False)

print("数据处理完成并保存至文件:", output_file_path)