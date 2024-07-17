# filename: find_and_save_skus.py

import pandas as pd

# 加载数据集
file_path = "C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\自动sp广告\\复开SKU\\预处理.csv"
data = pd.read_csv(file_path)

# 筛选满足条件的SKU
# 定义一的筛选条件
def1 = (data['ACOS_30d'] > 0) & (data['ACOS_30d'] <= 0.24) & (data['ACOS_7d'] > 0) & (data['ACOS_7d'] <= 0.24)

# 定义二的筛选条件
def2 = (data['ACOS_30d'] > 0) & (data['ACOS_30d'] <= 0.24) & (data['total_clicks_7d'] == 0)

# 组合条件筛选
filtered_data = data[def1 | def2].copy()

# 增加满足的定义字段
filtered_data['定义'] = "定义一"
filtered_data.loc[def2, '定义'] = "定义二"

# 选择需要的列
output_columns = [
    'campaignName', 'adId', 'adGroupName', 'ACOS_30d', 
    'ACOS_7d', 'total_clicks_7d', 'advertisedSku', 'ORDER_1m', '定义'
]
filtered_data = filtered_data[output_columns]

# 输出结果到CSV文件
output_file_path = "C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\自动sp广告\\复开SKU\\提问策略\\自动_复开SKU_v1_1_LAPASA_IT_2024-06-30.csv"
filtered_data.to_csv(output_file_path, index=False)

print(f"Filtered data has been saved to {output_file_path}")