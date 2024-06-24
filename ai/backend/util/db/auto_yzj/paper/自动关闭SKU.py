# filename: sku_optimization_filter.py

import pandas as pd

# 读取CSV文件
file_path = "C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\自动sp广告\\SKU优化\\预处理.csv"
data = pd.read_csv(file_path)

# 筛选符合条件的SKU
filtered_data = data[
    ((data['total_clicks_7d'] > 10) & (data['ACOS_7d'] > 0.24)) |
    ((data['ACOS_30d'] > 0.24) & (data['total_sales14d_7d'] == 0) & (data['total_clicks_7d'] > 10)) |
    ((data['ACOS_7d'] > 0.24) & (data['ACOS_7d'] < 0.5) & (data['ACOS_30d'] > 0) & (data['ACOS_30d'] < 0.24) & (data['total_clicks_7d'] > 13)) |
    ((data['ACOS_7d'] > 0.24) & (data['ACOS_30d'] > 0.24)) |
    (data['ACOS_7d'] > 0.5) |
    ((data['total_clicks_30d'] > 13) & (data['total_sales14d_30d'] == 0))
]

# 选择需要的列
output_data = filtered_data[[
    'campaignName', 'adId', 'adGroupName', 'ACOS_30d', 'ACOS_7d', 'total_clicks_7d', 'advertisedSku'
]]

# 输出结果到CSV文件
output_file_path = "C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\自动sp广告\\SKU优化\\提问策略\\自动_关闭SKU_v1_1_IT_2024-06-13.csv"
output_data.to_csv(output_file_path, index=False)

print("筛选并保存完毕！")