# filename: find_sku_by_acos.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\复开SKU\预处理.csv'
data = pd.read_csv(file_path)

# 筛选满足定义一的SKU
condition1 = (data['ACOS_30d'] > 0) & (data['ACOS_30d'] <= 0.24) & (data['ACOS_7d'] > 0) & (data['ACOS_7d'] <= 0.24)

# 筛选满足定义二的SKU
condition2 = (data['ACOS_30d'] > 0) & (data['ACOS_30d'] <= 0.24) & (data['total_clicks_7d'] == 0)

# 合并条件
filtered_data = data[condition1 | condition2]

# 添加一个新列，用于标记满足的定义
filtered_data['满足的定义'] = ""
filtered_data.loc[condition1 & (condition2 == False), '满足的定义'] = "定义一"
filtered_data.loc[condition2 == True, '满足的定义'] = "定义二"

# 选择所需的列
result = filtered_data[[
    'campaignName', 
    'adId', 
    'adGroupName', 
    'ACOS_30d', 
    'ACOS_7d', 
    'total_clicks_7d', 
    'advertisedSku', 
    'ORDER_1m', 
    '满足的定义'
]]

# 保存结果到CSV文件
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\复开SKU\提问策略\手动_复开SKU_v1_1_LAPASA_UK_2024-07-02.csv'
result.to_csv(output_path, index=False)

print("结果已保存到文件:", output_path)