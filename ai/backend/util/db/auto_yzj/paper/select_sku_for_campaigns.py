# filename: select_sku_for_campaigns.py

import pandas as pd

# 读取CSV文件
data = pd.read_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\复开SKU\预处理.csv')

# 条件定义一
condition_1 = (data['ACOS_30d'] > 0) & (data['ACOS_30d'] <= 0.24) & \
              (data['ACOS_7d'] > 0) & (data['ACOS_7d'] <= 0.24)

# 条件定义二
condition_2 = (data['ACOS_30d'] > 0) & (data['ACOS_30d'] <= 0.24) & \
              (data['total_clicks_7d'] == 0)

# 满足定义一或定义二
filtered_data = data[condition_1 | condition_2]

# 添加满足的定义列
filtered_data['satisfied_definition'] = filtered_data.apply(lambda row: '定义一' if (row['ACOS_30d'] > 0 and row['ACOS_30d'] <= 0.24 and row['ACOS_7d'] > 0 and row['ACOS_7d'] <= 0.24) else '定义二', axis=1)

# 选择需要输出的列
output_columns = ['campaignName', 'adId', 'adGroupName', 'ACOS_30d', 'ACOS_7d', 'total_clicks_7d', 'advertisedSku', 'ORDER_1m', 'satisfied_definition']
output_data = filtered_data[output_columns]

# 保存输出到CSV文件
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\复开SKU\提问策略\手动_复开SKU_v1_1_IT_2024-06-30.csv'
output_data.to_csv(output_path, index=False)

print("任务完成，结果已保存至:", output_path)