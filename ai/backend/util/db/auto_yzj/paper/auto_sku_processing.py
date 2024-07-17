# filename: auto_sku_processing.py
import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\复开SKU\预处理.csv'
data = pd.read_csv(file_path)

# 定义筛选条件
condition_1 = (data['ACOS_30d'] > 0) & (data['ACOS_30d'] <= 0.24) & (data['ACOS_7d'] > 0) & (data['ACOS_7d'] <= 0.24)
condition_2 = (data['ACOS_30d'] > 0) & (data['ACOS_30d'] <= 0.24) & (data['total_clicks_7d'] == 0)

# 筛选符合条件的数据并添加新的列 - 满足的定义
filtered_data = data[condition_1 | condition_2].copy()
filtered_data['定义'] = '未定义'
filtered_data.loc[condition_1, '定义'] = '定义一'
filtered_data.loc[condition_2, '定义'] = '定义二'

# 选取需要的字段
output_data = filtered_data[
    ['campaignName', 'adId', 'adGroupName', 'ACOS_30d', 'ACOS_7d', 'total_clicks_7d', 'advertisedSku', 'ORDER_1m', '定义']
]

# 保存到新的CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\复开SKU\提问策略\自动_复开SKU_v1_1_LAPASA_ES_2024-06-30.csv'
output_data.to_csv(output_file_path, index=False)

print(f"数据已成功保存到 {output_file_path}")