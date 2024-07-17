# filename: fetch_sku_data.py
import pandas as pd

# 读取CSV数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\复开SKU\预处理.csv'
data = pd.read_csv(file_path)

# 定义过滤条件
condition1 = (data['ACOS_30d'] > 0) & (data['ACOS_30d'] <= 0.24) & (data['ACOS_7d'] > 0) & (data['ACOS_7d'] <= 0.24)
condition2 = (data['ACOS_30d'] > 0) & (data['ACOS_30d'] <= 0.24) & (data['total_clicks_7d'] == 0)

# 过滤符合定义一和定义二条件的数据
filtered_data = data[condition1 | condition2].copy()

# 添加满足的条件定义
filtered_data['满足的定义'] = '定义一'
filtered_data.loc[condition2, '满足的定义'] = '定义二'

# 选择并重命名输出列
output_columns = ['campaignName', 'adId', 'adGroupName', 'ACOS_30d', 'ACOS_7d', 'total_clicks_7d', 'advertisedSku', 'ORDER_1m', '满足的定义']
filtered_data = filtered_data[output_columns]

# 输出结果到新的CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\复开SKU\提问策略\手动_复开SKU_v1_1_LAPASA_ITES_2024-07-02.csv'
filtered_data.to_csv(output_file_path, index=False)

print("数据处理完成，结果已保存到输出文件。")