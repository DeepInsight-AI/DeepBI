# filename: sku_selection.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\复开SKU\预处理.csv'
df = pd.read_csv(file_path)

# 通过定义条件筛选数据
def filter_skus(data):
    # 定义一条件
    condition_def1 = (data['ACOS_30d'] > 0) & (data['ACOS_30d'] <= 0.24) & \
                     (data['ACOS_7d'] > 0) & (data['ACOS_7d'] <= 0.24)
    
    # 定义二条件
    condition_def2 = (data['ACOS_30d'] > 0) & (data['ACOS_30d'] <= 0.24) & \
                     (data['total_clicks_7d'] == 0)
    
    # 满足定义一或定义二
    result = data[condition_def1 | condition_def2]
    return result

# 筛选符合条件的SKU
filtered_df = filter_skus(df)

# 指定输出的列
output_cols = ['campaignName', 'adId', 'adGroupName', 'ACOS_30d', 'ACOS_7d', 'total_clicks_7d', 'advertisedSku', 'ORDER_1m']
# 新增 '满足的定义' 列
filtered_df['满足的定义'] = filtered_df.apply(lambda row: '定义一' if ((row['ACOS_30d'] > 0 and row['ACOS_30d'] <= 0.24 and row['ACOS_7d'] > 0 and row['ACOS_7d'] <= 0.24)) else '定义二', axis=1)

# 保存结果到新CSV文件
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\复开SKU\提问策略\自动_复开SKU_v1_1_ES_2024-06-30.csv'
filtered_df[output_cols + ['满足的定义']].to_csv(output_path, index=False, encoding='utf-8')

print(f'筛选后的结果保存在 {output_path}') 