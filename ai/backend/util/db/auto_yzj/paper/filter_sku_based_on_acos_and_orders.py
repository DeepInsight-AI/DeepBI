# filename: filter_sku_based_on_acos_and_orders.py

import pandas as pd

# 数据集路径
input_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\复开SKU\预处理.csv'
output_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\复开SKU\提问策略\自动_复开SKU_v1_1_ES_2024-06-27_revised.csv'

# 读取CSV文件
df = pd.read_csv(input_file)

# 过滤满足条件的SKU：ACOS_30d大于0且小于等于0.24
condition = (df['ACOS_30d'] > 0) & (df['ACOS_30d'] <= 0.24)

# 创建符合条件的数据框的副本
filtered_df = df[condition].copy()

# 如果没有符合条件的记录，输出消息并退出
if filtered_df.empty:
    print("No records found matching the ACOS_30d condition.")
else:
    # 选择需要的列
    result_df = filtered_df[[
        'campaignName', 
        'adId', 
        'adGroupName', 
        'ACOS_30d', 
        'ACOS_7d', 
        'total_clicks_7d', 
        'advertisedSku',
        'ORDER_1m'
    ]]

    # 将结果保存到输出文件
    result_df.to_csv(output_file, index=False, encoding='utf-8-sig')

    print(f"Filtered data has been saved to {output_file} based on ACOS_30d condition.")