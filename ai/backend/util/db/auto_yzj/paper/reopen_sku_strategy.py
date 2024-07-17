# filename: reopen_sku_strategy.py

import pandas as pd

# 数据集路径
input_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\复开SKU\预处理.csv'
output_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\复开SKU\提问策略\自动_复开SKU_v1_1_ES_2024-06-27.csv'

# 读取CSV文件
df = pd.read_csv(input_file)

# 过滤满足条件的SKU：定义一或定义二
condition_1 = (df['ACOS_30d'] > 0) & (df['ACOS_30d'] <= 0.24) & (df['ACOS_7d'] > 0) & (df['ACOS_7d'] <= 0.24)
condition_2 = (df['ACOS_30d'] > 0) & (df['ACOS_30d'] <= 0.24) & (df['total_clicks_7d'] == 0)

# 创建符合条件的数据框的副本
filtered_df = df[condition_1 | condition_2].copy()

# 如果没有符合条件的记录，输出消息并退出
if filtered_df.empty:
    print("No records found matching the criteria.")
else:
    # 为新列初始化为NaN
    filtered_df['满足的定义'] = pd.NA

    # 增加新列 '满足的定义' 来标识条件
    filtered_df.loc[condition_1, '满足的定义'] = '定义一'
    filtered_df.loc[condition_2, '满足的定义'] = '定义二'

    # 选择需要的列
    result_df = filtered_df[[
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

    # 将结果保存到输出文件
    result_df.to_csv(output_file, index=False, encoding='utf-8-sig')

    print(f"Filtered data has been saved to {output_file}")