# filename: 筛选符合条件的SKU_FR.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\SKU优化\预处理.csv'
data = pd.read_csv(file_path, encoding='utf-8-sig')

# 筛选符合条件的数据
filtered_data = data[
    # 定义一
    ((data['total_clicks_7d'] > 10) & (data['ACOS_7d'] > 0.24)) |
    # 定义二
    ((data['ACOS_30d'] > 0.24) & (data['total_sales14d_7d'] == 0) & (data['total_clicks_7d'] > 10)) |
    # 定义三
    ((data['ACOS_7d'] > 0.24) & (data['ACOS_7d'] < 0.5) & (data['ACOS_30d'] > 0) & (data['ACOS_30d'] < 0.24) & (data['total_clicks_7d'] > 13)) |
    # 定义四
    ((data['ACOS_7d'] > 0.24) & (data['ACOS_30d'] > 0.24)) |
    # 定义五
    (data['ACOS_7d'] > 0.5) |
    # 定义六
    ((data['total_clicks_30d'] > 13) & (data['total_sales14d_30d'] == 0))
]

# 选择需要的列
result = filtered_data[['campaignName', 'adGroupName', 'ACOS_30d', 'ACOS_7d', 'total_clicks_7d', 'advertisedSku']]

# 保存结果到新的CSV文件
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\SKU优化\提问策略\关闭SKU_FR.csv'
result.to_csv(output_path, index=False, encoding='utf-8-sig')

print('筛选并保存结果成功！')