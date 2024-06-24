# filename: find_sku_to_close.py

import pandas as pd

# 1. 加载数据
file_path = "C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/手动sp广告/SKU优化/预处理.csv"
data = pd.read_csv(file_path)

# 2. 定义筛选条件

def filter_skus(data):
    # 定义各种原因的筛选条件
    conditions = {
        "定义一": (data['total_clicks_7d'] > 10) & (data['ACOS_7d'] > 0.24),
        "定义二": (data['ACOS_30d'] > 0.24) & (data['total_sales14d_7d'] == 0) & (data['total_clicks_7d'] > 10),
        "定义三": (data['ACOS_7d'] > 0.24) & (data['ACOS_7d'] < 0.5) & (data['ACOS_30d'] > 0) & (data['ACOS_30d'] < 0.24) & (data['total_clicks_7d'] > 13),
        "定义四": (data['ACOS_7d'] > 0.24) & (data['ACOS_30d'] > 0.24),
        "定义五": (data['ACOS_7d'] > 0.5),
        "定义六": (data['total_clicks_30d'] > 13) & (data['total_sales14d_30d'] == 0)
    }
    
    # 用于存放筛选结果的数据框
    filtered_data_list = []
    
    # 逐个条件应用并添加原因列
    for reason, condition in conditions.items():
        filtered_data = data[condition].copy()
        filtered_data['关闭操作原因'] = reason
        filtered_data_list.append(filtered_data)
    
    # 合并所有结果
    filtered_data = pd.concat(filtered_data_list, ignore_index=True)
    
    return filtered_data

# 3. 筛选数据
filtered_data = filter_skus(data)

# 4. 选择并重新命名所需字段
filtered_data = filtered_data[
    ['campaignName', 'adGroupName', 'ACOS_30d', 'ACOS_7d', 'total_clicks_7d', 'advertisedSku', '关闭操作原因']
    ].rename(columns={
    'campaignName': '广告活动', 
    'adGroupName': '广告组', 
    'ACOS_30d': '近30天的acos值', 
    'ACOS_7d': '近7天的acos值', 
    'total_clicks_7d': '近7天的点击数', 
    'advertisedSku': 'sku'
})

# 5. 保存结果
output_file_path = "C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/手动sp广告/SKU优化/提问策略/关闭SKU_FR_2024-5-27.csv"
filtered_data.to_csv(output_file_path, index=False)

print("筛选结果已保存到:", output_file_path)