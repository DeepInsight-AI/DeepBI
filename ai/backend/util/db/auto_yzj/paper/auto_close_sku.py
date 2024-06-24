# filename: auto_close_sku.py

import pandas as pd

# 加载数据
file_path = "C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\自动sp广告\\SKU优化\\预处理.csv"
data = pd.read_csv(file_path, encoding='utf-8')

# 按定义筛选数据
def filter_skus(data):
    conditions = []

    # 定义一
    condition1 = (data['total_clicks_7d'] > 10) & (data['ACOS_7d'] > 0.24)
    if any(condition1):
        data.loc[condition1, '关闭原因'] = '定义一'
        conditions.append(condition1)

    # 定义二
    condition2 = (data['ACOS_30d'] > 0.24) & (data['total_sales14d_7d'] == 0) & (data['total_clicks_7d'] > 10)
    if any(condition2):
        data.loc[condition2, '关闭原因'] = '定义二'
        conditions.append(condition2)

    # 定义三
    condition3 = (data['ACOS_7d'] > 0.24) & (data['ACOS_7d'] < 0.5) & (data['ACOS_30d'] > 0) & (data['ACOS_30d'] < 0.24) & (data['total_clicks_7d'] > 13)
    if any(condition3):
        data.loc[condition3, '关闭原因'] = '定义三'
        conditions.append(condition3)

    # 定义四
    condition4 = (data['ACOS_7d'] > 0.24) & (data['ACOS_30d'] > 0.24)
    if any(condition4):
        data.loc[condition4, '关闭原因'] = '定义四'
        conditions.append(condition4)
    
    # 定义五
    condition5 = (data['ACOS_7d'] > 0.5)
    if any(condition5):
        data.loc[condition5, '关闭原因'] = '定义五'
        conditions.append(condition5)
    
    # 定义六
    condition6 = (data['total_clicks_30d'] > 13) & (data['total_sales14d_30d'] == 0)
    if any(condition6):
        data.loc[condition6, '关闭原因'] = '定义六'
        conditions.append(condition6)
    
    # 合并所有条件
    combined_condition = condition1 | condition2 | condition3 | condition4 | condition5 | condition6
    return data.loc[combined_condition]

# 筛选满足条件的SKU
result = filter_skus(data)

# 选择需要的列
result = result[['campaignName', 'adGroupName', 'ACOS_30d', 'ACOS_7d', 'total_clicks_7d', 'advertisedSku', '关闭原因']]

# 保存结果到 CSV 文件
output_path = "C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\自动sp广告\\SKU优化\\提问策略\\自动_关闭SKU_IT_2024-06-08.csv"
result.to_csv(output_path, index=False, encoding='utf-8')

print("数据已保存至: " + output_path)