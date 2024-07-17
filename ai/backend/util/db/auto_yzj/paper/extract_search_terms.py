# filename: extract_search_terms.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放搜索词优化\预处理.csv'
data = pd.read_csv(file_path)

# 确保所需列存在
required_columns = [
    'ACOS_30d', 'ORDER_1m', 'total_clicks_30d', 
    'ACOS_7d', 'ORDER_7d', 'total_clicks_7d', 
    'campaignName', 'adGroupName', 'searchTerm'
]
for col in required_columns:
    if col not in data.columns:
        raise ValueError(f"Missing required column: {col}")

# 定义各个条件过滤
condition1 = (data['ACOS_30d'] > 0.24) & (data['ACOS_30d'] < 0.36) & (data['ORDER_1m'] <= 5)
condition2 = (data['ACOS_30d'] >= 0.36) & (data['ORDER_1m'] <= 8)
condition3 = (data['total_clicks_30d'] > 13) & (data['ORDER_1m'] == 0)
condition4 = (data['ACOS_7d'] > 0.24) & (data['ACOS_7d'] < 0.36) & (data['ORDER_7d'] <= 3)
condition5 = (data['ACOS_7d'] >= 0.36) & (data['ORDER_7d'] <= 5)
condition6 = (data['total_clicks_7d'] > 10) & (data['ORDER_7d'] == 0)

# 合并所有条件
all_conditions = condition1 | condition2 | condition3 | condition4 | condition5 | condition6

# 筛选满足条件的行
filtered_data = data[all_conditions].copy()

# 添加满足的定义为新列
filtered_data['reason'] = ''
filtered_data.loc[condition1, 'reason'] = '定义一'
filtered_data.loc[condition2, 'reason'] = '定义二'
filtered_data.loc[condition3, 'reason'] = '定义三'
filtered_data.loc[condition4, 'reason'] = '定义四'
filtered_data.loc[condition5, 'reason'] = '定义五'
filtered_data.loc[condition6, 'reason'] = '定义六'

# 选择需要导出的列
output_cols = [
    'campaignName',
    'adGroupName',
    'total_clicks_7d',
    'ACOS_7d',
    'ORDER_7d',
    'total_clicks_30d',
    'ORDER_1m',
    'ACOS_30d',
    'searchTerm',
    'reason'
]
output_data = filtered_data[output_cols]

# 保存结果到新的CSV文件
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放搜索词优化\提问策略\手动_劣质_ASIN_搜索词_v1_1_LAPASA_DE_2024-07-03.csv'
output_data.to_csv(output_path, index=False)

print("Process completed and results saved.")