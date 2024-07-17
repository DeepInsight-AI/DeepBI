# filename: find_negative_search_terms.py

import pandas as pd

# 读取数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放搜索词优化\预处理.csv'
data = pd.read_csv(file_path)

# 筛选定义一的搜索词
filtered_def1 = data[
    (data['total_clicks_30d'] > 13) &
    (data['total_cost_30d'] > 7) &
    (data['ORDER_1m'] == 0)
]
filtered_def1 = filtered_def1.copy()
filtered_def1['reason'] = '定义一'

# 筛选定义二的搜索词
filtered_def2 = data[
    (data['total_clicks_7d'] > 10) &
    (data['total_cost_7d'] > 5) &
    (data['ORDER_7d'] == 0)
]
filtered_def2 = filtered_def2.copy()
filtered_def2['reason'] = '定义二'

# 合并结果
results = pd.concat([filtered_def1, filtered_def2]).drop_duplicates()

# 选择并重命名要输出的列
output_columns = {
    'campaignName': '广告活动',
    'adGroupName': '广告组',
    'total_clicks_7d': '近七天的点击次数',
    'ACOS_7d': '近七天的acos值',
    'ORDER_7d': '近七天的订单数',
    'total_cost_7d': '近七天的总花费',
    'total_clicks_30d': '近一个月的总点击数',
    'total_cost_30d': '近一个月的总花费',
    'ORDER_1m': '近一个月的订单数',
    'ACOS_30d': '近一个月的acos值',
    'searchTerm': '搜索词',
    'reason': '满足的定义'
}
results = results[list(output_columns.keys())].rename(columns=output_columns)

# 保存结果到CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放搜索词优化\提问策略\手动_劣质_ASIN_搜索词_v1_1_LAPASA_IT_2024-07-13.csv'
results.to_csv(output_file_path, index=False, encoding='utf-8-sig')

print(f"筛选结果已成功保存至 {output_file_path}")