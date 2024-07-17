# filename: search_term_analysis.py

import pandas as pd

# 定义文件路径
file_path = "C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\手动sp广告\\商品投放搜索词优化\\预处理.csv"
output_path = "C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\手动sp广告\\商品投放搜索词优化\\提问策略\\手动_劣质_ASIN_搜索词_v1_1_LAPASA_FR_2024-07-15.csv"

# 读取数据集
df = pd.read_csv(file_path)

# 过滤数据：定义一
def1 = df[(df['total_clicks_30d'] > 13) & (df['total_cost_30d'] > 7) & (df['ORDER_1m'] == 0)]
def1['reason'] = '定义一'

# 过滤数据：定义二
def2 = df[(df['total_clicks_7d'] > 10) & (df['ORDER_7d'] == 0) & (df['total_cost_7d'] > 5)]
def2['reason'] = '定义二'

# 合并过滤结果
result_df = pd.concat([def1, def2])

# 选择所需的列，并重命名
result_df = result_df[[
    'campaignName', 'adGroupName', 'total_clicks_7d', 'ACOS_7d', 'ORDER_7d', 'total_cost_7d', 
    'total_clicks_30d', 'total_cost_30d', 'ORDER_1m', 'ACOS_30d', 'searchTerm', 'reason'
]]

result_df.columns = [
    'Campaign Name', 'adGroupName', '近七天的点击次数', '近七天的acos值', '近七天的订单数', '近七天的总花费', 
    '近一个月的总点击数', '近一个月的总花费', '近一个月的订单数', '近一个月的acos值', '搜索词', '满足的定义'
]

# 保存结果到CSV文件
result_df.to_csv(output_path, index=False, encoding='utf-8-sig')

print(f"结果已保存到 {output_path}")