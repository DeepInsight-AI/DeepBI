# filename: search_terms_analysis.py

import pandas as pd

# 加载数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放搜索词优化\预处理.csv'
data = pd.read_csv(file_path)

# 筛选符合定义一的搜索词
def_1 = (data['total_clicks_30d'] > 13) & (data['total_cost_30d'] > 7) & (data['ORDER_1m'] == 0)

# 筛选符合定义二的搜索词
def_2 = (data['total_clicks_7d'] > 10) & (data['total_cost_7d'] > 5) & (data['ORDER_7d'] == 0)

# 为符合条件的搜索词添加原因标签
data['reason'] = None
data.loc[def_1, 'reason'] = '定义一'
data.loc[def_2, 'reason'] = '定义二'

# 筛选出符合条件的搜索词行
filtered_data = data[data['reason'].notnull()]

# 选择需要输出的列
output_data = filtered_data[[
    'campaignName',       # 广告活动
    'adGroupName',        # 广告组
    'total_clicks_7d',    # 近七天的点击次数
    'ACOS_7d',            # 近七天的ACOS值
    'ORDER_7d',           # 近七天的订单数
    'total_cost_7d',      # 近七天的总花费
    'total_clicks_30d',   # 近一个月的总点击数
    'total_cost_30d',     # 近一个月的总花费
    'ORDER_1m',           # 近一个月的订单数
    'ACOS_30d',           # 近一个月的ACOS值
    'searchTerm',         # 搜索词
    'reason'              # 满足的定义
]]

# 输出结果到新的CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放搜索词优化\提问策略\手动_劣质_ASIN_搜索词_v1_1_LAPASA_ES_2024-07-15.csv'
output_data.to_csv(output_file_path, index=False)

print(f"Filtered search terms have been saved to {output_file_path}")