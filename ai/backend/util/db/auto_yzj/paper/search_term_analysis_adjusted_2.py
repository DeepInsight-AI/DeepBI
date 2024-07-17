# filename: search_term_analysis_adjusted_2.py

import pandas as pd

# 定义csv文件路径和输出文件路径
input_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放搜索词优化\预处理.csv'
output_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放搜索词优化\提问策略\手动_劣质_ASIN_搜索词_v1_1_IT_2024-06-30.csv'

# 读取csv文件
df = pd.read_csv(input_file)

# 定义调整后的条件
conditions = [
    (df['ACOS_30d'] > 0.1) & (df['ACOS_30d'] < 0.4) & (df['ORDER_1m'] <= 10),
    (df['ACOS_30d'] >= 0.4) & (df['ORDER_1m'] <= 15),
    (df['total_clicks_30d'] > 5) & (df['ORDER_1m'] == 0),
    (df['ACOS_7d'] > 0.1) & (df['ACOS_7d'] < 0.36) & (df['ORDER_7d'] <= 5),
    (df['ACOS_7d'] >= 0.36) & (df['ORDER_7d'] <= 10),
    (df['total_clicks_7d'] > 5) & (df['ORDER_7d'] == 0),
]

# 定义原因
reasons = [
    "最近一个月的平均ACOS值大于0.1且小于0.4且订单数小于等于10",
    "最近一个月的平均ACOS值大于等于0.4且订单数小于等于15",
    "最近一个月的总点击次数大于5且订单数为0",
    "最近7天的ACOS值大于0.1且小于0.36且订单数小于等于5",
    "最近7天的ACOS值大于等于0.36且订单数小于等于10",
    "最近七天的点击次数大于5且订单数为0",
]

# 用于存储符合条件的数据
result_df_list = []

# 筛选数据并记录原因
for condition, reason in zip(conditions, reasons):
    filtered_df = df[condition].copy()
    if not filtered_df.empty:
        filtered_df['reason'] = reason
        result_df_list.append(filtered_df)

# 合并所有符合条件的数据到一个结果集
if result_df_list:
    result_df = pd.concat(result_df_list, ignore_index=True)
    result_df = result_df[['campaignName', 'adGroupName', 'total_clicks_7d', 'ACOS_7d', 'ORDER_7d', 'total_clicks_30d', 'ORDER_1m', 'ACOS_30d', 'searchTerm', 'reason']]
    result_df.columns = ['Campaign Name', 'Ad Group Name', '7d Clicks', '7d ACOS', '7d Orders', '30d Clicks', '30d Orders', '30d ACOS', 'Search Term', 'Reason']
    # 保存结果到csv
    result_df.to_csv(output_file, index=False)
    print(f"结果已成功保存到 {output_file}")
else:
    print("没有符合条件的搜索词。")