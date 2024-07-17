# filename: process_search_terms.py

import pandas as pd

# 读取数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放搜索词优化\预处理.csv'
df = pd.read_csv(file_path)

# 定义结果列表
results = []

# 定义筛选条件和筛选
conditions = [
    ((df['ACOS_30d'] > 0.24) & (df['ACOS_30d'] < 0.36) & (df['ORDER_1m'] <= 5), '定义一'),
    ((df['ACOS_30d'] >= 0.36) & (df['ORDER_1m'] <= 8), '定义二'),
    ((df['total_clicks_30d'] > 13) & (df['ORDER_1m'] == 0), '定义三'),
    ((df['ACOS_7d'] > 0.24) & (df['ACOS_7d'] < 0.36) & (df['ORDER_7d'] <= 3), '定义四'),
    ((df['ACOS_7d'] >= 0.36) & (df['ORDER_7d'] <= 5), '定义五'),
    ((df['total_clicks_7d'] > 10) & (df['ORDER_7d'] == 0), '定义六')
]

for condition, reason in conditions:
    result_df = df[condition].copy()
    result_df['reason'] = reason
    results.append(result_df)

# 合并所有结果
final_df = pd.concat(results)

# 选取需要的列并重命名
final_df = final_df[['campaignName', 'adGroupName', 'total_clicks_7d', 'ACOS_7d', 'ORDER_7d',
                     'total_clicks_30d', 'ORDER_1m', 'ACOS_30d', 'searchTerm', 'reason']]
final_df.columns = ['广告活动', '广告组', '近七天的点击次数', '近七天的acos值', '近七天的订单数',
                    '近一个月的总点击数', '近一个月的订单数', '近一个月的acos值', '搜索词', '满足的定义']

# 保存结果
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放搜索词优化\提问策略\手动_劣质_ASIN_搜索词_v1_1_LAPASA_US_2024-07-09.csv'
final_df.to_csv(output_path, index=False)

print(f"处理完成，结果已保存至 {output_path}")