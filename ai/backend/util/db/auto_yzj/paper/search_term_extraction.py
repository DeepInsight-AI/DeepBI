# filename: search_term_extraction.py
import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放搜索词优化\预处理.csv'
df = pd.read_csv(file_path)

# 筛选出满足“定义一”的搜索词
mask1 = (df['total_clicks_30d'] > 13) & (df['total_cost_30d'] > 7) & (df['ORDER_1m'] == 0)

# 筛选出满足“定义二”的搜索词
mask2 = (df['total_clicks_7d'] > 10) & (df['ORDER_7d'] == 0) & (df['total_cost_7d'] > 5)

# 组合两个筛选条件
filtered_df = df[mask1 | mask2].copy()

# 添加满足的定义原因
filtered_df['reason'] = ''
filtered_df.loc[mask1, 'reason'] = '定义一'
filtered_df.loc[mask2, 'reason'] = filtered_df.loc[mask2, 'reason'] + ' 定义二'    # 根据要求可能是一个或两个条件同事满足

# 保持必要的字段
output_fields = [
    'campaignName', 'adGroupName', 'total_clicks_7d', 'ACOS_7d', 'ORDER_7d', 'total_cost_7d',
    'total_clicks_30d', 'total_cost_30d', 'ORDER_1m', 'ACOS_30d', 'searchTerm', 'reason'
]

# 保存到新的CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放搜索词优化\提问策略\手动_劣质_ASIN_搜索词_v1_1_LAPASA_ES_2024-07-11.csv'
filtered_df.to_csv(output_file_path, index=False, columns=output_fields)

print(f"完成！结果已保存到 {output_file_path}")