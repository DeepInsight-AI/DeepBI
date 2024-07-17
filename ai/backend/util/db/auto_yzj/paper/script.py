# filename: script.py
import pandas as pd

# 读取数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放搜索词优化\预处理.csv'
df = pd.read_csv(file_path)

# 定义筛选条件
conditions = [
    ((df['ACOS_30d'] > 0.24) & (df['ACOS_30d'] < 0.36) & (df['ORDER_1m'] <= 5), '定义一'),
    ((df['ACOS_30d'] >= 0.36) & (df['ORDER_1m'] <= 8), '定义二'),
    ((df['total_clicks_30d'] > 13) & (df['ORDER_1m'] == 0), '定义三'),
    ((df['ACOS_7d'] > 0.24) & (df['ACOS_7d'] < 0.36) & (df['ORDER_7d'] <= 3), '定义四'),
    ((df['ACOS_7d'] >= 0.36) & (df['ORDER_7d'] <= 5), '定义五'),
    ((df['total_clicks_7d'] > 10) & (df['ORDER_7d'] == 0), '定义六')
]

# 筛选符合条件的数据，并添加原因列
filtered_data = pd.DataFrame()
for condition, reason in conditions:
    temp_df = df[condition].copy()
    temp_df['reason'] = reason
    filtered_data = pd.concat([filtered_data, temp_df])

# 创建最终输出数据框
output_columns = [
    'campaignName', 'adGroupName', 'total_clicks_7d', 'ACOS_7d', 'ORDER_7d',
    'total_clicks_30d', 'ORDER_1m', 'ACOS_30d', 'searchTerm', 'reason'
]
output_df = filtered_data[output_columns]

# 保存结果
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放搜索词优化\提问策略\手动_劣质_ASIN_搜索词_v1_1_LAPASA_ES_2024-07-10.csv'
output_df.to_csv(output_path, index=False)

print("处理完成，结果已保存到指定文件路径。")