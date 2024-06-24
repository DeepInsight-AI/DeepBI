# filename: read_and_filter_data.py
import pandas as pd

# 读取数据
data = pd.read_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\搜索词优化\预处理.csv')

# 假设今天是2024年5月27日，计算近七天的日期范围
today = pd.to_datetime('2024-05-27')
start_date_7d = today - pd.DateOffset(days=7)

# 筛选出近七天有销售额且ACOS值小于0.2的搜索词
filtered_data = data[(data['total_sales14d_7d'] > 0) & (data['ACOS_7d'] < 0.2)]

# 输出筛选后的数据
print(filtered_data)