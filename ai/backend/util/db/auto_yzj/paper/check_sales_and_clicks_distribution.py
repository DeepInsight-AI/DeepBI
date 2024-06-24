# filename: check_sales_and_clicks_distribution.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\预算优化\预处理.csv'
df = pd.read_csv(file_path)

# 查看 sales_1m 和 clicks_1m 列的分布
print("\nsales_1m 列的值分布:")
print(df['sales_1m'].value_counts())

print("\nclicks_1m 列的值分布:")
print(df['clicks_1m'].value_counts())