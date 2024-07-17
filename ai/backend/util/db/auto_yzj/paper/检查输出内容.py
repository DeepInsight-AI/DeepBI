# filename: 检查输出内容.py

import pandas as pd

# 检查CSV文件内容
output_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_劣质商品投放_v1_1_LAPASA_DE_2024-07-03.csv'
output_data = pd.read_csv(output_file)

# 输出前几行作为检查
print("输出文件的前几行内容:")
print(output_data.head())