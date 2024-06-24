# filename: verify_results.py

import pandas as pd

# 验证生成的文件内容
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\提问策略\自动_优质广告位_ES_2024-06-10.csv'
result_df = pd.read_csv(output_file_path)

# 打印前几行内容进行验证
print(result_df.head())