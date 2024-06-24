# filename: verify_output_file.py

import os
import pandas as pd

# 验证文件是否存在
output_file_path = 'C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/自动sp广告/自动定位组优化/提问策略/劣质自动定位组_FR.csv'

if os.path.exists(output_file_path):
    print(f"文件存在于: {output_file_path}")
    # 读取并显示文件内容的前几行
    results_data = pd.read_csv(output_file_path)
    print("文件内容的前几行:")
    print(results_data.head())
else:
    print(f"文件未找到: {output_file_path}")