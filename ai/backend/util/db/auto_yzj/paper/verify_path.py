# filename: verify_path.py

import os

# 原始文件路径
data_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\预处理.csv'

# 输出文件路径和检查文件是否存在
print(f"Checking file path: {data_file}")
file_exists = os.path.exists(data_file)
print(f"File exists: {file_exists}")