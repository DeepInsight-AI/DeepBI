# filename: list_files.py

import os

# 指定目录
directory = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\SKU优化'

# 列出目录中的文件
if os.path.exists(directory):
    files = os.listdir(directory)
    print("目录中的文件：")
    for file in files:
        print(file)
else:
    print(f"目录未找到: {directory}")