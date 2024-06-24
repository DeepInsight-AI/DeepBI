# filename: confirm_file_path.py

import os

file_path = input("请输入文件的完整路径: ")

if not os.path.exists(file_path):
    print(f"文件未找到: {file_path}")
else:
    print(f"文件已找到: {file_path}")