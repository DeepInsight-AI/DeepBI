# filename: check_file_exists.py
import os

file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\预处理.csv'

# 检查文件是否存在
if os.path.exists(file_path):
    print("文件存在。")
else:
    print("文件不存在，请确认文件路径。")

# 再次检查文件内容是否为空
with open(file_path, 'r') as file:
    content = file.readlines()

if len(content) > 0:
    print("文件读出成功，文件内容如下：")
    print(''.join(content[:10]))  # 打印文件前10行
else:
    print("文件为空。")