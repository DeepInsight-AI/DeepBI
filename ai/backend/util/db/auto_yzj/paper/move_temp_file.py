# filename: move_temp_file.py
import os

temp_file_path = r'C:\Users\admin\AppData\Local\Temp\tmpkyql83o5.csv'
final_output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\提问策略\劣质广告位_FR.csv'

# 移动文件到最终路径
os.replace(temp_file_path, final_output_path)

print(f"Results moved to: {final_output_path}")