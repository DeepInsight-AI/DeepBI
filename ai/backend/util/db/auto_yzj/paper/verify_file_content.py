# filename: verify_file_content.py
import pandas as pd

# 文件路径
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\预处理.csv'

# 读取CSV文件
data = pd.read_csv(file_path)

# 检查文件是否存在和文件是否被完全读取
print(f'File loaded successfully with {data.shape[0]} rows and {data.shape[1]} columns.')
print(data.head(10)) # 显示前几行数据，用于确认文件内容