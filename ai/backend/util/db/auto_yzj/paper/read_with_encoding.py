# filename: read_with_encoding.py
import pandas as pd

# 文件路径
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\预处理.csv'

# 尝试使用不同编码读取
data = pd.read_csv(file_path, encoding='utf-8-sig')

# 打印数据前几行以确认成功读取且无乱码
print(data.head())
print(f'File loaded successfully with {data.shape[0]} rows and {data.shape[1]} columns.')