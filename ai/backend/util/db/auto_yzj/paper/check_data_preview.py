# filename: check_data_preview.py
import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\预处理.csv'
data = pd.read_csv(file_path)

# 输出前几行数据以便检查
print(data.head())
print(f"Number of records in the dataset: {len(data)}")