# filename: load_and_filter_data.py
import pandas as pd

# 加载数据
file_path = "C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\自动sp广告\\广告位优化\\预处理.csv"
data = pd.read_csv(file_path)

# 查看数据头部
print(data.head())