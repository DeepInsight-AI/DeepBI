# filename: handle_missing_data.py
import pandas as pd

# 读取CSV文件
data = pd.read_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\关键词优化\预处理.csv')

# 使用平均值填充ACOS相关的NaN值
acos_columns = ['ACOS_7d', 'ACOS_30d', 'ACOS_yesterday']
for column in acos_columns:
    mean_value = data[column].mean()
    data[column].fillna(mean_value, inplace=True)

# 再次检查是否还有NaN值
has_nan = data[acos_columns].isna().any(axis=1)
print("Number of rows with NaN in ACOS columns after filling:", has_nan.sum())