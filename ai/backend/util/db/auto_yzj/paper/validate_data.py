# filename: validate_data.py
import pandas as pd

# 读取CSV文件
data = pd.read_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\关键词优化\预处理.csv')

# 检查ACOS相关列是否有NaN值
acos_columns = ['ACOS_7d', 'ACOS_30d', 'ACOS_yesterday']
has_nan = data[acos_columns].isna().any(axis=1)

# 输出包含NaN值的行数
print("Number of rows with NaN in ACOS columns:", has_nan.sum())