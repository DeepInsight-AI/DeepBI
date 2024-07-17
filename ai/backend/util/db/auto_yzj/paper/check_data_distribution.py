# filename: check_data_distribution.py
import pandas as pd

# 文件路径
input_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\滞销品优化\自动sp广告\关闭SKU\预处理.csv'

# 读取CSV文件到DataFrame
df = pd.read_csv(input_file_path)

# 打印整个数据集的描述信息
print("数据描述信息：")
print(df.describe())

# 检查各个字段的分布
print("\n字段类型信息：")
print(df.dtypes)

# 检查每个条件的分布情况
print("\nORDER_1m 小于 5 的数据数量:")
print((df['ORDER_1m'] < 5).sum())

print("\nACOS_7d 大于 0.6 的数据数量:")
print((df['ACOS_7d'] > 0.6).sum())

print("\ntotal_clicks_7d 大于 13 的数据数量:")
print((df['total_clicks_7d'] > 13).sum())

# 针对其他条件也可以进行类似检查...