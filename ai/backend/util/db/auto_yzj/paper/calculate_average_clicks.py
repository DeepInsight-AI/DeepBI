# filename: calculate_average_clicks.py
import pandas as pd

# 读取CSV文件
data = pd.read_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\商品\预处理1.csv')

# 计算7天和30天的每单平均点击次数
data['7d_average_clicks'] = data['total_clicks_7d'] / data['total_purchases7d_7d']
data['30d_average_clicks'] = data['total_clicks_30d'] / data['total_purchases7d_30d']

# 打印计算结果
print(data[['advertisedSku', '7d_average_clicks', '30d_average_clicks']])