# filename: qualify_bid_adjustment.py

import pandas as pd
import matplotlib.pyplot as plt

# 读取数据集
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\预处理.csv'
data = pd.read_csv(file_path)

# 只对数值列进行中位数填充
numeric_cols = data.select_dtypes(include=['float64', 'int64']).columns
data[numeric_cols] = data[numeric_cols].fillna(data[numeric_cols].median())

# 绘制数据分布图表
plt.figure(figsize=(14, 10))

plt.subplot(2, 2, 1)
data['ACOS_7d'].plot(kind='hist', title='ACOS 7d Distribution')
plt.subplot(2, 2, 2)
data['ACOS_3d'].plot(kind='hist', title='ACOS 3d Distribution')

plt.subplot(2, 2, 3)
data['total_clicks_7d'].plot(kind='hist', title='Total Clicks 7d Distribution')
plt.subplot(2, 2, 4)
data['total_clicks_3d'].plot(kind='hist', title='Total Clicks 3d Distribution')

plt.tight_layout()
plt.savefig(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\提问策略\数据分布图.png')

print("数据分布图已经保存至 C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\自动sp广告\\广告位优化\\提问策略\\数据分布图.png")