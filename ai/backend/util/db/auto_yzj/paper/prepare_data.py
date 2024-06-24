# filename: prepare_data.py
import pandas as pd

# 读取数据
data = pd.read_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\商品\预处理1.csv')

# 确保数据集包含所有必要的字段
required_columns = [
    'campaignName', 'adGroupName', 'advertisedSku', 'clicks_yesterday',
    'total_clicks_7d', 'total_clicks_30d', 'purchases7d_yesterday',
    'total_purchases7d_7d', 'total_purchases7d_30d', 'ACOS_yesterday',
    'ACOS_7d', 'ACOS_30d'
]

for col in required_columns:
    if col not in data.columns:
        raise ValueError(f"Column {col} not found in the dataset")

# 数据预处理
data['total_clicks_7d'] = data['total_clicks_7d'].fillna(0)
data['total_clicks_30d'] = data['total_clicks_30d'].fillna(0)
data['total_purchases7d_7d'] = data['total_purchases7d_7d'].fillna(0)
data['total_purchases7d_30d'] = data['total_purchases7d_30d'].fillna(0)
data['ACOS_7d'] = data['ACOS_7d'].fillna(0)
data['ACOS_30d'] = data['ACOS_30d'].fillna(0)

# 计算7天和30天每单平均点击次数
data['average_clicks_per_order_7d'] = (data['total_clicks_7d'] / data['total_purchases7d_7d']).round(2)
data['average_clicks_per_order_30d'] = (data['total_clicks_30d'] / data['total_purchases7d_30d']).round(2)

# 异常判断
data['anomaly_description'] = ''

# 销售额为0检查
zero_sales_mask = (data['ACOS_yesterday'].isnull()) & (data['purchases7d_yesterday'] == 0) & (data['clicks_yesterday'] > data['average_clicks_per_order_7d'])
data.loc[zero_sales_mask, 'anomaly_description'] = '昨天点击量足够但无销售'

# ACOS波动检查
delta_7d = (data['ACOS_yesterday'] - data['ACOS_7d']) / data['ACOS_7d']
delta_30d = (data['ACOS_yesterday'] - data['ACOS_30d']) / data['ACOS_30d']

delta_7d_mask = (delta_7d.abs() > 0.3)
delta_30d_mask = (delta_30d.abs() > 0.3)

data.loc[delta_7d_mask, 'anomaly_description'] = 'ACOS波动异常（7天）'
data.loc[delta_30d_mask, 'anomaly_description'] = 'ACOS波动异常（30天）'

# 输出结果
output_data = data[['campaignName', 'adGroupName', 'advertisedSku', 'anomaly_description', 'ACOS_yesterday', 'ACOS_7d', 'ACOS_30d', 'clicks_yesterday', 'average_clicks_per_order_7d', 'average_clicks_per_order_30d']]
output_data.to_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\商品\提问策略\商品_点击量足够但ACOS异常1_FR_2024-05-17_deepseek.csv', index=False)