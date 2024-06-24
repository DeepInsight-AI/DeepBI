# filename: calculate_sales_7d.py
import pandas as pd

# 读取CSV文件
data = pd.read_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\预处理.csv')

# 计算最近7天的总销售
data['date'] = pd.to_datetime(data['date'])
data['sales_7d'] = data.groupby(['campaignId'])['sales'].transform(lambda x: x.rolling('7D').sum())

# 确保所有必要的字段都存在
required_fields = [
    'date', 'campaignName', 'Budget', 'clicks', 'ACOS', 'avg_ACOS_7d', 'clicks_7d', 'sales_7d',
    'avg_ACOS_1m', 'clicks_1m', 'sales_1m', 'country_avg_ACOS_1m', 'cost'
]
missing_fields = [field for field in required_fields if field not in data.columns]
if missing_fields:
    print(f"Missing fields: {missing_fields}")
else:
    print("All required fields are present.")