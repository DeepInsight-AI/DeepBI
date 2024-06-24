# filename: load_and_prepare_data.py
import pandas as pd

# 加载数据
data = pd.read_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\商品\预处理1.csv')

# 确保所有必要的字段都存在
required_fields = [
    'campaignName', 'adGroupName', 'advertisedSku', 'clicks_yesterday',
    'total_clicks_7d', 'total_clicks_30d', 'purchases7d_yesterday',
    'total_purchases7d_7d', 'total_purchases7d_30d', 'ACOS_yesterday',
    'ACOS_7d', 'ACOS_30d'
]

for field in required_fields:
    if field not in data.columns:
        raise ValueError(f"Missing required field: {field}")

# 打印数据的前几行以验证加载
print(data.head())