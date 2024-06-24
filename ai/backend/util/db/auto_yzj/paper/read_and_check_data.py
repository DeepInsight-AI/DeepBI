# filename: read_and_check_data.py
import pandas as pd

# 读取CSV文件
data = pd.read_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\关键词优化\预处理.csv')

# 检查数据中是否包含所有需要的列
required_columns = [
    'keywordId', 'keyword', 'targeting', 'keywordBid', 'matchType', 'adGroupName', 'campaignName',
    'ORDER_1m', 'ACOS_7d', 'ACOS_30d', 'total_cost_7d', 'total_clicks_7d'
]
missing_columns = [col for col in required_columns if col not in data.columns]

# 输出结果
if missing_columns:
    print(f"Missing columns: {missing_columns}")
else:
    print("All required columns are present.")

# 输出数据的前几行以供检查
print(data.head())