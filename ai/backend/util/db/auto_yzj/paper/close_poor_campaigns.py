# filename: close_poor_campaigns.py
import pandas as pd
from datetime import datetime, timedelta

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\预处理.csv'
df = pd.read_csv(file_path)

# 计算昨天的日期
today = datetime(2024, 5, 28)
yesterday = today - timedelta(days=1)
yesterday_str = yesterday.strftime('%Y-%m-%d')

# 筛选符合条件的广告活动
criteria1 = df['sales_1m'] == 0
criteria2 = df['clicks_1m'] >= 75

poor_campaigns = df[criteria1 & criteria2]

# 添加“关闭原因”列
poor_campaigns['关闭原因'] = '最近一个月的总sales为0，最近一个月的总点击次数大于等于75'

# 选择需要的列
output_columns = [
    'date', 'campaignName', 'Budget', 'clicks', 'ACOS', 
    'avg_ACOS_7d', 'avg_ACOS_1m', 'clicks_1m', 'sales_1m', '关闭原因'
]

result = poor_campaigns[output_columns]

# 将结果保存到新的CSV文件中
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\提问策略\手动_关闭的广告活动_IT_2024-06-08.csv'
result.to_csv(output_file_path, index=False)

print(f"操作完成。结果保存在 {output_file_path}")