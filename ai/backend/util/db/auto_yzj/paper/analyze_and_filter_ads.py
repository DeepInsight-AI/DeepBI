# filename: analyze_and_filter_ads.py
import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\预处理.csv'
data = pd.read_csv(file_path)

# 当前日期设定为2024年5月28日
current_date = pd.to_datetime('2024-05-28')

# 定义过滤条件
condition = (data['sales_1m'] == 0) & (data['clicks_1m'] >= 75)

# 筛选满足条件的广告活动
filtered_data = data[condition]

# 添加关闭原因字段
filtered_data['关闭原因'] = '最近一个月的总sales为0且总点击次数大于等于75'

# 提取需要的字段
output_columns = [
    'date',
    'campaignName',
    'Budget',
    'clicks',
    'ACOS',
    'avg_ACOS_7d',
    'avg_ACOS_1m',
    'clicks_1m',
    'sales_1m',
    '关闭原因'
]

# 生成输出数据
output_data = filtered_data[output_columns]

# 保存为新的CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\提问策略\手动_关闭的广告活动_IT_2024-06-06.csv'
output_data.to_csv(output_file_path, index=False)

print(f"Filtered data has been saved to {output_file_path}")