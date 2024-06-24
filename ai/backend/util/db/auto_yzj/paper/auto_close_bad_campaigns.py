# filename: auto_close_bad_campaigns.py
import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\预算优化\预处理.csv'
data = pd.read_csv(file_path)

# 筛选最近一个月总销售为0且最近一个月总点击次数大于等于75的广告活动
bad_campaigns = data[(data['sales_1m'] == 0) & (data['clicks_1m'] >= 75)]

# 为每个符合条件的广告活动添加关闭原因
bad_campaigns['关闭原因'] = '最近一个月的总销售为0且最近一个月的总点击次数大于等于75'

# 选择需要输出的字段
output_columns = [
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
output_data = bad_campaigns[output_columns]

# 输出到新的CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\预算优化\提问策略\自动_关闭的广告活动_IT_2024-06-08.csv'
output_data.to_csv(output_file_path, index=False)

print(f"结果已保存到 {output_file_path}")