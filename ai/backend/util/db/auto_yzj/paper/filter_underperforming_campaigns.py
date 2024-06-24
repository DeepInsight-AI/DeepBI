# filename: filter_underperforming_campaigns.py

import pandas as pd
from datetime import datetime, timedelta

# 读取CSV文件
input_file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\预算优化\预处理.csv"
data = pd.read_csv(input_file_path)

# 假设今天是2024年5月28日
today = datetime(2024, 5, 28)
yesterday = today - timedelta(days=1)

# 定义关闭广告活动的条件
underperforming_campaigns = data[
    (data['sales_1m'] == 0) &
    (data['clicks_1m'] >= 75)
]

# 增加关闭原因字段
underperforming_campaigns['关闭原因'] = '最近一个月总销售为0，且总点击次数>=75'

# 选择所需字段
output_data = underperforming_campaigns[[
    'campaignName',
    'Budget',
    'clicks',
    'ACOS',
    'avg_ACOS_7d',
    'avg_ACOS_1m',
    'clicks_1m',
    'sales_1m',
    '关闭原因'
]]

# 输出到CSV文件
output_file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\预算优化\提问策略\关闭的广告活动_ES_2024-6-02.csv"
output_data.to_csv(output_file_path, index=False, encoding='utf-8-sig')

print("数据处理完成，结果已保存到:", output_file_path)