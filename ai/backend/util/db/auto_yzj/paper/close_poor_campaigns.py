# filename: close_poor_campaigns.py

import pandas as pd
from datetime import date, timedelta

# 读取数据文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\预处理.csv'
data = pd.read_csv(file_path)

# 设置今天的日期
today = date(2024, 5, 28)
yesterday = today - timedelta(days=1)

# 筛选出符合条件的广告活动
poor_campaigns = data[
    (data['total_sales14d_30d'] == 0) &
    (data['total_clicks_30d'] >= 75)
]

# 添加关闭原因列
poor_campaigns['关闭原因'] = '最近一个月总销售为0且最近一个月总点击次数>=75'

# 创建用于输出的列
export_data = poor_campaigns[[
    'campaignName', 'Budget', 'total_clicks_30d', 'ACOS_7d', 
    'country_avg_ACOS_1m', 'total_clicks_30d', 'total_sales14d_30d', '关闭原因'
]]
# 重命名列标题
export_data = export_data.rename(columns={
    'campaignName': 'campaignName',
    'Budget': 'Budget',
    'total_clicks_30d': '最近一个月的总点击数',
    'ACOS_7d': '最近7天的平均ACOS值',
    'country_avg_ACOS_1m': '最近一个月的平均ACOS值',
    'total_sales14d_30d': '最近一个月的总销售',
    '关闭原因': '关闭原因'
})

# 插入日期列
export_data.insert(0, 'date', yesterday)

# 定义输出文件路径
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\提问策略\手动_关闭的广告活动_v1_0_LAPASA_IT_2024-07-09.csv'
# 导出数据到CSV文件
export_data.to_csv(output_file_path, index=False, encoding='utf-8-sig')

print("过滤并导出完成")