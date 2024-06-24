# filename: filter_bad_campaigns.py
import pandas as pd

# 读取CSV文件的位置
input_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\预算优化\预处理.csv'
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\预算优化\提问策略\自动_关闭的广告活动_ES_2024-06-10.csv'

# 读取CSV文件
df = pd.read_csv(input_file_path)

# 筛选符合条件的广告活动
bad_campaigns = df[(df['sales_1m'] == 0) & (df['clicks_1m'] >= 75)]

# 添加关闭原因列
bad_campaigns['close_reason'] = '最近一个月的总销售为0且总点击次数大于等于75'

# 需要导出的字段
output_columns = [
    'campaignName', 
    'Budget', 
    'clicks', 
    'ACOS', 
    'avg_ACOS_7d', 
    'avg_ACOS_1m', 
    'clicks_1m', 
    'sales_1m', 
    'close_reason'
]

# 导出结果到CSV文件
bad_campaigns[output_columns].to_csv(output_file_path, index=False)

print(f"劣质广告活动已成功保存到 {output_file_path}")