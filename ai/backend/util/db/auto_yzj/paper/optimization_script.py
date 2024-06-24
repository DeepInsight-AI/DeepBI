# filename: optimization_script.py
import pandas as pd

# 文件路径
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\预处理.csv'
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\提问策略\关闭的广告活动_FR.csv'

# 读取CSV文件
data = pd.read_csv(file_path)

# 筛选条件
condition = (data['sales_1m'] == 0) & (data['clicks_1m'] >= 75)
bad_campaigns = data[condition]

# 添加关闭原因
bad_campaigns['关闭原因'] = '最近一个月的总sales为0且最近一个月的总点击次数大于等于75'

# 选择需要的列
output_columns = [
    'date', 'campaignName', 'Budget', 'clicks', 'ACOS', 
    'avg_ACOS_7d', 'avg_ACOS_1m', 'clicks_1m', 'sales_1m', '关闭原因'
]

# 保存到CSV文件
bad_campaigns.to_csv(output_path, index=False, columns=output_columns)

print("处理完成，结果已保存到指定路径。")