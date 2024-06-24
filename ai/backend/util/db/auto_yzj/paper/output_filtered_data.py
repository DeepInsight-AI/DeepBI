# filename: output_filtered_data.py
import pandas as pd

# 读取筛选后的数据
filtered_data = pd.read_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\搜索词优化\预处理.csv')

# 假设今天是2024年5月27日，计算近七天的日期范围
today = pd.to_datetime('2024-05-27')
start_date_7d = today - pd.DateOffset(days=7)

# 筛选出近七天有销售额且ACOS值小于0.2的搜索词
filtered_data = filtered_data[(filtered_data['total_sales14d_7d'] > 0) & (filtered_data['ACOS_7d'] < 0.2)]

# 准备输出数据
output_data = filtered_data[['campaignName', 'adGroupName', 'ACOS_7d', 'searchTerm', 'matchType']]
output_data.columns = ['广告活动', '广告组', '近七天的acos值', '搜索词', '匹配类型']
output_data['原因'] = '近七天有销售额且ACOS值小于0.2'

# 输出到CSV文件
output_data.to_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\搜索词优化\提问策略\优质搜索词_FR_2024-5-27_deepseek.csv', index=False)

# 打印输出数据
print(output_data)