# filename: process_poorly_performing_keywords.py
import pandas as pd

# 读取数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\特殊关键词\预处理.csv'
data = pd.read_csv(file_path)

# 设置竞价提高值
bid_increase = 0.02

# 筛选表现较差的关键词
poor_performing_keywords = data[(data['total_clicks'] <= 12) & (data['total_sales'] == 0)]

# 提高竞价
poor_performing_keywords['keywordBid'] += bid_increase

# 增加操作原因列
poor_performing_keywords['操作竞价原因'] = '总点击次数小于等于12，总销售额为0，竞价提高0.02'

# 筛选所需要的列
columns_needed = ['campaignName', 'adGroupName', 'keyword', 'keywordId', 'targeting', 'matchtype', 'total_clicks', 'total_sales', 'keywordBid', '操作竞价原因']
result = poor_performing_keywords[columns_needed]

# 保存结果到新的CSV文件
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\特殊关键词\提问策略\手动_特殊关键词_ES_2024-06-05.csv'
result.to_csv(output_path, index=False)

print("处理完成，结果已保存到：", output_path)