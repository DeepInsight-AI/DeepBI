# filename: keyword_performance_analysis.py

import pandas as pd

# 定义文件路径
input_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\特殊关键词\预处理.csv'
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\特殊关键词\提问策略\特殊关键词_FR.csv'

# 读取CSV文件
df = pd.read_csv(input_file_path)

# 筛选表现较差的关键词
poor_keywords = df[(df['total_clicks'] <= 12) & (df['total_sales'] == 0)]

# 提高竞价0.02
poor_keywords['keywordBid'] = poor_keywords['keywordBid'] + 0.02

# 添加竞价操作原因
poor_keywords['原因'] = '总点击次数小于等于12, 总销售额为0, 增加竞价0.02'

# 选择需要导出的列
output_columns = ['campaignName', 'adGroupName', 'keyword', 'keywordId', 'targeting', 'matchtype', 'total_clicks', 'total_sales', 'keywordBid', '原因']

# 输出结果到CSV文件
poor_keywords[output_columns].to_csv(output_file_path, index=False)

print(f"处理完成，结果已保存至{output_file_path}")

# 终止任务的标识
print("TERMINATE")