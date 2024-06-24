# filename: special_keywords_analysis.py

import pandas as pd

# 读取原始数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\特殊关键词\预处理.csv'
data = pd.read_csv(file_path)

# 过滤符合条件的关键词：总点击次数 <= 12 且 总销售额 == 0
filtered_data = data[(data['total_clicks'] <= 12) & (data['total_sales'] == 0)]

# 增加一个新列，标识需要增加竞价的原因
filtered_data['increase_bid_reason'] = '总点击次数 <= 12 且 总销售额 == 0，提高竞价0.02'

# 选择输出的列
result_data = filtered_data[['campaignName', 'adGroupName', 'keyword', 'keywordId', 'targeting', 'matchtype', 'total_clicks', 'total_sales', 'keywordBid', 'increase_bid_reason']]

# 输出结果到CSV文件
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\特殊关键词\提问策略\手动_特殊关键词_IT_2024-06-06.csv'
result_data.to_csv(output_path, index=False)

# 打印成功消息
print(f"筛选后的数据已成功保存到 {output_path}")