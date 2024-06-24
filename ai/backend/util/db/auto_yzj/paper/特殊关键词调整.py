# filename: 特殊关键词调整.py

import pandas as pd

# 1. 加载数据集
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\特殊关键词\预处理.csv'
df = pd.read_csv(file_path)

# 2. 找到符合条件的广告组和关键词
# 筛选广告组层面
ad_groups = df.groupby('adGroupName').filter(lambda x: x['total_sales_15d'].sum() == 0 and x['total_clicks_7d'].sum() <= 12)

# 对符合条件的关键词进行竞价调整
ad_groups['new_keywordBid'] = ad_groups['keywordBid'] + 0.02
ad_groups['reason'] = "广告组最近15天的总销售额为0并且广告组里的所有关键词的最近7天的总点击次数小于等于12"

# 3. 输出结果到CSV
output_columns = [
    'campaignName',
    'adGroupName',
    'total_sales_15d',
    'total_clicks_7d',
    'keyword',
    'matchType',
    'keywordBid',
    'keywordId',
    'new_keywordBid',
    'reason'
]

output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\特殊关键词\提问策略\手动_特殊关键词_v1_1_ES_2024-06-12.csv'
ad_groups.to_csv(output_path, columns=output_columns, index=False)

print("任务完成，结果已保存到：", output_path)