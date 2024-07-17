# filename: 提问策略_调整竞价.py

import pandas as pd

# 读取数据
file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\特殊商品投放\预处理.csv"
data = pd.read_csv(file_path)

# 筛选出广告组的最近15天总销售额为0的商品投放
zero_sales_groups = data[data['total_sales_15d'] == 0]

# 使用groupby和filter找到广告组中所有商品投放的7天总点击次数都<=12 的广告组
def all_clicks_lessthan12(group):
    return (group['total_clicks_7d'] <= 12).all()

filtered_groups = zero_sales_groups.groupby('adGroupName').filter(all_clicks_lessthan12)

# 对符合条件的商品投放增加竞价0.02
filtered_groups['New Bid'] = filtered_groups['keywordBid'] + 0.02
filtered_groups['操作竞价原因'] = '广告组的最近15天总销售额为0，且所有商品投放的最近7天的总点击次数<=12'

# 选择输出列
output_columns = [
    'campaignName',
    'adGroupName',
    'total_sales_15d',
    'total_clicks_7d',
    'keyword',
    'matchType',
    'keywordBid',
    'keywordId',
    'New Bid',
    '操作竞价原因'
]

output_data = filtered_groups[output_columns]

# 保存结果到新的CSV文件
output_file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\特殊商品投放\提问策略\手动_ASIN_特殊商品投放_v1_1_DE_2024-06-30.csv"
output_data.to_csv(output_file_path, index=False)

print("处理完成，文件已保存到: ", output_file_path)