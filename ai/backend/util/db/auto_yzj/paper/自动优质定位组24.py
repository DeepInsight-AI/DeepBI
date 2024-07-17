# filename: update_keyword_bids.py
import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\自动定位组优化\预处理.csv'
data = pd.read_csv(file_path)

# 定义提价规则
def adjust_bid(row):
    if 0 < row['ACOS_7d'] < 0.24 and row['ACOS_30d'] > 0.5:
        return 0.01, '定义一'
    elif 0 < row['ACOS_7d'] < 0.24 and row['ACOS_30d'] > 0.5:
        return 0.02, '定义二'
    elif 0.1 < row['ACOS_7d'] < 0.24 and 0 < row['ACOS_30d'] < 0.24:
        return 0.03, '定义三'
    elif 0 < row['ACOS_7d'] < 0.1 and 0 < row['ACOS_30d'] < 0.24:
        return 0.05, '定义四'
    else:
        return 0, '不满足条件'

# 应用规则调整竞价
data['bid_adjustment'], data['adjustment_reason'] = zip(*data.apply(adjust_bid, axis=1))
data['New_keywordBid'] = data['keywordBid'] + data['bid_adjustment']

# 筛选出满足条件的行
result = data[data['bid_adjustment'] > 0].copy()

# 准备输出的数据
output_data = result[['campaignName', 'adGroupName', 'keyword', 'keywordBid', 'New_keywordBid', 'ACOS_30d', 'ACOS_7d', 'total_clicks_7d', 'bid_adjustment', 'adjustment_reason']]

# 输出结果到CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\自动定位组优化\提问策略\自动_优质自动定位组_v1_1_IT_2024-06-24.csv'
output_data.to_csv(output_file_path, index=False)

print('The CSV file has been successfully created at', output_file_path)