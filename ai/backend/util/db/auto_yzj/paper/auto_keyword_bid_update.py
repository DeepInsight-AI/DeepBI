# filename: auto_keyword_bid_update.py
import pandas as pd

# 读取数据集
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\滞销品优化\自动sp广告\自动定位组优化\预处理.csv'
data = pd.read_csv(file_path)

# 初始化新的竞价和提价原因的列
data['New Bid'] = data['keywordBid']
data['提价多少'] = 0
data['提价的原因'] = ''

# 定义的筛选规则
def update_bid(row):
    update_reason = ''
    increase_amount = 0

    if 0 < row['ACOS_7d'] < 0.27:
        if row['ACOS_30d'] > 0.5:
            increase_amount = 0.01
            update_reason = '定义一'
        elif 0.27 <= row['ACOS_30d'] <= 0.5:
            increase_amount = 0.02
            update_reason = '定义二'
        elif 0.1 < row['ACOS_7d'] < 0.27 and 0 < row['ACOS_30d'] < 0.27:
            increase_amount = 0.03
            update_reason = '定义三'
        elif 0 < row['ACOS_7d'] < 0.1 and 0 < row['ACOS_30d'] < 0.27:
            increase_amount = 0.05
            update_reason = '定义四'

    if increase_amount > 0:
        row['New Bid'] = row['keywordBid'] + increase_amount
        row['提价多少'] = increase_amount
        row['提价的原因'] = update_reason

    return row

# 应用筛选规则并更新数据
data = data.apply(update_bid, axis=1)

# 筛选并保存符合条件的关键词
result = data[data['提价的原因'] != '']

output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\滞销品优化\自动sp广告\自动定位组优化\提问策略\自动_优质自动定位组_v1_1_LAPASA_FR_2024-07-03.csv'
columns = [
    'campaignName',
    'adGroupName',
    'keyword',
    'keywordBid',
    'New Bid',
    'ACOS_30d',
    'ACOS_7d',
    'total_clicks_7d',
    '提价多少',
    '提价的原因'
]
result.to_csv(output_path, columns=columns, index=False)

print(f"结果已保存到 {output_path}")