# filename: optimize_keywords.py

import pandas as pd

# 读取数据集
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\自动定位组优化\预处理.csv'
data = pd.read_csv(file_path)

# 定义调整竞价的条件并执行提价操作
conditions = [
    {
        'condition': (data['ACOS_7d'] > 0) & (data['ACOS_7d'] < 0.24) & (data['ACOS_30d'] > 0.5),
        'increment': 0.01,
        'reason': '定义一：7天ACOS在 0 与 0.24 之间且 30天ACOS 大于 0.5'
    },
    {
        'condition': (data['ACOS_7d'] > 0) & (data['ACOS_7d'] < 0.24) & (data['ACOS_30d'] > 0.5),  # Same as 条件一
        'increment': 0.02,
        'reason': '定义二：7天ACOS在 0 与 0.24 之间且30天ACOS大于 0.5'
    },
    {
        'condition': (data['ACOS_7d'] > 0.1) & (data['ACOS_7d'] < 0.24) & (data['ACOS_30d'] > 0) & (data['ACOS_30d'] < 0.24),
        'increment': 0.03,
        'reason': '定义三：7天ACOS在 0.1 与 0.24 之间且30天ACOS在 0 与 0.24 之间'
    },
    {
        'condition': (data['ACOS_7d'] > 0) & (data['ACOS_7d'] < 0.1) & (data['ACOS_30d'] > 0) & (data['ACOS_30d'] < 0.24),
        'increment': 0.05,
        'reason': '定义四：7天ACOS在 0 与 0.1 之间且30天ACOS在 0 与 0.24 之间'
    }
]

results = []

for condition in conditions:
    filtered_data = data[condition['condition']]
    for _, row in filtered_data.iterrows():
        new_bid = row['keywordBid'] + condition['increment']
        results.append([
            row['campaignName'],
            row['adGroupName'],
            row['keyword'],
            row['keywordBid'],
            new_bid,
            row['ACOS_30d'],
            row['ACOS_7d'],
            row['total_clicks_7d'],
            condition['increment'],
            condition['reason']
        ])

# 创建 DataFrame 存储结果
columns = [
    'campaignName',
    'adGroupName',
    'keyword',
    'keywordBid',
    'New_keywordBid',
    'ACOS_30d',
    'ACOS_7d',
    'clicks_7d',
    'increment',
    'reason'
]

results_df = pd.DataFrame(results, columns=columns)

# 将结果保存到CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\自动定位组优化\提问策略\自动_优质自动定位组_v1_1_ES_2024-06-24.csv'
results_df.to_csv(output_file_path, index=False, encoding='utf-8-sig')

print(f"Results have been saved to {output_file_path}")