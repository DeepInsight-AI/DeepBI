# filename: process_bid_adjustment.py

import pandas as pd

# 加载数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\自动定位组优化\预处理.csv'
df = pd.read_csv(file_path)

# 筛选和提价
def get_adjustment_reason(row):
    if 0 < row['ACOS_7d'] < 0.24 and row['ACOS_30d'] > 0.5:
        return 0.01, '定义一'
    elif 0 < row['ACOS_7d'] < 0.24 and row['ACOS_30d'] > 0.5:
        return 0.02, '定义二'
    elif 0.1 < row['ACOS_7d'] < 0.24 and 0 < row['ACOS_30d'] < 0.24:
        return 0.03, '定义三'
    elif 0 < row['ACOS_7d'] < 0.1 and 0 < row['ACOS_30d'] < 0.24:
        return 0.05, '定义四'
    else:
        return (None, '')

# Apply adjustment
adjustments = df.apply(lambda row: get_adjustment_reason(row), axis=1)

# Ensure every row returns two values by filling NaNs with default
valid_adjustments = [(0 if isnan(x[0]) else x[0], '' if isnan(x[1]) else x[1]) for x in adjustments if isinstance(x, tuple)]

# Ensure every tuple has two elements
valid_adjustments = [(x[0], x[1]) if isinstance(x, tuple) and len(x) == 2 else (None, '') for x in valid_adjustments]

# Unpack the adjustments into new values
if valid_adjustments:
    df['提价'], df['提价的原因'] = zip(*valid_adjustments)
else:
    df['提价'], df['提价的原因'] = None, ''

# Filtering out rows without adjustments
df_filtered = df.dropna(subset=['提价'])

# 结果保存
result_columns = ['campaignName', 'adGroupName', 'keyword', 'ACOS_30d', 'ACOS_7d', 'total_clicks_7d', '提价的原因']
df_result = df_filtered[result_columns]

output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\自动定位组优化\提问策略\优质自动定位组_ES_2024-6-04.csv'
df_result.to_csv(output_path, index=False)

print(f"处理结果已保存至: {output_path}")