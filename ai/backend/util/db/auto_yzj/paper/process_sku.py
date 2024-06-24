# filename: process_sku.py

import pandas as pd

# 读取数据集
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\SKU优化\预处理.csv'
df = pd.read_csv(file_path)

# 选择满足条件的SKU闭操作的原因
def determine_reason(row):
    # 定义默认原因为空
    reason = ''
    # 定义一
    if row['total_clicks_7d'] > 10 and row['ACOS_7d'] > 0.24:
        reason = 'def1'
    # 定义二
    elif row['ACOS_30d'] > 0.24 and row['total_sales14d_7d'] == 0 and row['total_clicks_7d'] > 10:
        reason = 'def2'
    # 定义三
    elif 0.24 < row['ACOS_7d'] < 0.5 and 0 < row['ACOS_30d'] < 0.24 and row['total_clicks_7d'] > 13:
        reason = 'def3'
    # 定义四
    elif row['ACOS_7d'] > 0.24 and row['ACOS_30d'] > 0.24:
        reason = 'def4'
    # 定义五
    elif row['ACOS_7d'] > 0.5:
        reason = 'def5'
    # 定义六
    elif row['total_clicks_30d'] > 13 and row['total_sales14d_30d'] == 0:
        reason = 'def6'
    return reason

# 筛选符合条件的行并添加原因列
df['reason'] = df.apply(determine_reason, axis=1)

# 过滤掉没有原因的行
filtered_df = df[df['reason'] != '']

# 选择需要的列
output_columns = [
    'campaignName',
    'adGroupName',
    'ACOS_30d',
    'ACOS_7d',
    'total_clicks_7d',
    'advertisedSku',
    'reason'
]
output_df = filtered_df[output_columns]

# 保存结果到新的CSV文件
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\SKU优化\提问策略\关闭SKU_ES_2024-6-02.csv'
output_df.to_csv(output_path, index=False)

print(f"结果已保存到 {output_path}")