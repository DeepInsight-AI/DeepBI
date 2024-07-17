# filename: analyze_sku.py

import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\SKU优化\预处理.csv'
df = pd.read_csv(file_path)

# 筛选符合条件的SKU
def find_reason(row):
    reason = ""
    if row['total_clicks_7d'] > 10 and row['ACOS_7d'] > 0.24:
        reason = "定义一"
    elif row['ACOS_30d'] > 0.24 and row['total_sales14d_7d'] == 0 and row['total_clicks_7d'] > 10:
        reason = "定义二"
    elif 0.24 < row['ACOS_7d'] < 0.5 and 0 < row['ACOS_30d'] < 0.24 and row['total_clicks_7d'] > 13:
        reason = "定义三"
    elif row['ACOS_7d'] > 0.24 and row['ACOS_30d'] > 0.24:
        reason = "定义四"
    elif row['ACOS_7d'] > 0.5:
        reason = "定义五"
    elif row['total_clicks_30d'] > 13 and row['total_sales14d_30d'] == 0:
        reason = "定义六"
    return reason

# 应用规则并添加列
df['关闭原因'] = df.apply(find_reason, axis=1)

# 筛选符合条件的数据
result_df = df[df['关闭原因'] != ""]

# 选择并重命名需要的列
result_df = result_df[['campaignName', 'adGroupName', 'ACOS_30d', 'ACOS_7d', 'total_clicks_7d', 'advertisedSku', '关闭原因']]
result_df.rename(columns={
    'campaignName': '广告活动',
    'adGroupName': '广告组',
    'ACOS_30d': '近30天的acos值',
    'ACOS_7d': '近7天的acos值',
    'total_clicks_7d': '近7天的点击数',
    'advertisedSku': 'sku',
    '关闭原因': '对sku进行关闭操作的原因',
}, inplace=True)

# 保存结果到新的CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\SKU优化\提问策略\自动_关闭SKU_v1_0_LAPASA_IT_2024-07-09.csv'
result_df.to_csv(output_file_path, index=False, encoding='utf-8-sig')

print("筛选结果已经保存到指定文件路径。")