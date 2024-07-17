# filename: find_sku_leads.py
import pandas as pd

# 读取数据集
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\关闭SKU\预处理.csv'
df = pd.read_csv(file_path)

# 定义筛选条件
conditions = [
    (
        (df['ORDER_1m'] < 8) &
        (df['ACOS_7d'] > 0.24) &
        (df['total_cost_7d'] > 5),
        '定义一'
    ),
    (
        (df['ORDER_1m'] < 8) &
        (df['ACOS_30d'] > 0.24) &
        (df['total_sales_7d'] == 0) &
        (df['total_cost_7d'] > 5),
        '定义二'
    ),
    (
        (df['ORDER_1m'] < 8) &
        (df['ACOS_7d'].between(0.24, 0.5, inclusive='right')) &
        (df['ACOS_30d'].between(0, 0.24, inclusive='right')) &
        (df['total_cost_7d'] > 5),
        '定义三'
    ),
    (
        (df['ORDER_1m'] < 8) &
        (df['ACOS_7d'] > 0.24) &
        (df['ACOS_30d'] > 0.24),
        '定义四'
    ),
    (
        (df['ACOS_7d'] > 0.5),
        '定义五'
    ),
    (
        (df['total_cost_30d'] > 5) &
        (df['total_sales_30d'] == 0),
        '定义六'
    ),
    (
        (df['ORDER_1m'] < 8) &
        (df['total_cost_7d'] >= 5) &
        (df['total_sales_7d'] == 0),
        '定义七'
    ),
    (
        (df['ORDER_1m'] >= 8) &
        (df['total_cost_7d'] >= 10) &
        (df['total_sales_7d'] == 0),
        '定义八'
    )
]

# 筛选数据并添加满足的定义
result_df = pd.DataFrame()

for condition, definition in conditions:
    filtered = df[condition].copy()
    if not filtered.empty:
        filtered['满足的定义'] = definition
        result_df = pd.concat([result_df, filtered])

# 筛选需要的列
result_df = result_df[['campaignName', 'adId', 'adGroupName', 'ACOS_30d', 'ACOS_7d', 'total_clicks_7d', 'advertisedSku', 'ORDER_1m', '满足的定义']]

# 保存结果到CSV
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\关闭SKU\提问策略\SD_关闭SKU_v1_1_LAPASA_US_2024-07-16.csv'
result_df.to_csv(output_path, index=False)

print("筛选完成，结果已保存到：", output_path)