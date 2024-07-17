# filename: process_sku_data.py
import pandas as pd

# 读取CSV文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\关闭SKU\预处理.csv'
df = pd.read_csv(file_path, encoding='utf-8')

# 定义筛选条件函数
def filter_sku(df):
    conditions = [
        # 定义一
        (df['ORDER_1m'] < 8) & (df['ACOS_7d'] > 0.24) & (df['total_cost_7d'] > 5),
        # 定义二
        (df['ORDER_1m'] < 8) & (df['ACOS_30d'] > 0.24) & (df['total_sales_7d'] == 0) & (df['total_cost_7d'] > 5),
        # 定义三
        (df['ORDER_1m'] < 8) & (df['ACOS_7d'] > 0.24) & (df['ACOS_7d'] < 0.5) & (df['ACOS_30d'] > 0) & (df['ACOS_30d'] < 0.24) & (df['total_cost_7d'] > 5),
        # 定义四
        (df['ORDER_1m'] < 8) & (df['ACOS_7d'] > 0.24) & (df['ACOS_30d'] > 0.24),
        # 定义五
        (df['ACOS_7d'] > 0.5),
        # 定义六
        (df['total_cost_30d'] > 5) & (df['total_sales_30d'] == 0),
        # 定义七
        (df['ORDER_1m'] < 8) & (df['total_cost_7d'] >= 5) & (df['total_sales_7d'] == 0),
        # 定义八
        (df['ORDER_1m'] >= 8) & (df['total_cost_7d'] >= 10) & (df['total_sales_7d'] == 0)
    ]

    # 初始化一个空DataFrame来存储满足条件的记录
    filtered_df = pd.DataFrame()

    # 遍历每个定义条件，并筛选符合条件的行
    for i, condition in enumerate(conditions):
        filtered = df[condition].copy()
        filtered['Definition'] = f'定义{i+1}'
        filtered_df = pd.concat([filtered_df, filtered], axis=0)

    return filtered_df

# 筛选数据
result_df = filter_sku(df)

# 提取所需字段
output_df = result_df[['campaignName', 'adId', 'adGroupName', 'ACOS_30d', 'ACOS_7d', 'total_clicks_7d', 'advertisedSku', 'ORDER_1m', 'Definition']]

# 保存结果
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\关闭SKU\提问策略\SD_关闭SKU_v1_1_LAPASA_FR_2024-07-16.csv'
output_df.to_csv(output_file_path, index=False, encoding='utf-8')

print(f'结果已保存到: {output_file_path}')