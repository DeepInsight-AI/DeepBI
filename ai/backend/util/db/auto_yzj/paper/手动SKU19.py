# filename: filter_skus.py
import pandas as pd

# 读取CSV数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\SKU优化\预处理.csv'
data = pd.read_csv(file_path)

# 筛选符合定义条件的SKU
def filter_skus(df):
    conditions = [
        ("定义一", (df['ORDER_1m'] < 8) & (df['ACOS_7d'] > 0.24) & (df['total_clicks_7d'] > 13)),
        ("定义二", (df['ORDER_1m'] < 8) & (df['ACOS_30d'] > 0.24) & (df['total_sales14d_7d'] == 0) & (df['total_clicks_7d'] > 13)),
        ("定义三", (df['ORDER_1m'] < 8) & (df['ACOS_7d'] > 0.24) & (df['ACOS_7d'] < 0.5) & (df['ACOS_30d'] > 0) & (df['ACOS_30d'] < 0.24) & (df['total_clicks_7d'] > 13)),
        ("定义四", (df['ORDER_1m'] < 8) & (df['ACOS_7d'] > 0.24) & (df['ACOS_30d'] > 0.24)),
        ("定义五", (df['ACOS_7d'] > 0.5)),
        ("定义六", (df['total_clicks_30d'] > 13) & (df['total_sales14d_30d'] == 0)),
        ("定义七", (df['total_clicks_7d'] >= 19) & (df['total_sales14d_7d'] == 0))
    ]

    filtered_df = pd.DataFrame()
    
    for name, condition in conditions:
        current_df = df[condition].copy()
        if not current_df.empty:
            current_df['满足的定义'] = name
            filtered_df = pd.concat([filtered_df, current_df])

    return filtered_df

filtered_data = filter_skus(data)

# 选择并重命名需要的列
result = filtered_data[[
    'campaignName', 'adId', 'adGroupName', 'ACOS_30d', 'ACOS_7d', 
    'total_clicks_7d', 'advertisedSku', 'ORDER_1m', '满足的定义'
]]

# 保存结果到新的CSV文件
output_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\SKU优化\提问策略\手动_关闭sku_v1_1_IT_2024-06-19.csv'
result.to_csv(output_file, index=False, encoding='utf-8-sig')

print("数据处理完成，结果已保存到:", output_file)