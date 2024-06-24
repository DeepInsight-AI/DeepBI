# filename: find_skus.py

import pandas as pd
import os

# 更新后的文件路径
file_path = r'C:\Users\admin\Documents\预处理.csv'

# 检查文件是否存在
if not os.path.exists(file_path):
    print(f"文件未找到: {file_path}")
else:
    df = pd.read_csv(file_path)
    
    # 当前日期假设为2024年5月27日
    # 转化需要的列字段
    df['ACOS_7d'] = pd.to_numeric(df['ACOS_7d'], errors='coerce')
    df['ACOS_30d'] = pd.to_numeric(df['ACOS_30d'], errors='coerce')
    df['total_clicks_7d'] = pd.to_numeric(df['total_clicks_7d'], errors='coerce')
    df['total_clicks_30d'] = pd.to_numeric(df['total_clicks_30d'], errors='coerce')
    df['total_sales14d_7d'] = pd.to_numeric(df['total_sales14d_7d'], errors='coerce')
    df['total_sales14d_30d'] = pd.to_numeric(df['total_sales14d_30d'], errors='coerce')

    # 定义过滤条件
    condition1 = (df['total_clicks_7d'] > 10) & (df['ACOS_7d'] > 0.24)
    condition2 = (df['ACOS_30d'] > 0.24) & (df['total_sales14d_7d'] == 0) & (df['total_clicks_7d'] > 10)
    condition3 = (df['ACOS_7d'] > 0.24) & (df['ACOS_7d'] < 0.5) & (df['ACOS_30d'] > 0) & (df['ACOS_30d'] < 0.24) & (df['total_clicks_7d'] > 13)
    condition4 = (df['ACOS_7d'] > 0.24) & (df['ACOS_30d'] > 0.24)
    condition5 = df['ACOS_7d'] > 0.5
    condition6 = (df['total_clicks_30d'] > 13) & (df['total_sales14d_30d'] == 0)

    # 综合以上条件
    filtered_df = df[condition1 | condition2 | condition3 | condition4 | condition5 | condition6]

    # 选择所需的列
    columns_to_save = ['campaignName', 'adGroupName', 'ACOS_30d', 'ACOS_7d', 'total_clicks_7d', 'advertisedSku']
    filtered_df = filtered_df[columns_to_save]

    # 输出结果到新的CSV文件
    output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\SKU优化\提问策略\关闭SKU_FR.csv'
    filtered_df.to_csv(output_path, index=False)

    print("CSV文件已保存到：" + output_path)