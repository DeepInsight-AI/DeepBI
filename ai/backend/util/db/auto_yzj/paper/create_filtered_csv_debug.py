# filename: create_filtered_csv_debug.py

import pandas as pd

# 加载数据
data_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\搜索词优化\预处理.csv'
try:
    df = pd.read_csv(data_path)
    print("Data loaded successfully. Here are the column names and first 5 rows of the dataframe:")
    print(df.columns)
    print(df.head())
    
    # 筛选条件
    condition_sales = df['total_sales14d_7d'] > 0
    condition_acos = df['ACOS_7d'] < 0.2

    # 筛选数据
    filtered_df = df[condition_sales & condition_acos].copy()

    # 检查过滤后的数据框
    print("Filtered data contains the following rows (first 5 shown):")
    print(filtered_df.head())

    # 添加原因
    filtered_df['reason'] = '近七天有销售额且该搜索词的近七天acos值在0.2以下'

    # 选择需要的列并重命名
    result_df = filtered_df[['campaignName', 'adGroupName', 'ACOS_7d', 'searchTerm', 'reason']]
    result_df.columns = ['Campaign Name', 'adGroup', 'week_acos', 'searchTerm', 'reason']

    # 检查结果数据框
    print("Final result dataframe (first 5 rows):")
    print(result_df.head())

    # 保存结果
    output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\搜索词优化\提问策略\自动_优质搜索词_ES_2024-06-10.csv'
    result_df.to_csv(output_path, index=False)

    print("CSV file has been created and saved to:", output_path)
    
except FileNotFoundError:
    print("The file was not found. Please check the path.")
except pd.errors.EmptyDataError:
    print("The file is empty. Please check the file content.")
except Exception as e:
    print(f"An error occurred: {e}")