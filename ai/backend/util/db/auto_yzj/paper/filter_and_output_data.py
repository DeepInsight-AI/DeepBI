# filename: filter_and_output_data.py
import pandas as pd

# 读取CSV文件
data = pd.read_csv(r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\预处理.csv')

# 筛选出2024年5月26日的数据
yesterday_data = data[data['total_clicks_yesterday'] == data['total_clicks_yesterday'].max()]

# 定义筛选条件
def filter_ads(df):
    # 定义一
    df_def1 = df[(df['total_sales14d_7d'] == 0) & (df['total_clicks_7d'] > 0)]
    # 定义二
    df_def2 = df[
        (df['ACOS_7d'] > 0.24) & (df['ACOS_7d'] < 0.5) &
        ((df['ACOS_7d'].max() - df['ACOS_7d'].min()) >= 0.2)
    ]
    # 定义三
    df_def3 = df[df['ACOS_7d'] >= 0.5]
    
    # 合并所有筛选结果
    result = pd.concat([df_def1, df_def2, df_def3])
    
    return result

# 应用筛选条件
filtered_data = filter_ads(yesterday_data)

# 输出结果到CSV文件
output_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\广告位优化\提问策略\劣质广告位_FR_2024-5-27_deepseek.csv'
filtered_data.to_csv(output_file, index=False)

# 打印筛选结果
print(filtered_data)