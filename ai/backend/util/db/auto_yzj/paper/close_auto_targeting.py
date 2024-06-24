# filename: close_auto_targeting.py
import pandas as pd

# 读取csv文件
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\自动定位组优化\预处理.csv'
df = pd.read_csv(file_path)

# 定义一
condition_1 = (
    (df['total_sales14d_7d'] == 0) & 
    (df['total_clicks_7d'] > 0) & 
    (df['total_sales14d_30d'] == 0) & 
    (df['total_clicks_30d'] > 10)
)

# 定义二
condition_2 = (
    (df['total_sales14d_7d'] == 0) & 
    (df['total_clicks_7d'] > 0) & 
    (df['ACOS_30d'] > 0.5)
)

# 定义三
condition_3 = (
    (df['ACOS_7d'] > 0.5) & 
    (df['ACOS_30d'] > 0.24)
)

# 筛选满足条件的行
df_filtered_1 = df[condition_1].copy()
df_filtered_1['原因'] = '定义一'

df_filtered_2 = df[condition_2].copy()
df_filtered_2['原因'] = '定义二'

df_filtered_3 = df[condition_3].copy()
df_filtered_3['原因'] = '定义三'

# 合并所有满足条件的数据
df_final = pd.concat([df_filtered_1, df_filtered_2, df_filtered_3])

# 选择所需的字段
df_final = df_final[['campaignName', 'adGroupName', 'keyword', 'ACOS_30d', 'ACOS_7d', '原因']]

# 保存结果到新的CSV文件
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\自动定位组优化\提问策略\关闭自动定位组_FR.csv'
df_final.to_csv(output_path, index=False)

print("任务完成，结果已保存在:", output_path)