# filename: filter_ad_data.py

import pandas as pd

# 读取csv文件
data_path = "C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\自动sp广告\\搜索词优化\\预处理.csv"
df = pd.read_csv(data_path)

# 筛选定义一条件的数据
filtered_df = df[(df['total_sales14d_7d'] > 0) & (df['ACOS_7d'] < 0.2)]

# 添加原因列
filtered_df['reason'] = '近七天的ACoS值在0.2以下且有销售额'

# 选择需要的列
result_df = filtered_df[['campaignName', 'adGroupName', 'ACOS_7d', 'searchTerm', 'reason']]

# 保存结果到新的csv文件
output_path = "C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\自动sp广告\\搜索词优化\\提问策略\\优质搜索词_FR.csv"
result_df.to_csv(output_path, index=False, encoding='utf-8-sig')

print(f"筛选结果已保存到 {output_path}")