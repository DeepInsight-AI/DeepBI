# filename: close_auto_positioning_group.py

import pandas as pd

# Read the CSV file
file_path = 'C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\自动sp广告\\自动定位组优化\\预处理.csv'
data = pd.read_csv(file_path)

# Define conditions

# Definition 1: 最近7天没有销售额并且近七天总点击数大于0，并且最近一个月也没有销售额，并且近一个月点击数大于10
condition1 = (data['total_sales14d_7d'] == 0) & (data['total_clicks_7d'] > 0) & \
             (data['total_sales14d_30d'] == 0) & (data['total_clicks_30d'] > 10)

# Definition 2: 最近7天没有销售额并且近七天总点击数大于0，并且最近一个月的acos值大于0.5
condition2 = (data['total_sales14d_7d'] == 0) & (data['total_clicks_7d'] > 0) & \
             (data['ACOS_30d'] > 0.5)

# Definition 3: 最近7天acos值大于0.5，最近30天acos值大于0.24
condition3 = (data['ACOS_7d'] > 0.5) & (data['ACOS_30d'] > 0.24)

# Add reasons for closing
data['reason'] = ''
data.loc[condition1, 'reason'] = 'Definition 1'
data.loc[condition2, 'reason'] = 'Definition 2'
data.loc[condition3, 'reason'] = 'Definition 3'

# Filter the data based on conditions
filtered_data = data[data['reason'] != '']

# Select necessary columns
output_data = filtered_data[['campaignName', 'adGroupName', 'keyword', 'ACOS_30d', 'ACOS_7d', 'reason']]

# Save the filtered data to a new CSV file
output_file_path = 'C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\自动sp广告\\自动定位组优化\\提问策略\\关闭自动定位组_ES_2024-6-02.csv'
output_data.to_csv(output_file_path, index=False)

print(f"Filtered data saved to {output_file_path}")