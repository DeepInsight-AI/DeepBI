# filename: filter_keywords_for_closure.py
import pandas as pd

# Step 1: Load the data from the CSV file
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\自动定位组优化\预处理.csv'
data = pd.read_csv(file_path)

# Step 2: Determine keywords to close based on the three definitions

# Conditions for Definition One
condition_def1 = (data['total_sales14d_7d'] == 0) & (data['total_clicks_7d'] > 0) & \
                 (data['total_sales14d_30d'] == 0) & (data['total_clicks_30d'] > 10)

# Conditions for Definition Two
condition_def2 = (data['total_sales14d_7d'] == 0) & (data['total_clicks_7d'] > 0) & \
                 (data['ACOS_30d'] > 0.5)

# Conditions for Definition Three
condition_def3 = (data['ACOS_7d'] > 0.5) & (data['ACOS_30d'] > 0.24)

# Overall condition combining all definitions using logical OR
final_condition = condition_def1 | condition_def2 | condition_def3

# Filter the data based on the combined condition
filtered_data = data[final_condition].copy()

# Determine the reason for each keyword
filtered_data['原因'] = ''
filtered_data.loc[condition_def1, '原因'] = '定义一'
filtered_data.loc[condition_def2, '原因'] = '定义二'
filtered_data.loc[condition_def3, '原因'] = '定义三'

# Selecting necessary columns
result = filtered_data[['campaignName', 'adGroupName', 'keyword', 'ACOS_30d', 'ACOS_7d', '原因']]

# Step 3: Save the result to a new CSV file
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\自动定位组优化\提问策略\自动_关闭自动定位组_IT_2024-06-06.csv'
result.to_csv(output_file_path, index=False)

print("Filtered data has been saved to:", output_file_path)