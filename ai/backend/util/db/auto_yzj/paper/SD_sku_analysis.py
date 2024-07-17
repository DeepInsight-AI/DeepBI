# filename: SD_sku_analysis.py
import pandas as pd

# Load the CSV file
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\关闭SKU\预处理.csv'
df = pd.read_csv(file_path)

# Define the conditions for each SKU based on the provided definitions
conditions = [
    # Definition 1
    (df['ORDER_1m'] < 8) & (df['ACOS_7d'] > 0.24) & (df['total_cost_7d'] > 5),
    
    # Definition 2
    (df['ORDER_1m'] < 8) & (df['ACOS_30d'] > 0.24) & (df['total_sales_7d'] == 0) & (df['total_cost_7d'] > 5),
    
    # Definition 3
    (df['ORDER_1m'] < 8) & (df['ACOS_7d'].between(0.24, 0.5)) & (df['ACOS_30d'].between(0, 0.24)) & (df['total_cost_7d'] > 5),
    
    # Definition 4
    (df['ORDER_1m'] < 8) & (df['ACOS_7d'] > 0.24) & (df['ACOS_30d'] > 0.24),
    
    # Definition 5
    df['ACOS_7d'] > 0.5,
    
    # Definition 6
    (df['total_cost_30d'] > 5) & (df['total_sales_30d'] == 0),
    
    # Definition 7
    (df['ORDER_1m'] < 8) & (df['total_cost_7d'] >= 5) & (df['total_sales_7d'] == 0),
    
    # Definition 8
    (df['ORDER_1m'] >= 8) & (df['total_cost_7d'] >= 10) & (df['total_sales_7d'] == 0)
]

# Combine conditions using bitwise OR to filter SKUs that satisfy any definition
final_condition = conditions[0]
for condition in conditions[1:]:
    final_condition |= condition

# Filter the DataFrame based on the combined condition
filtered_df = df[final_condition]

# Prepare the output DataFrame with relevant columns
output_df = filtered_df[['campaignName', 'adId', 'adGroupName', 'ACOS_30d', 'ACOS_7d', 'total_clicks_7d', 'advertisedSku', 'ORDER_1m']].copy()

# Add a column to indicate which definitions were satisfied
output_df['definitions'] = ""

# Populate the definitions column
for i, df_condition in enumerate(conditions, 1):
    output_df.loc[df_condition, 'definitions'] += f"Definition {i} "

# Save the result to the specified path
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\关闭SKU\提问策略\SD_关闭SKU_v1_1_LAPASA_UK_2024-07-14.csv'
output_df.to_csv(output_file_path, index=False)

print("CSV file has been successfully saved.")