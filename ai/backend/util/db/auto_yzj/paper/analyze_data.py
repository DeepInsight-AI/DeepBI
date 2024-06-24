# filename: analyze_data.py

import pandas as pd

# Step 1: Read the CSV file
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\预处理.csv'
data = pd.read_csv(file_path)

# Step 2: Display basic statistics
print("Basic Statistics:")
print(data.describe())

# Step 3: Display some example records
print("\nExample Records:")
print(data.head(10))

# Step 4: Display unique campaign names
print("\nUnique Campaign Names:")
print(data['campaignName'].unique())