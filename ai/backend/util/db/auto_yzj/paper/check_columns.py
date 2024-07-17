# filename: check_columns.py
import pandas as pd

# Load the dataset
dataset_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv"
df = pd.read_csv(dataset_path)

# Print the column names
print("Columns in the CSV file:")
print(df.columns)