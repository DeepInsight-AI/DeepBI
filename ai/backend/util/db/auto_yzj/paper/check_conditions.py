# filename: check_conditions.py
import pandas as pd

# Step 1: Load the dataset
dataset_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv"
df = pd.read_csv(dataset_path)

# Approach each condition with single-step checks
print("ACOS_7d > 0: ", df[df["ACOS_7d"] > 0].shape)
print("ACOS_7d <= 0.5: ", df[df["ACOS_7d"] <= 0.5].shape)
print("ACOS_30d > 0: ", df[df["ACOS_30d"] > 0].shape)
print("ACOS_30d <= 0.5: ", df[df["ACOS_30d"] <= 0.5].shape)
print("total_clicks_7d > 0: ", df[df["total_clicks_7d"] > 0].shape)
print("total_clicks_7d <= 10: ", df[df["total_clicks_7d"] <= 10].shape)
print("total_cost_7d > 0: ", df[df["total_cost_7d"] > 0].shape)
print("total_cost_7d <= 0.5: ", df[df["total_cost_7d"] <= 0.5].shape)