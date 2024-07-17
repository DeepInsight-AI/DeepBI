# filename: filter_sku_defined.py
import pandas as pd

# 读取CSV文件
file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\关闭SKU\预处理.csv"
df = pd.read_csv(file_path)

# 将满足定义的结果保存在一个新的DataFrame中
result_df = pd.DataFrame(columns=[
    "campaignName", "adId", "adGroupName", "ACOS_30d", "ACOS_7d", 
    "total_clicks_7d", "advertisedSku", "ORDER_1m", "definition"
])

def check_definitions(row):
    definitions = []
    
    # 定义一
    if row['ORDER_1m'] < 8 and row['ACOS_7d'] > 0.24 and row['total_cost_7d'] > 5:
        definitions.append(1)

    # 定义二
    if row['ORDER_1m'] < 8 and row['ACOS_30d'] > 0.24 and row['total_sales_7d'] == 0 and row['total_cost_7d'] > 5:
        definitions.append(2)

    # 定义三
    if row['ORDER_1m'] < 8 and 0.24 < row['ACOS_7d'] < 0.5 and 0 < row['ACOS_30d'] < 0.24 and row['total_cost_7d'] > 5:
        definitions.append(3)

    # 定义四
    if row['ORDER_1m'] < 8 and row['ACOS_7d'] > 0.24 and row['ACOS_30d'] > 0.24:
        definitions.append(4)

    # 定义五
    if row['ACOS_7d'] > 0.5:
        definitions.append(5)

    # 定义六
    if row['total_cost_30d'] > 5 and row['total_sales_30d'] == 0:
        definitions.append(6)

    # 定义七
    if row['ORDER_1m'] < 8 and row['total_cost_7d'] >= 5 and row['total_sales_7d'] == 0:
        definitions.append(7)

    # 定义八
    if row['ORDER_1m'] >= 8 and row['total_cost_7d'] >= 10 and row['total_sales_7d'] == 0:
        definitions.append(8)

    return definitions

# Apply the function and filter rows that match any definition
for index, row in df.iterrows():
    matching_definitions = check_definitions(row)
    if matching_definitions:
        row_data = {
            "campaignName": row["campaignName"],
            "adId": row["adId"],
            "adGroupName": row["adGroupName"],
            "ACOS_30d": row["ACOS_30d"],
            "ACOS_7d": row["ACOS_7d"],
            "total_clicks_7d": row["total_clicks_7d"],
            "advertisedSku": row["advertisedSku"],
            "ORDER_1m": row["ORDER_1m"],
            "definition": ','.join(map(str, matching_definitions))
        }
        result_df = result_df.append(row_data, ignore_index=True)

# Save the result to a new CSV file
output_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\关闭SKU\提问策略\SD_关闭SKU_v1_1_LAPASA_US_2024-07-14.csv"
result_df.to_csv(output_path, index=False)

print(f"Result saved to {output_path}")