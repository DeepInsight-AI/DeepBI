# filename: 手动_复开SKU_v1_1_LAPASA_IT_2024-06-30.py

import pandas as pd

# 步骤 1: 加载数据集
file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\复开SKU\预处理.csv"
data = pd.read_csv(file_path)

# 步骤 2: 筛选数据

# 满足定义一
condition1_a = (data["ACOS_30d"] > 0) & (data["ACOS_30d"] <= 0.24)
condition1_b = (data["ACOS_7d"] > 0) & (data["ACOS_7d"] <= 0.24)
filter1 = condition1_a & condition1_b

# 满足定义二
condition2_a = (data["ACOS_30d"] > 0) & (data["ACOS_30d"] <= 0.24)
condition2_b = data["total_clicks_7d"] == 0
filter2 = condition2_a & condition2_b

# 组合条件
final_filter = filter1 | filter2

# 筛选符合条件的数据
result = data[final_filter]

# 补充"满足的定义"列
def get_definition(row):
    if (row["ACOS_30d"] > 0 and row["ACOS_30d"] <= 0.24 and
            row["ACOS_7d"] > 0 and row["ACOS_7d"] <= 0.24):
        return "定义一"
    elif (row["ACOS_30d"] > 0 and row["ACOS_30d"] <= 0.24 and row["total_clicks_7d"] == 0):
        return "定义二"
    else:
        return ""

result["定义"] = result.apply(get_definition, axis=1)

# 保留需要列
columns_to_keep = ["campaignName", "adId", "adGroupName", "ACOS_30d", "ACOS_7d", "total_clicks_7d", "advertisedSku", "ORDER_1m", "定义"]
result_final = result[columns_to_keep]

# 步骤 3: 输出结果
output_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\复开SKU\提问策略\手动_复开SKU_v1_1_LAPASA_IT_2024-06-30.csv"
result_final.to_csv(output_path, index=False)

print(f"结果已保存到 {output_path}")