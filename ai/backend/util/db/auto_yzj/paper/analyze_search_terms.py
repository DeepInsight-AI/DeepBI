# filename: analyze_search_terms.py

import pandas as pd

# 读取CSV文件内容
file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放搜索词优化\预处理.csv"
data = pd.read_csv(file_path)

# 筛选定义一至定义六的条件
conditions = [
    ((data["ACOS_30d"] > 0.24) & (data["ACOS_30d"] < 0.36) & (data["ORDER_1m"] <= 5)),        # 定义一
    ((data["ACOS_30d"] >= 0.36) & (data["ORDER_1m"] <= 8)),                                   # 定义二
    ((data["total_clicks_30d"] > 13) & (data["ORDER_1m"] == 0)),                              # 定义三
    ((data["ACOS_7d"] > 0.24) & (data["ACOS_7d"] < 0.36) & (data["ORDER_7d"] <= 3)),          # 定义四
    ((data["ACOS_7d"] >= 0.36) & (data["ORDER_7d"] <= 5)),                                    # 定义五
    ((data["total_clicks_7d"] > 10) & (data["ORDER_7d"] == 0)),                               # 定义六
]

# 定义满足的原因描述
reasons = [
    "定义一",
    "定义二",
    "定义三",
    "定义四",
    "定义五",
    "定义六",
]

# 查找满足条件的搜索词，并标记定义
filtered_data = pd.DataFrame()

for condition, reason in zip(conditions, reasons):
    temp_data = data[condition].copy()
    temp_data['reason'] = reason
    filtered_data = pd.concat([filtered_data, temp_data], ignore_index=True)

# 选择所需的列并重命名
output_columns = {
    "campaignName": "广告活动",
    "adGroupName": "广告组",
    "total_clicks_7d": "近七天的点击次数",
    "ACOS_7d": "近七天的acos值",
    "ORDER_7d": "近七天的订单数",
    "total_clicks_30d": "近一个月的总点击数",
    "ORDER_1m": "近一个月的订单数",
    "ACOS_30d": "近一个月的acos值",
    "searchTerm": "搜索词",
    "reason": "满足的定义"
}

filtered_data = filtered_data[output_columns.keys()]
filtered_data = filtered_data.rename(columns=output_columns)

# 保存结果到新的CSV文件
output_file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放搜索词优化\提问策略\手动_劣质_ASIN_搜索词_v1_1_LAPASA_DE_2024-07-09.csv"
filtered_data.to_csv(output_file_path, index=False, encoding='utf-8-sig')

print(f"结果已保存到 {output_file_path}")