# filename: find_keywords_to_reduce_bid.py

import pandas as pd

# 加载数据
file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\自动定位组优化\预处理.csv"
data = pd.read_csv(file_path)

# 自动定位词降价的规则定义
rules = [
    {"condition": lambda row: 0 < row["ACOS_30d"] < 0.24 and 0.24 < row["ACOS_7d"] < 0.5, "减价": 0.03, "原因": "定义一：高于0.24小于0.5，并且最近30天的平均ACOS值大于0小于0.24"},
    {"condition": lambda row: 0.24 < row["ACOS_7d"] < 0.5 and 0.24 < row["ACOS_30d"] < 0.5, "减价": 0.04, "原因": "定义二：高于0.24小于0.5，并且最近30天的平均ACOS值大于0.24小于0.5"},
    {"condition": lambda row: row["total_sales14d_7d"] == 0 and row["total_clicks_7d"] > 0 and 0.24 < row["ACOS_30d"] < 0.5, "减价": 0.04, "原因": "定义三：近七天无销售额，有点击，并且平均acos大于0.24小于0.5"},
    {"condition": lambda row: 0.24 < row["ACOS_7d"] < 0.5 and row["ACOS_30d"] > 0.5, "减价": 0.05, "原因": "定义四：7天高于0.24小于0.5，并且最近30天的平均ACOS值大于0.5"},
    {"condition": lambda row: row["ACOS_7d"] > 0.5 and 0 < row["ACOS_30d"] < 0.24, "减价": 0.05, "原因": "定义五：7天高于0.5，并且最近30天的平均ACOS值大于0小于0.24"},
]

# 创建一个新的DataFrame来存放筛选结果
results = []

# 遍历数据，每一行数据应用所有规则
for index, row in data.iterrows():
    for rule in rules:
        if rule["condition"](row):
            results.append({
                "campaignName": row["campaignName"],
                "adGroupName": row["adGroupName"],
                "keyword": row["keyword"],
                "ACOS_30d": row["ACOS_30d"],
                "ACOS_7d": row["ACOS_7d"],
                "原因": rule["原因"]
            })
            break  # 一旦匹配一个规则，就不继续匹配其他规则

# 转换结果为DataFrame
result_df = pd.DataFrame(results)

# 保存结果为CSV文件
result_df.to_csv(r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\自动定位组优化\提问策略\劣质自动定位组_FR.csv", index=False)
print("结果已保存到指定路径")