# filename: detect_acos_anomaly.py

import pandas as pd

# 读取数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告活动\预处理.csv'
data = pd.read_csv(file_path)

# 检查数据的前几行
print(data.head())

# 将列名和解释映射
columns_mapping = {
    "广告活动": "campaignName",
    "广告活动ID": "campaignId",
    "广告活动最近30天的总销售额": "total_sales14d_1m",
    "广告活动最近7天的总销售额": "total_sales14d_7d",
    "广告活动最近30天的总花费": "total_cost_30d",
    "广告活动最近7天的总花费": "total_cost_7d",
    "广告活动最近30天的平均ACOS值": "ACOS_30d",
    "广告活动最近7天的平均ACOS值": "ACOS_7d",
    "广告活动昨天的ACOS值": "ACOS_yesterday",
    "广告活动昨天的impressions": "impressions_yesterday",
    "广告活动昨天的点击数": "clicks_yesterday",
    "广告活动昨天的花费": "cost_yesterday",
    "广告活动的预算": "campaignBudgetAmount",
    "广告活动昨天的销售额": "sales14d_yesterday"
}

data = data.rename(columns=columns_mapping)

# 检查昨日ACOS异常情况
def check_acos_anomaly(row):
    anomalies = []
    
    # 检查昨日ACOS为空的异常情况
    if pd.isna(row["ACOS_yesterday"]) or row["ACOS_yesterday"] == 0:
        if row["ACOS_7d"] < 25 or row["ACOS_30d"] < 25:
            level = "较好" if row["ACOS_7d"] < 25 else "极好"
            anomalies.append(f"昨日无销售额，近7天ACOS{level}")
        elif row["ACOS_30d"] < 25:
            level = "较好" if row["ACOS_30d"] < 25 else "极好"
            anomalies.append(f"昨日无销售额，近30天ACOS{level}")
    
    # 检查昨日ACOS相对于7天平均的变化情况
    if row["ACOS_yesterday"] > 0 and row["ACOS_7d"] > 0:
        if abs(row["ACOS_yesterday"] - row["ACOS_7d"]) / row["ACOS_7d"] > 0.3:
            anomalies.append("昨日ACOS相对近7天ACOS变化超过30%")
    
    # 检查昨日ACOS相对于30天平均的变化情况
    if row["ACOS_yesterday"] > 0 and row["ACOS_30d"] > 0:
        if abs(row["ACOS_yesterday"] - row["ACOS_30d"]) / row["ACOS_30d"] > 0.3:
            anomalies.append("昨日ACOS相对近30天ACOS变化超过30%")
    
    return anomalies if anomalies else None
    
# 检查所有数据的异常情况
data["anomalies"] = data.apply(check_acos_anomaly, axis=1)

# 保留存在异常的数据
anomalous_data = data[data["anomalies"].notnull()]

# 展开异常现象
exploded_data = anomalous_data.explode("anomalies")

# 重命名和选择最终输出列
exploded_data = exploded_data.rename(columns={
    "campaignId": "异常广告活动ID",
    "campaignName": "异常广告活动",
    "ACOS_yesterday": "昨天的ACOS",
    "ACOS_7d": "近七天ACOS",
    "ACOS_30d": "近30天ACOS",
    "anomalies": "异常现象"
})

# 选择所需列
output_columns = ["异常广告活动ID", "异常广告活动", "异常现象", "昨天的ACOS", "近七天ACOS", "近30天ACOS"]
final_output = exploded_data[output_columns]

# 保存到CSV文件
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告活动\提问策略\异常检测_广告活动_ACOS异常_v1_0_LAPASA_IT_2024-07-13.csv'
final_output.to_csv(output_path, index=False)

print("异常检测结果保存成功！")