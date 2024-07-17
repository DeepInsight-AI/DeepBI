# filename: analyze_ads_performance.py

import pandas as pd

# 读取数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\预处理.csv'
data = pd.read_csv(file_path)

# 查询的广告位类型
placements = ["Top of Search on-Amazon", "Detail Page on-Amazon", "Other on-Amazon"]

# 定义一个用来检查是否符合条件并调整竞价的函数
def adjust_bid(row):
    change_reason = None
    # 定义一
    if (row["ACOS_7d"] > 0 and row["ACOS_7d"] <= 0.24 and 
        row["ACOS_3d"] > 0 and row["ACOS_3d"] <= 0.24 and 
        row["total_clicks_7d"] != data[data["campaignId"] == row["campaignId"]]["total_clicks_7d"].max() and
        row["total_clicks_3d"] != data[data["campaignId"] == row["campaignId"]]["total_clicks_3d"].max()):
        
        change_reason = "定义一"
    
    # 定义二
    elif (row["ACOS_7d"] > 0 and row["ACOS_7d"] <= 0.24 and 
          row["total_clicks_7d"] != data[data["campaignId"] == row["campaignId"]]["total_clicks_7d"].max() and
          row["total_cost_3d"] < 4 and 
          row["total_clicks_3d"] != data[data["campaignId"] == row["campaignId"]]["total_clicks_3d"].max()):
        
        change_reason = "定义二"

    if change_reason:
        row["new_bid"] = row["bid"] + 5
        if row["new_bid"] > 50:
            row["new_bid"] = 50
        row["change_reason"] = change_reason
    return row

# 初始化新列
data["new_bid"] = data["bid"]
data["change_reason"] = ""

# 应用竞价调整
data = data.apply(adjust_bid, axis=1)

# 过滤出已变更竞价的行
result_data = data[data["change_reason"] != ""]

# 输出结果到新的CSV文件
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\提问策略\自动_优质广告位_v1_1_LAPASA_IT_2024-07-12.csv'

result_data.to_csv(output_path, index=False, columns=[
    "campaignName", "campaignId", "placementClassification", "ACOS_7d", "ACOS_3d", 
    "total_clicks_7d", "total_clicks_3d", "bid", "new_bid", "change_reason"
])

print("Execution complete. Results are saved in:", output_path)