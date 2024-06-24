# filename: gutana_task.py

import pandas as pd

# 读取CSV文件
file_path = "C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\自动sp广告\\搜索词优化\\预处理.csv"
data = pd.read_csv(file_path)

# 定义 acos 边界分类函数
def classify_acos(acos):
    if pd.isna(acos) or acos == float('inf'):
        return "极高"
    elif acos < 20:
        return "极低"
    elif 20 <= acos < 30:
        return "较低"
    elif 30 <= acos < 50:
        return "较高"
    else:
        return "极高"

# 预处理 acos
data['ACOS_7d'] = data['ACOS_7d'].apply(classify_acos)

# 满足定义一,定义二,定义三的搜索词
reasons = []

for index, row in data.iterrows():
    reason = ""

    # 定义一
    if row['ACOS_7d'] == "较高" and row['total_clicks_7d'] > 50 and row['total_sales14d_7d'] < (0.05 * row['total_sales14d_30d']):
        reason = "定义一"

    # 定义二
    if row['ACOS_7d'] == "极高" and row['total_sales14d_7d'] < (0.05 * row['total_sales14d_30d']):
        reason = "定义二"

    # 定义三
    if row['total_clicks_30d'] > 10 and row['total_cost_30d'] > 0 and row['total_sales14d_30d'] == 0:
        reason = reason + ", 定义三" if reason else "定义三"

    if reason:
        reasons.append({
            "campaignName": row['campaignName'],
            "adGroupName": row['adGroupName'],
            "cost_7d": row['total_cost_7d'],
            "week_acos": row['ACOS_7d'],
            "sum_clicks": row['total_clicks_30d'],
            "searchTerm": row['searchTerm'],
            "reason": reason.strip()
        })

# 转换为 DataFrame
reason_data = pd.DataFrame(reasons)

# 保存到新 CSV 文件中
output_path = "C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\自动sp广告\\搜索词优化\\提问策略\\自动_劣质搜索词_ES_2024-06-05.csv"
reason_data.to_csv(output_path, index=False)

print("Data processed and saved successfully.")