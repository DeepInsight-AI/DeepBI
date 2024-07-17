# filename: auto_bid_optimization.py

import pandas as pd

# 读取数据
file_path = "C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\自动sp广告\\广告位优化\\预处理.csv"
data = pd.read_csv(file_path)

# 定义条件函数
def check_conditions(row, all_ads):
    # 满足定义一 或 定义二的广告位
    def is_condition_met(ac_cols, click_col, acos_range, cost_col=None, click_cond=True, cost_cond=True):
        ac_min_7d = all_ads[ac_cols[0]].min()
        ac_min_3d = all_ads[ac_cols[1]].min()
        max_clicks_7d = all_ads[click_col[0]].max()
        max_clicks_3d = all_ads[click_col[1]].max()

        base_condition = (row[ac_cols[0]] == ac_min_7d and row[ac_cols[0]] > 0 and row[ac_cols[0]] <= acos_range and
                          row[ac_cols[1]] == ac_min_3d and row[ac_cols[1]] > 0 and row[ac_cols[1]] <= acos_range and
                          (row[click_col[0]] != max_clicks_7d if click_cond else True) and
                          (row[click_col[1]] != max_clicks_3d if click_cond else True))
        
        additional_condition = True
        if cost_col:
            additional_condition = row[cost_col] < 4 if cost_cond else True
        
        return base_condition and additional_condition

    # 定义字段
    acos_cols = ["ACOS_7d", "ACOS_3d"]
    click_cols = ["total_clicks_7d", "total_clicks_3d"]
    return (is_condition_met(acos_cols, click_cols, 0.24, click_cond=True, cost_cond=True) or 
            is_condition_met(acos_cols, click_cols, 0.24, cost_col="total_cost_3d", click_cond=False, cost_cond=True))

# 处理每一个广告活动
result = []
for campaign, group in data.groupby("campaignName"):
    for _, row in group.iterrows():
        if check_conditions(row, group):
            adjusted_bid = min(row['bid'] + 5, 50)
            reason = "Definition 1" if row['total_cost_3d'] >= 4 else "Definition 2"
            result.append([row['campaignName'], row['campaignId'], row['placementClassification'], 
                           row['ACOS_7d'], row['ACOS_3d'], row['total_clicks_7d'], row['total_clicks_3d'], 
                           row['bid'], adjusted_bid, reason])
            
# 转换为 DataFrame 并保存结果
columns = ["campaignName", "campaignId", "placementClassification", "ACOS_7d", "ACOS_3d", 
           "total_clicks_7d", "total_clicks_3d", "bid", "adjusted_bid", "reason"]
result_df = pd.DataFrame(result, columns=columns)

# 保存到指定路径
output_path = "C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\自动sp广告\\广告位优化\\提问策略\\自动_优质广告位_v1_1_LAPASA_DE_2024-07-12.csv"
result_df.to_csv(output_path, index=False)

print("文件已成功生成并保存到：", output_path)