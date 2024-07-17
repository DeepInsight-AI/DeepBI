# filename: process_autopilot_words.py
import pandas as pd
import os

# 读取原始数据
input_file = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\滞销品优化\自动sp广告\自动定位组优化\预处理.csv"
df = pd.read_csv(input_file)

# 定义符合条件的函数
def determine_action(row):
    ACOS_7d = row['ACOS_7d']
    ACOS_30d = row['ACOS_30d']
    clicks_7d = row['total_clicks_7d']
    sales_7d = row['total_sales14d_7d']
    clicks_30d = row['total_clicks_30d']
    sales_30d = row['total_sales14d_30d']
    
    action = None
    reason = None
    new_bid = row['keywordBid']

    # 定义一
    if 0.27 < ACOS_7d < 0.5 and 0 < ACOS_30d < 0.27:
        action = -0.03
        reason = "定义一：降低竞价0.03"
    
    # 定义二
    elif 0.27 < ACOS_7d < 0.5 and 0.27 < ACOS_30d < 0.5:
        action = -0.04
        reason = "定义二：降低竞价0.04"
        
    # 定义三
    elif sales_7d == 0 and clicks_7d > 20 and 0.27 < ACOS_30d < 0.5:
        action = -0.04
        reason = "定义三：降低竞价0.04"
    
    # 定义四
    elif 0.27 < ACOS_7d < 0.5 and ACOS_30d > 0.5:
        action = -0.05
        reason = "定义四：降低竞价0.05"
        
    # 定义五
    elif ACOS_7d > 0.5 and 0 < ACOS_30d < 0.27:
        action = -0.05
        reason = "定义五：降低竞价0.05"
        
    # 定义六
    elif sales_30d == 0 and clicks_30d > 20 and clicks_7d > 0:
        action = '关闭'
        reason = "定义六：关闭自动定位词"
    
    # 定义七
    elif sales_7d == 0 and clicks_7d > 0 and ACOS_30d > 0.5:
        action = '关闭'
        reason = "定义七：关闭自动定位词"
    
    # 定义八
    elif ACOS_7d > 0.5 and ACOS_30d > 0.27:
        action = '关闭'
        reason = "定义八：关闭自动定位词"
    
    if action is not None:
        if action == '关闭':
            new_bid = action
        else:
            new_bid -= action
            new_bid = max(new_bid, 0)  # 确保竞价不会降到负值
    
        return pd.Series([row['campaignName'], row['adGroupName'], row['keyword'], row['keywordBid'], new_bid, row['ACOS_30d'], row['ACOS_7d'], row['total_clicks_7d'], action, reason])
    else:
        return None

# 筛选并处理数据
result_df = df.apply(determine_action, axis=1).dropna()

# 添加结果表头
result_df.columns = ["campaignName", "adGroupName", "keyword", "keywordBid", "New Bid", "ACOS_30d", "ACOS_7d", "clicks_7d", "对该词进行降价多少", "降价的原因"]

# 保存结果到新的CSV文件
output_file = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\滞销品优化\自动sp广告\自动定位组优化\提问策略\自动_劣质自动定位组_v1_1_LAPASA_IT_2024-07-03.csv"
os.makedirs(os.path.dirname(output_file), exist_ok=True)
result_df.to_csv(output_file, index=False)

print("处理完毕，结果已保存至：", output_file)