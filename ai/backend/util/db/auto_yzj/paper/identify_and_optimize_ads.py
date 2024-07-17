# filename: identify_and_optimize_ads.py
import pandas as pd

# 文件位置
file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv"
output_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_劣质商品投放_v1_1_LAPASA_DE_2024-07-02.csv"

# 读取CSV文件
df = pd.read_csv(file_path)

# 定义不符合条件的信息列表
result = []

# 共享部分数据筛选
conditions = [
    (df['ACOS_7d'] > 0.24) & (df['ACOS_7d'] <= 0.5) & (df['ACOS_30d'] > 0) & (df['ACOS_30d'] <= 0.5),
    (df['ACOS_7d'] > 0.5) & (df['ACOS_30d'] <= 0.36),
    (df['total_clicks_7d'] >= 10) & (df['total_sales14d_7d'] == 0) & (df['ACOS_30d'] <= 0.36),
    (df['total_clicks_7d'] > 10) & (df['total_sales14d_7d'] == 0) & (df['ACOS_30d'] > 0.5),
    (df['ACOS_7d'] > 0.5) & (df['ACOS_30d'] > 0.36),
    (df['total_sales14d_30d'] == 0) & (df['total_cost_30d'] >= 5),
    (df['total_sales14d_30d'] == 0) & (df['total_clicks_30d'] >= 15) & (df['total_clicks_7d'] > 0)
]

# 对每个条件进行筛选
for i, condition in enumerate(conditions):
    subset = df[condition]
    for _, row in subset.iterrows():
        keywordBid = row['keywordBid']
        ACOS_7d = row['ACOS_7d']
        ACOS_30d = row['ACOS_30d']
        new_keywordBid = '关闭'
        reason = ''
        
        # 定义一和定义二的新竞价
        if i == 0 or i == 1:
            new_keywordBid = keywordBid / ((ACOS_7d - 0.24) / 0.24 + 1)
            reason = '符合定义一' if i == 0 else '符合定义二'
        elif i == 2:
            new_keywordBid = keywordBid - 0.04
            reason = '符合定义三'
        elif i == 3:
            reason = '符合定义四'
        elif i == 4:
            reason = '符合定义五'
        elif i == 5:
            reason = '符合定义六'
        elif i == 6:
            reason = '符合定义七'
        
        result.append({
            'keyword': row['keyword'],
            'keywordId': row['keywordId'],
            'campaignName': row['campaignName'],
            'adGroupName': row['adGroupName'],
            '匹配类型': row['matchType'],
            '商品投放出价(keywordBid)': row['keywordBid'],
            'New_keywordBid': new_keywordBid,
            'targeting': row['targeting'],
            'cost': row['total_cost_7d'],
            'clicks': row['total_clicks_7d'],
            '商品投放最近7天的总花费': row['total_cost_7d'],
            '商品投放最近7天的总销售额': row['total_sales14d_7d'],
            '广告组最近7天的总花费': row['total_cost_7d'],
            '商品投放最近7天的平均ACOS值': row['ACOS_7d'],
            '商品投放最近一个月的平均ACOS值': row['ACOS_30d'],
            '商品投放最近30天的点击次数': row['total_clicks_30d'],
            '操作原因': reason
        })

# 将结果写入 CSV 文件
result_df = pd.DataFrame(result)
result_df.to_csv(output_path, index=False, encoding='utf-8-sig')

print("处理完毕，结果已保存到：", output_path)