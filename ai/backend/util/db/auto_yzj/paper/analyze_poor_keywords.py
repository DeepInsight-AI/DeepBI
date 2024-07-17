# filename: analyze_poor_keywords.py
import pandas as pd

# 读取CSV数据
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv'
df = pd.read_csv(file_path)

# 根据定义的条件进行筛选和计算

def calculate_new_bid(keywordBid, ACOS_7d, threshold=0.24):
    new_bid = keywordBid / ((ACOS_7d - threshold) / threshold + 1)
    return max(new_bid, 0.05)

# 增加新的字段
df['new_keywordBid'] = df.apply(
    lambda row: calculate_new_bid(row['keywordBid'], row['ACOS_7d'], 0.24) if row['ACOS_7d'] > 0.24 and row['ACOS_7d'] <= 0.5 and row['ACOS_30d'] > 0 and row['ACOS_30d'] <= 0.5 and row['ORDER_1m'] < 5 and row['ACOS_3d'] >= 0.24 else (
        calculate_new_bid(row['keywordBid'], row['ACOS_7d'], 0.24) if row['ACOS_7d'] > 0.5 and row['ACOS_30d'] <= 0.36 and row['ACOS_3d'] > 0.24 else (
            row['keywordBid'] - 0.03 if row['total_clicks_7d'] >= 10 and row['total_sales14d_7d'] == 0 and row['total_cost_7d'] <= 5 and row['ACOS_30d'] <= 0.36 else (
                0.05 if row['total_clicks_7d'] > 10 and row['total_sales14d_7d'] == 0 and row['total_cost_7d'] > 7 and row['ACOS_30d'] > 0.5 else (
                    0.05 if row['ACOS_7d'] > 0.5 and row['ACOS_3d'] > 0.24 and row['ACOS_30d'] > 0.36 else (
                        0.05 if row['total_sales14d_30d'] == 0 and row['total_cost_30d'] >= 10 and row['total_clicks_30d'] >= 15 else (
                            calculate_new_bid(row['keywordBid'], row['ACOS_7d'], 0.24) if row['ACOS_7d'] > 0.24 and row['ACOS_7d'] <= 0.5 and row['ACOS_30d'] > 0 and row['ACOS_30d'] <= 0.5 and row['ORDER_1m'] < 5 and row['total_sales14d_3d'] == 0 else (
                                calculate_new_bid(row['keywordBid'], row['ACOS_7d'], 0.24) if row['ACOS_7d'] > 0.5 and row['ACOS_30d'] <= 0.36 and row['total_sales14d_3d'] == 0 else (
                                    0.05 if row['ACOS_7d'] > 0.5 and row['total_sales14d_3d'] == 0 and row['ACOS_30d'] > 0.36 else (
                                        calculate_new_bid(row['keywordBid'], row['ACOS_7d'], 0.24) if row['ACOS_7d'] > 0.24 and row['ACOS_7d'] <= 0.5 and row['ACOS_30d'] > 0.5 and row['ORDER_1m'] < 5 and row['ACOS_3d'] >= 0.24 else (
                                            calculate_new_bid(row['keywordBid'], row['ACOS_7d'], 0.24) if row['ACOS_7d'] > 0.24 and row['ACOS_7d'] <= 0.5 and row['total_sales14d_3d'] == 0 and row['ACOS_30d'] > 0.5 else (
                                                row['keywordBid']-0.01 if row['ACOS_7d'] <= 0.24 and row['total_sales14d_3d'] == 0 and row['total_cost_3d'] > 3 and row['total_cost_3d'] < 5 else (
                                                    row['keywordBid']-0.02 if row['ACOS_7d'] <= 0.24 and row['ACOS_3d'] > 0.24 and row['ACOS_3d'] < 0.36 else (
                                                        row['keywordBid']-0.03 if row['ACOS_7d'] <= 0.24 and row['ACOS_3d'] > 0.36 else (
                                                            0.05 if row['total_clicks_7d'] > 10 and row['total_sales14d_7d'] == 0 and row['total_cost_7d'] >= 10 and row['ACOS_30d'] <= 0.36 else (
                                                                row['keywordBid']-0.07 if row['total_clicks_7d'] > 10 and row['total_sales14d_7d'] == 0 and row['total_cost_7d'] > 5 and row['total_cost_7d'] < 10 and row['ACOS_30d'] <= 0.36 else row['keywordBid']
                                                            )
                                                        )
                                                    )
                                                )
                                            )
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
            )
        )
    ),
    axis=1
)

# 筛选出需要调整的关键词
filtered_df = df[(df['new_keywordBid'] != df['keywordBid']) | (df['new_keywordBid'] == 0.05)]

# 输出结果到CSV文件
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_劣质商品投放_v1_1_LAPASA_IT_2024-07-12.csv'
filtered_df.to_csv(output_file_path, index=False)

print("文件已成功生成并保存到：", output_file_path)