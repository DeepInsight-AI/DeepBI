# filename: handle_ad_performance.py
import os
import pandas as pd


def main(path, cur_time, country):
    # 读取CSV文件
    file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\预算优化\预处理.csv'
    file_name = "自动_劣质广告活动" + '_' + country + '_' + cur_time + '.csv'
    output_file_path = os.path.join(path, file_name)
    data = pd.read_csv(file_path)

    # 定义结果列表
    results = []

    # 遍历广告活动数据并检查每条数据是否满足条件
    for index, row in data.iterrows():
        campaignId = row['campaignId']
        campaignName = row['campaignName']
        Budget = row['Budget']
        ACOS_7d = row['ACOS_7d']
        ACOS_yesterday = row['ACOS_yesterday']
        clicks_yesterday = row['clicks_yesterday']
        cost_yesterday = row['cost_yesterday']
        country_avg_ACOS_1m = row['country_avg_ACOS_1m']
        ACOS_30d = row['ACOS_30d']
        total_clicks_7d = row['total_clicks_7d']
        total_clicks_30d = row['total_clicks_30d']
        total_sales14d_7d = row['total_sales14d_7d']
        total_sales14d_30d = row['total_sales14d_30d']

        # 定义一
        if (ACOS_7d > 0.24 and ACOS_yesterday > 0.24 and clicks_yesterday >= 10 and ACOS_30d > country_avg_ACOS_1m):
            reason = '定义一'
            new_budget = max(8, Budget - 5)
            results.append([campaignId, campaignName, Budget, new_budget, clicks_yesterday, ACOS_yesterday, ACOS_7d,
                            total_clicks_7d, total_sales14d_7d, ACOS_30d, total_clicks_30d, total_sales14d_30d,
                            country_avg_ACOS_1m, reason])

        # 定义二
        elif (ACOS_7d > 0.24 and ACOS_yesterday > 0.24 and cost_yesterday > 0.8 * Budget and ACOS_30d > country_avg_ACOS_1m):
            reason = '定义二'
            new_budget = max(8, Budget - 5)
            results.append([campaignId, campaignName, Budget, new_budget, clicks_yesterday, ACOS_yesterday, ACOS_7d,
                            total_clicks_7d, total_sales14d_7d, ACOS_30d, total_clicks_30d, total_sales14d_30d,
                            country_avg_ACOS_1m, reason])

        # 定义三
        elif (ACOS_30d > 0.24 and ACOS_30d > country_avg_ACOS_1m and total_sales14d_7d == 0 and total_clicks_7d >= 15):
            reason = '定义三'
            new_budget = max(5, Budget - 5)
            results.append([campaignId, campaignName, Budget, new_budget, clicks_yesterday, ACOS_yesterday, ACOS_7d,
                            total_clicks_7d, total_sales14d_7d, ACOS_30d, total_clicks_30d, total_sales14d_30d,
                            country_avg_ACOS_1m, reason])

        # 定义四
        elif (total_sales14d_30d == 0 and total_clicks_30d >= 75):
            reason = '定义四'
            new_budget = '关闭'
            results.append([campaignId, campaignName, Budget, new_budget, clicks_yesterday, ACOS_yesterday, ACOS_7d,
                            total_clicks_7d, total_sales14d_7d, ACOS_30d, total_clicks_30d, total_sales14d_30d,
                            country_avg_ACOS_1m, reason])

    # 转换为DataFrame并保存为新的CSV文件
    columns = ['campaignId', 'campaignName', 'Budget', 'New_Budget', 'clicks_yesterday', 'ACOS_yesterday', 'ACOS_7d',
               'total_clicks_7d', 'total_sales14d_7d', 'ACOS_30d', 'total_clicks_30d', 'total_sales14d_30d',
               'country_avg_ACOS_1m', 'Reason']
    results_df = pd.DataFrame(results, columns=columns)

    results_df.to_csv(output_file_path, index=False)
    print(f'分析结果已保存在 {output_file_path} 文件中。')
