# filename: handle_poor_performing_keywords.py
import os
import pandas as pd


def main(path, cur_time, country, version: int = 2):
    # 路径常量
    input_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\关键词优化\预处理.csv'
    file_name = "手动_劣质关键词" + '_' + country + '_' + cur_time + '.csv'
    output_file_path = os.path.join(path, file_name)

    # 读取CSV文件
    df = pd.read_csv(input_file_path)

    # 定义输出列表
    output_data = []
    if version == 1:
        # 循环遍历每个关键词
        for index, row in df.iterrows():
            new_keywordBid = row['keywordBid']
            action_reason = ""
            total_cost_7d = row['total_cost_7d']
            total_sales14d_7d = row['total_sales14d_7d']

            if 0.24 < row['ACOS_7d'] <= 0.5 and 0 < row['ACOS_30d'] <= 0.5:
                new_keywordBid = row['keywordBid'] / ((row['ACOS_7d'] - 0.24) / 0.24 + 1)
                action_reason = "定义一：更新出价"

            elif row['ACOS_7d'] > 0.5 and row['ACOS_30d'] <= 0.36:
                new_keywordBid = row['keywordBid'] / ((row['ACOS_7d'] - 0.24) / 0.24 + 1)
                action_reason = "定义二：更新出价"

            elif row['total_clicks_7d'] >= 10 and row['total_sales14d_7d'] == 0 and row['ACOS_30d'] <= 0.36:
                new_keywordBid = row['keywordBid'] - 0.04
                action_reason = "定义三：降低出价"

            elif row['total_clicks_7d'] >= 10 and row['total_sales14d_7d'] == 0 and row['ACOS_30d'] > 0.5:
                new_keywordBid = "关闭"
                action_reason = "定义四：关闭该词"

            elif row['ACOS_7d'] > 0.5 and row['ACOS_30d'] > 0.36:
                new_keywordBid = "关闭"
                action_reason = "定义五：关闭该词"

            elif row['total_sales14d_30d'] == 0 and row['total_cost_7d'] > (
                    df[df['adGroupName'] == row['adGroupName']]['total_cost_7d'].sum() / 5):
                new_keywordBid = "关闭"
                action_reason = "定义六：关闭该词"

            elif row['total_sales14d_30d'] == 0 and row['total_clicks_30d'] >= 13:
                new_keywordBid = "关闭"
                action_reason = "定义七：关闭该词"

            # 如果关键词受到了调整或需要关闭，加进输出数据
            if new_keywordBid != row['keywordBid']:
                output_data.append({
                    'keyword': row['keyword'],
                    'keywordId': row['keywordId'],
                    'campaignName': row['campaignName'],
                    'adGroupName': row['adGroupName'],
                    'matchType': row['matchType'],
                    'keywordBid': row['keywordBid'],
                    'new_keywordBid': new_keywordBid,
                    'targeting': row['targeting'],
                    'total_cost_7d': row['total_cost_7d'],
                    'total_sales14d_7d': row['total_sales14d_7d'],
                    'adGroup_total_cost_7d': df[df['adGroupName'] == row['adGroupName']]['total_cost_7d'].sum(),
                    'ACOS_7d': row['ACOS_7d'],
                    'ACOS_30d': row['ACOS_30d'],
                    'action_reason': action_reason
                })
    elif version == 2:
        # 循环遍历每个关键词
        for index, row in df.iterrows():
            new_keywordBid = row['keywordBid']
            action_reason = ""
            total_cost_7d = row['total_cost_7d']
            total_sales14d_7d = row['total_sales14d_7d']

            if 0.24 < row['ACOS_7d'] <= 0.5 and 0 < row['ACOS_30d'] <= 0.5:
                new_keywordBid = row['keywordBid'] / ((row['ACOS_7d'] - 0.24) / 0.24 + 1)
                action_reason = "定义一：更新出价"

            elif row['ACOS_7d'] > 0.5 and row['ACOS_30d'] <= 0.36:
                new_keywordBid = row['keywordBid'] / ((row['ACOS_7d'] - 0.24) / 0.24 + 1)
                action_reason = "定义二：更新出价"

            elif row['total_clicks_7d'] >= 10 and row['total_sales14d_7d'] == 0 and row['ACOS_30d'] <= 0.36:
                new_keywordBid = row['keywordBid'] - 0.04
                action_reason = "定义三：降低出价"

            elif row['total_clicks_7d'] >= 10 and row['total_sales14d_7d'] == 0 and row['ACOS_30d'] > 0.5:
                new_keywordBid = "关闭"
                action_reason = "定义四：关闭该词"

            elif row['ACOS_7d'] > 0.5 and row['ACOS_30d'] > 0.36:
                new_keywordBid = "关闭"
                action_reason = "定义五：关闭该词"

            elif row['total_sales14d_30d'] == 0 and row['total_cost_30d'] >= 5:
                new_keywordBid = "关闭"
                action_reason = "定义六：关闭该词"

            elif row['total_sales14d_30d'] == 0 and row['total_clicks_30d'] >= 15 and row['total_clicks_7d'] > 0:
                new_keywordBid = "关闭"
                action_reason = "定义七：关闭该词"

            # 如果关键词受到了调整或需要关闭，加进输出数据
            if new_keywordBid != row['keywordBid']:
                output_data.append({
                    'keyword': row['keyword'],
                    'keywordId': row['keywordId'],
                    'campaignName': row['campaignName'],
                    'adGroupName': row['adGroupName'],
                    'matchType': row['matchType'],
                    'keywordBid': row['keywordBid'],
                    'new_keywordBid': new_keywordBid,
                    'targeting': row['targeting'],
                    'total_cost_7d': row['total_cost_7d'],
                    'total_sales14d_7d': row['total_sales14d_7d'],
                    'adGroup_total_cost_7d': df[df['adGroupName'] == row['adGroupName']]['total_cost_7d'].sum(),
                    'ACOS_7d': row['ACOS_7d'],
                    'ACOS_30d': row['ACOS_30d'],
                    'action_reason': action_reason
                })
    # 写出结果到CSV
    output_df = pd.DataFrame(output_data)
    output_df.to_csv(output_file_path, index=False)

    print(f"结果已输出到文件：{output_file_path}")
