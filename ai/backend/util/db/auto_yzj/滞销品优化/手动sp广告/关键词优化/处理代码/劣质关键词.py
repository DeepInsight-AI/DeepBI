# filename: handle_poor_performing_keywords.py
import os
import pandas as pd
import numpy as np
from ai.backend.util.db.auto_process.tools_db_new_sp import DbNewSpTools
from datetime import datetime


def main(path, brand, cur_time, country, version: int = 1):
    # 路径常量
    input_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\滞销品优化\手动sp广告\关键词优化\预处理.csv'
    file_name = "手动_劣质关键词" + '_' + brand + '_' + country + '_' + cur_time + '.csv'
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

            if 0.27 < row['ACOS_7d'] <= 0.5 and 0 < row['ACOS_30d'] <= 0.5 and row['ORDER_1m'] < 5:
                new_keywordBid = row['keywordBid'] / ((row['ACOS_7d'] - 0.27) / 0.27 + 1)
                action_reason = "定义一：更新出价"

            elif row['ACOS_7d'] > 0.5 and row['ACOS_30d'] <= 0.36:
                new_keywordBid = row['keywordBid'] / ((row['ACOS_7d'] - 0.27) / 0.27 + 1)
                action_reason = "定义二：更新出价"

            elif row['total_clicks_7d'] >= 10 and row['total_sales14d_7d'] == 0 and row['ACOS_30d'] <= 0.36:
                new_keywordBid = row['keywordBid'] - 0.04
                action_reason = "定义三：降低出价"

            elif row['total_clicks_7d'] > 10 and row['total_sales14d_7d'] == 0 and row['ACOS_30d'] > 0.5:
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
                    'ACOS_30d': row['ACOS_30d'],
                    'ORDER_1m': row['ORDER_1m'],
                    'total_clicks_30d': row['total_clicks_30d'],
                    'total_sales14d_30d': row['total_sales14d_30d'],
                    'total_cost_30d': row['total_cost_30d'],
                    'ACOS_7d': row['ACOS_7d'],
                    'total_clicks_7d': row['total_clicks_7d'],
                    'total_sales14d_7d': row['total_sales14d_7d'],
                    'total_cost_7d': row['total_cost_7d'],
                    'ACOS_3d': row['ACOS_3d'],
                    'total_sales14d_3d': row['total_sales14d_3d'],
                    'total_cost_3d': row['total_cost_3d'],
                    'action_reason': action_reason
                })

    # 写出结果到CSV
    output_df = pd.DataFrame(output_data)
    output_df.replace({np.nan: None}, inplace=True)
    api = DbNewSpTools(brand)
    for index, row in output_df.iterrows():
        api.create_keyword_info(country, brand, '滞销品优化', '手动_劣质', row['keyword'], row['keywordId'],
                                row['campaignName'], row['adGroupName'], row['matchType'], row['keywordBid'],
                                row['new_keywordBid'], row['ACOS_30d'], row['ORDER_1m'], row['total_clicks_30d'], row['total_sales14d_30d'], row['total_cost_30d'],
                                row['ACOS_7d'], row['total_clicks_7d'], row['total_sales14d_7d'], row['total_cost_7d'], row['ACOS_3d'], row['total_sales14d_3d'], row['total_cost_3d'], row['action_reason'], cur_time,
                                datetime.now(), 0)
    output_df.to_csv(output_file_path, index=False)

    print(f"结果已输出到文件：{output_file_path}")
