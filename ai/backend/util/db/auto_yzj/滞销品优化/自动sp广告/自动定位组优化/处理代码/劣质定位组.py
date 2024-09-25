# filename: process_keywords.py
import pandas as pd
import os
import numpy as np
from ai.backend.util.db.auto_process.tools_db_new_sp import DbNewSpTools
from datetime import datetime


def main(path, brand, cur_time, country, version: int = 2):
    # Load the data
    file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\滞销品优化\自动sp广告\自动定位组优化\预处理.csv'
    file_name = "自动_劣质定位组" + '_' + brand + '_' + country + '_' + cur_time + '.csv'
    output_file_path = os.path.join(path, file_name)
    data = pd.read_csv(file_path)

    # Initialize an empty list to store the results
    results = []

    if version == 1:
        # Process each row in the data
        for index, row in data.iterrows():
            keyword_info = {
                "campaignName": row['campaignName'],
                "adGroupName": row['adGroupName'],
                "keyword": row['keyword'],
                'keywordId':row['keywordId'],
                "keywordBid": row['keywordBid'],
                "New_keywordBid": row['keywordBid'],
                'ACOS_30d': row['ACOS_30d'],
                'total_clicks_30d': row['total_clicks_30d'],
                'total_sales14d_30d': row['total_sales14d_30d'],
                'total_cost_30d': row['total_cost_30d'],
                'ACOS_7d': row['ACOS_7d'],
                'total_clicks_7d': row['total_clicks_7d'],
                'total_sales14d_7d': row['total_sales14d_7d'],
                'total_cost_7d': row['total_cost_7d'],
                "Reason": ""
            }

            if 0.27 < row['ACOS_7d'] < 0.5:
                if 0 <= row['ACOS_30d'] < 0.27:
                    keyword_info["New_keywordBid"] = row['keywordBid'] - 0.03
                    keyword_info["Reason"] = "定义一"
                elif 0.27 <= row['ACOS_30d'] < 0.5:
                    keyword_info["New_keywordBid"] = row['keywordBid'] - 0.04
                    keyword_info["Reason"] = "定义二"
                elif row['ACOS_30d'] >= 0.5:
                    keyword_info["New_keywordBid"] = row['keywordBid'] - 0.05
                    keyword_info["Reason"] = "定义四"
            if (row['total_sales14d_7d'] == 0 and
                row['total_clicks_7d'] > 20 and
                0.27 < row['ACOS_30d'] < 0.5):
                keyword_info["New_keywordBid"] = row['keywordBid'] - 0.04
                keyword_info["Reason"] += "定义三"
            if (row['total_sales14d_30d'] == 0 and
                row['total_clicks_30d'] > 20 and
                row['total_clicks_7d'] > 10):
                keyword_info["New_keywordBid"] = "关闭"
                keyword_info["Reason"] += "定义六"
            if (row['total_sales14d_7d'] == 0 and
                row['total_clicks_7d'] > 0 and
                row['ACOS_30d'] > 0.5):
                keyword_info["New_keywordBid"] = "关闭"
                keyword_info["Reason"] += "定义七"
            if row['ACOS_7d'] > 0.5:
                if row['ACOS_30d'] >= 0.27:
                    keyword_info["New_keywordBid"] = "关闭"
                    keyword_info["Reason"] += "定义八"
                elif 0 <= row['ACOS_30d'] < 0.27:
                    keyword_info["New_keywordBid"] = row['keywordBid'] - 0.05
                    keyword_info["Reason"] += "定义五"
            if keyword_info["Reason"]:  # Only keep the rows where we have performed an action
                results.append(keyword_info)

        # Create a DataFrame from the results
        results_df = pd.DataFrame(results)
        results_df.replace({np.nan: None}, inplace=True)
        api = DbNewSpTools(brand,country)
        for index, row in results_df.iterrows():
            api.create_automatic_targeting_info(country, brand, '滞销品优化', '自动_劣质', row['keyword'], row['keywordId'],
                                    row['campaignName'], row['adGroupName'], row['keywordBid'],
                                    row['new_keywordBid'], row['ACOS_30d'], row['total_clicks_30d'],
                                    row['total_sales14d_30d'], row['total_cost_30d'],None,
                                    row['ACOS_7d'], row['total_clicks_7d'], row['total_sales14d_7d'], row['total_cost_7d'],
                                    None, None, None, row['Reason'],
                                    cur_time,
                                    datetime.now(), 0)
        # Save the results to a new CSV file
        results_df.to_csv(output_file_path, index=False)

        print("Process finished and results saved.")
    elif version == 2:
        output_data = []
        # 循环遍历每个关键词
        for index, row in data.iterrows():
            new_keywordBid = row['keywordBid']
            action_reason = ""
            bid_adjust = ""

            if 0.27 < row['ACOS_7d'] < 0.5 and 0 < row['ACOS_30d'] < 0.27 and row['ACOS_3d'] >= 0.27:
                new_keywordBid = max(0.05, row['keywordBid'] - 0.03) if row['keywordBid'] >= 0.05 else \
                row['keywordBid']
                bid_adjust = -0.03
                action_reason = "定义一：更新出价"

            elif 0.27 < row['ACOS_7d'] < 0.5 and 0.27 < row['ACOS_30d'] < 0.5 and row['ACOS_3d'] >= 0.27:
                new_keywordBid = max(0.05, row['keywordBid'] - 0.04) if row['keywordBid'] >= 0.05 else \
                row['keywordBid']
                bid_adjust = -0.04
                action_reason = "定义二：更新出价"

            elif row['total_clicks_7d'] > 20 and row['total_sales14d_7d'] == 0 and 0.27 < row['ACOS_30d'] < 0.5:
                new_keywordBid = max(0.05, row['keywordBid'] - 0.04) if row['keywordBid'] >= 0.05 else row['keywordBid']
                bid_adjust = -0.04
                action_reason = "定义三：降低出价"

            elif 0.27 < row['ACOS_7d'] < 0.5 and row['ACOS_3d'] >= 0.27 and row['ACOS_30d'] > 0.5:
                new_keywordBid = max(0.05, row['keywordBid'] - 0.05) if row['keywordBid'] >= 0.05 else row['keywordBid']
                bid_adjust = -0.05
                action_reason = "定义四：降低出价"

            elif row['ACOS_7d'] > 0.5 and 0 < row['ACOS_30d'] < 0.27 and row['ACOS_3d'] >= 0.27:
                new_keywordBid = max(0.05, row['keywordBid'] - 0.05) if row['keywordBid'] >= 0.05 else row['keywordBid']
                bid_adjust = -0.05
                action_reason = "定义五：降低出价"

            elif row['total_sales14d_30d'] == 0 and row['total_cost_30d'] > 10 and row['total_clicks_30d'] > 20:
                new_keywordBid = 0.05
                bid_adjust = -1
                action_reason = "定义六：竞价设置为0.05"

            elif row['total_cost_7d'] > 5 and row['ACOS_30d'] > 0.5 and row['total_sales14d_7d'] == 0:
                new_keywordBid = 0.05
                bid_adjust = -1
                action_reason = "定义七：竞价设置为0.05"

            elif row['ACOS_7d'] > 0.5 and row['ACOS_30d'] > 0.3 and row['ACOS_30d'] >= 0.3:
                new_keywordBid = 0.05
                bid_adjust = -1
                action_reason = "定义八：竞价设置为0.05"

            elif row['ACOS_7d'] > 0.5 and row['total_sales14d_3d'] == 0 and row['total_cost_3d'] > 5:
                new_keywordBid = 0.05
                bid_adjust = -1
                action_reason = "定义九：竞价设置为0.05"

            elif 0.27 < row['ACOS_7d'] < 0.36 and row['total_sales14d_3d'] == 0:
                new_keywordBid = max(0.05, row['keywordBid'] - 0.01) if row['keywordBid'] >= 0.05 else row['keywordBid']
                bid_adjust = -0.01
                action_reason = "定义十：降低出价"

            elif 0.36 < row['ACOS_7d'] < 0.5 and row['total_sales14d_3d'] == 0:
                new_keywordBid = max(0.05, row['keywordBid'] - 0.02) if row['keywordBid'] >= 0.05 else row['keywordBid']
                bid_adjust = -0.02
                action_reason = "定义十一：降低出价"

            elif row['ACOS_7d'] <= 0.27 and 0.27 < row['ACOS_3d'] < 0.36:
                new_keywordBid = max(0.05, row['keywordBid'] - 0.02) if row['keywordBid'] >= 0.05 else row['keywordBid']
                bid_adjust = -0.02
                action_reason = "定义十二：更新出价"

            elif row['ACOS_7d'] <= 0.27 and row['ACOS_3d'] > 0.36:
                new_keywordBid = max(0.05, row['keywordBid'] - 0.03) if row['keywordBid'] >= 0.05 else row['keywordBid']
                bid_adjust = -0.03
                action_reason = "定义十三：更新出价"

            elif row['total_clicks_7d'] > 15 and row['total_cost_7d'] > 10 and row['total_sales14d_7d'] == 0 and 0.27 < row[
                'ACOS_30d'] < 0.5:
                new_keywordBid = 0.05
                bid_adjust = -1
                action_reason = "定义十四：竞价设置为0.05"

            # 如果关键词受到了调整或需要关闭，加进输出数据
            if new_keywordBid != row['keywordBid']:
                output_data.append({
                    'keyword': row['keyword'],
                    'keywordId': row['keywordId'],
                    'campaignName': row['campaignName'],
                    'adGroupName': row['adGroupName'],
                    'keywordBid': row['keywordBid'],
                    'new_keywordBid': new_keywordBid,
                    'ACOS_30d': row['ACOS_30d'],
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
                    'action_reason': action_reason,
                    'bid_adjust': bid_adjust
                })
        # 写出结果到CSV
    output_df = pd.DataFrame(output_data)
    output_df.replace({np.nan: None}, inplace=True)
    api = DbNewSpTools(brand,country)
    for index, row in output_df.iterrows():
        api.create_automatic_targeting_info(country, brand, '滞销品优化', '自动_劣质', row['keyword'], row['keywordId'],
                                row['campaignName'], row['adGroupName'], row['keywordBid'],
                                row['new_keywordBid'], row['ACOS_30d'], row['total_clicks_30d'],
                                row['total_sales14d_30d'], row['total_cost_30d'],None,
                                row['ACOS_7d'], row['total_clicks_7d'], row['total_sales14d_7d'], row['total_cost_7d'],
                                row['ACOS_3d'], row['total_sales14d_3d'], row['total_cost_3d'], row['action_reason'], row['bid_adjust'],
                                cur_time,
                                datetime.now(), 0)
    output_df.to_csv(output_file_path, index=False)

    print(f"结果已输出到文件：{output_file_path}")
