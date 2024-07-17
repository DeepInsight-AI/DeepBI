# filename: process_keywords.py
import pandas as pd
import os
import numpy as np
from ai.backend.util.db.auto_process.tools_db_new_sp import DbNewSpTools
from datetime import datetime


def main(path, brand, cur_time, country):
    # Load the data
    file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\滞销品优化\自动sp广告\自动定位组优化\预处理.csv'
    file_name = "自动_劣质定位组" + '_' + brand + '_' + country + '_' + cur_time + '.csv'
    output_file_path = os.path.join(path, file_name)
    data = pd.read_csv(file_path)

    # Initialize an empty list to store the results
    results = []

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
    api = DbNewSpTools(brand)
    for index, row in results_df.iterrows():
        api.create_automatic_targeting_info(country, brand, '滞销品优化', '自动_劣质', row['keyword'], row['keywordId'],
                                row['campaignName'], row['adGroupName'], row['keywordBid'],
                                row['new_keywordBid'], row['ACOS_30d'], row['total_clicks_30d'],
                                row['total_sales14d_30d'], row['total_cost_30d'],
                                row['ACOS_7d'], row['total_clicks_7d'], row['total_sales14d_7d'], row['total_cost_7d'],
                                None, None, None, row['Reason'],
                                cur_time,
                                datetime.now(), 0)
    # Save the results to a new CSV file
    results_df.to_csv(output_file_path, index=False)

    print("Process finished and results saved.")
