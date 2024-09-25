# filename: analyze_search_terms.py
import pandas as pd
import os
import numpy as np
from ai.backend.util.db.auto_process.tools_db_new_sp import DbNewSpTools
from datetime import datetime
from ai.backend.util.db.auto_process.tools_db_sp import DbSpTools


def main(path, brand, cur_time, country, db, version=2):
    # Load the CSV file
    file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\搜索词优化\预处理.csv"
    file_name = "自动_优质自动搜索词" + '_' + brand + '_' + country + '_' + cur_time + '.csv'
    output_file_path = os.path.join(path, file_name)
    data = pd.read_csv(file_path)

    if version == 1:
        # Apply Definition 1
        def1 = data[(data['total_sales14d_7d'] > 0) & (data['ACOS_7d'] <= 0.2)]
        def1['reason'] = "定义一"

        # Apply Definition 2
        def2 = data[(data['ORDER_1m'] >= 2) & (data['ACOS_30d'] < 0.24)]
        def2['reason'] = "定义二"

        # Combine the results
        result = pd.concat([def1, def2]).drop_duplicates(subset=['campaignId', 'adGroupId', 'searchTerm'])
    elif version == 2:
        # Apply Definition 1
        def1 = data[(data['ACOS_30d'] < 0.24)]
        def1['reason'] = "定义一"

        # Combine the results
        result = pd.concat([def1]).drop_duplicates(subset=['campaignId', 'adGroupId', 'searchTerm'])
    result['new_campaignId'] = ''
    result['new_campaignName'] = ''
    result['new_adGroupId'] = ''
    api2 = DbSpTools(db, brand,country)
    for index, row in result.iterrows():
        if len(row['searchTerm']) == 10 and row['searchTerm'].startswith('b0'):
            new_campaignId,new_campaignName,new_adGroupId = api2.select_sp_asin_campaignid_search_term_jiutong(cur_time,row['campaignId'])
        else:
            new_campaignId,new_campaignName,new_adGroupId = api2.select_sp_campaignid_search_term_jiutong(cur_time,row['campaignId'])
        print(new_campaignId)
        print(new_campaignName)
        print(new_adGroupId)
        result.loc[index, 'new_campaignId'] = new_campaignId
        result.loc[index, 'new_campaignName'] = new_campaignName
        result.loc[index, 'new_adGroupId'] = new_adGroupId

    # Select relevant columns for output
    output_columns = [
        'campaignName', 'campaignId', 'adGroupName', 'adGroupId',
        'ACOS_7d', 'total_sales14d_7d', 'ORDER_1m', 'ACOS_30d','total_clicks_30d',
        'total_cost_30d', 'CPC_30d',
        'searchTerm', 'reason', 'new_campaignId', 'new_campaignName',
        'new_adGroupId'
    ]
    output = result[output_columns]
    output.replace({np.nan: None}, inplace=True)
    api = DbNewSpTools(db,brand,country)
    for index, row in output.iterrows():
        api.create_search_term_info(country, brand, '日常优化', '自动_优质', row['campaignName'],row['campaignId'],
                               row['adGroupName'], row['adGroupId'], row['ACOS_30d'],
                               row['ORDER_1m'],None,None, row['ACOS_7d'], None,None, row['total_sales14d_7d'],
                               None, row['searchTerm'],row['new_campaignId'], row['reason'], cur_time,
                               datetime.now(), 0)
    # Save the result to a CSV file
    output.to_csv(output_file_path, index=False)

    print("Data has been successfully processed and saved to:", output_file_path)

#main('C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/输出结果/LAPASA_US_2024-07-16','LAPASA','2024-07-16','US')
