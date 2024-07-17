# filename: search_term_analysis.py
import os
import pandas as pd
import numpy as np
from ai.backend.util.db.auto_process.tools_db_new_sp import DbNewSpTools
from datetime import datetime


def main(path, brand, cur_time, country):
    # 1. Load the data
    file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\滞销品优化\自动sp广告\搜索词优化\预处理.csv'
    file_name = "手动_劣质搜索词" + '_' + brand + '_' + country + '_' + cur_time + '.csv'
    output_file_path = os.path.join(path, file_name)
    df = pd.read_csv(file_path)

    # 2. Define the conditions
    condition_1 = (df['ACOS_30d'] > 0.35) & (df['ORDER_1m'] < 3)
    condition_2 = (df['total_clicks_30d'] > 20) & (df['ORDER_1m'] == 0)
    condition_3 = (df['total_clicks_7d'] > 13) & (df['ORDER_7d'] == 0)

    # 3. Filtering and adding reasons
    filtered_df = df[condition_1 | condition_2 | condition_3]
    filtered_df['reason'] = ''  # Initializing the reason column

    # Add reasons based on conditions
    filtered_df.loc[condition_1, 'reason'] += '定义一 '
    filtered_df.loc[condition_2, 'reason'] += '定义二 '
    filtered_df.loc[condition_3, 'reason'] += '定义三 '

    # 4. Select the required columns
    selected_columns = ['campaignName', 'campaignId', 'adGroupName', 'adGroupId', 'total_clicks_7d',
                        'ACOS_7d', 'ORDER_7d', 'total_clicks_30d', 'ORDER_1m', 'ACOS_30d', 'searchTerm', 'reason']

    result_df = filtered_df[selected_columns]
    result_df.replace({np.nan: None}, inplace=True)
    api = DbNewSpTools(brand)
    for index, row in result_df.iterrows():
        api.create_search_term_info(country, brand, '滞销品优化', '自动_劣质', row['campaignName'],row['campaignId'],
                               row['adGroupName'], row['adGroupId'], row['ACOS_30d'],
                               row['ORDER_1m'],row['total_clicks_30d'],None, None, row['ORDER_7d'],row['total_clicks_7d'], None,
                               None, row['searchTerm'],None, row['reason'], cur_time,
                               datetime.now(), 0)
    # 5. Save to a new CSV
    result_df.to_csv(output_file_path, index=False)

    print("Data has been successfully filtered and saved to the new CSV file.")
