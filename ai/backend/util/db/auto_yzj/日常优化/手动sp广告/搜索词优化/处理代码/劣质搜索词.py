# filename: search_term_analysis.py
import os
import pandas as pd


def main(path, cur_time, country):
    # 1. Load the data
    file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\搜索词优化\预处理.csv'
    file_name = "手动_劣质搜索词" + '_' + country + '_' + cur_time + '.csv'
    output_file_path = os.path.join(path, file_name)
    df = pd.read_csv(file_path)

    # 2. Define the conditions
    condition_1 = (df['ACOS_30d'] > 0.24) & (df['ACOS_30d'] < 0.36) & (df['ORDER_1m'] <= 5)
    condition_2 = (df['ACOS_30d'] >= 0.36) & (df['ORDER_1m'] <= 8)
    condition_3 = (df['total_clicks_30d'] > 13) & (df['ORDER_1m'] == 0)
    condition_4 = (df['ACOS_7d'] > 0.24) & (df['ACOS_7d'] < 0.36) & (df['ORDER_7d'] <= 3)
    condition_5 = (df['ACOS_7d'] >= 0.36) & (df['ORDER_7d'] <= 5)
    condition_6 = (df['total_clicks_7d'] > 10) & (df['ORDER_7d'] == 0)

    # 3. Filtering and adding reasons
    filtered_df = df[condition_1 | condition_2 | condition_3 | condition_4 | condition_5 | condition_6]
    filtered_df['reason'] = ''  # Initializing the reason column

    # Add reasons based on conditions
    filtered_df.loc[condition_1, 'reason'] += '定义一 '
    filtered_df.loc[condition_2, 'reason'] += '定义二 '
    filtered_df.loc[condition_3, 'reason'] += '定义三 '
    filtered_df.loc[condition_4, 'reason'] += '定义四 '
    filtered_df.loc[condition_5, 'reason'] += '定义五 '
    filtered_df.loc[condition_6, 'reason'] += '定义六 '

    # 4. Select the required columns
    selected_columns = ['campaignName', 'campaignId', 'adGroupName', 'adGroupId', 'total_clicks_7d',
                        'ACOS_7d', 'ORDER_7d', 'total_clicks_30d', 'ORDER_1m', 'ACOS_30d', 'searchTerm', 'reason']

    result_df = filtered_df[selected_columns]

    # 5. Save to a new CSV
    result_df.to_csv(output_file_path, index=False)

    print("Data has been successfully filtered and saved to the new CSV file.")
