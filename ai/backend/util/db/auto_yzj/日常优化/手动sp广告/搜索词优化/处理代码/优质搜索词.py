# filename: analyze_search_terms.py
import pandas as pd
import os


def main(path, cur_time, country):
    # Load the CSV file
    file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\搜索词优化\预处理.csv"
    file_name = "手动_优质搜索词" + '_' + country + '_' + cur_time + '.csv'
    output_file_path = os.path.join(path, file_name)
    data = pd.read_csv(file_path)

    # Apply Definition 1
    def1 = data[(data['total_sales14d_7d'] > 0) & (data['ACOS_7d'] < 0.2)]
    def1['reason'] = "定义一"

    # Apply Definition 2
    def2 = data[(data['ORDER_1m'] >= 2) & (data['ACOS_30d'] < 0.24)]
    def2['reason'] = "定义二"

    # Combine the results
    result = pd.concat([def1, def2]).drop_duplicates(subset=['campaignId', 'adGroupId', 'searchTerm'])

    # Select relevant columns for output
    output_columns = [
        'campaignName', 'campaignId', 'adGroupName', 'adGroupId',
        'ACOS_7d', 'total_sales14d_7d', 'ORDER_1m', 'ACOS_30d',
        'searchTerm', 'reason'
    ]
    output = result[output_columns]

    # Save the result to a CSV file
    output.to_csv(output_file_path, index=False)

    print("Data has been successfully processed and saved to:", output_file_path)


