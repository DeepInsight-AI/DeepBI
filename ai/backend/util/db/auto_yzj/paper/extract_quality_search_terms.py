# filename: extract_quality_search_terms.py

import pandas as pd

def main():
    # Load the dataset
    file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\搜索词优化\预处理.csv"
    df = pd.read_csv(file_path)

    # Filter data based on criteria
    filtered_df = df[(df['total_sales14d_7d'] > 0) & (df['ACOS_7d'] < 0.2)]

    # Select necessary columns for output
    result_df = filtered_df[['campaignName', 'campaignId', 'adGroupName', 'adGroupId', 'searchTerm', 'matchType', 'ACOS_7d']]
    result_df = result_df.rename(columns={'ACOS_7d': 'week_acos'})

    # Add a reason column
    result_df['reason'] = '近七天有销售额且ACOS值低于0.2'

    # Define the output file path
    output_file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\搜索词优化\提问策略\手动_优质搜索词_v1_1_ES_2024-06-121.csv"

    # Save to CSV
    result_df.to_csv(output_file_path, index=False)

    print("Filtered data has been saved to:", output_file_path)

if __name__ == "__main__":
    main()