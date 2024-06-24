# filename: optimize_ads.py
import pandas as pd

# Step 1: Load the data
file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\预处理.csv"
df = pd.read_csv(file_path)

# Step 2: Calculate the average ACOS for the last 7 days and 3 days
df['avg_ACOS_7d'] = df['total_cost_7d'] / df['total_sales14d_7d']
df['avg_ACOS_3d'] = df['total_cost_3d'] / df['total_sales14d_3d']

# Step 3: Function to find the best ad placements based on the criteria provided
def find_best_placements(group):
    df_group = group.copy()
    best_7d = df_group[(df_group['avg_ACOS_7d'] > 0) & (df_group['avg_ACOS_7d'] <= 0.24)]
    best_3d = df_group[(df_group['avg_ACOS_3d'] > 0) & (df_group['avg_ACOS_3d'] <= 0.24)]
    
    if best_7d.empty or best_3d.empty:
        return None
    
    best_7d_placement = best_7d.loc[best_7d['avg_ACOS_7d'].idxmin()]
    best_3d_placement = best_3d.loc[best_3d['avg_ACOS_3d'].idxmin()]
    
    if (best_7d_placement['total_clicks_7d'] != df_group['total_clicks_7d'].max() and
        best_3d_placement['total_clicks_3d'] != df_group['total_clicks_3d'].max()):
        
        result = best_3d_placement.copy()
        result['新竞价调整'] = "提高5%到最终50%"
        result['原因'] = "最近7天和最近3天 ACOS 最低，点击数不是最大的，提高竞价"
        return result
    return None

# Step 4: Apply the function to each campaign group
result_df = df.groupby('campaignName').apply(find_best_placements).dropna().reset_index(drop=True)

# Step 5: Select the relevant columns to be saved in the output file
output_columns = ['campaignName', 'campaignId', 'placementClassification', 'avg_ACOS_7d', 'avg_ACOS_3d', 'total_clicks_7d', 'total_clicks_3d', '新竞价调整', '原因']

# Step 6: Save the result to a new CSV file at the specified location
output_file_path = r"C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\提问策略\自动_优质广告位_v1_1_IT_2024-06-13.csv"
result_df.to_csv(output_file_path, index=False, columns=output_columns)

print("Finished processing and the result has been saved to the specified path.")