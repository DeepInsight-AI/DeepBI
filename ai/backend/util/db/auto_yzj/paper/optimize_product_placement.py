# filename: optimize_product_placement.py
import pandas as pd

# Load the CSV file
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\预处理.csv'
df = pd.read_csv(file_path)

# Define the functions for adjustment and closure reasons
def adjust_bid(keyword_bid, acos_7d):
    return keyword_bid / (((acos_7d - 0.24) / 0.24) + 1)

# Apply filter conditions
df_filtered = pd.DataFrame(columns=df.columns)

for index, row in df.iterrows():
    keyword_bid = row['keywordBid']
    acos_7d = row['ACOS_7d']
    acos_30d = row['ACOS_30d']
    clicks_7d = row['total_clicks_7d']
    sales_30d = row['total_sales14d_30d']
    sales_7d = row['total_sales14d_7d']
    total_cost_30d = row['total_cost_30d']

    new_bid = keyword_bid
    
    if 0.24 < acos_7d <= 0.5 and 0 < acos_30d <= 0.5:
        new_bid = adjust_bid(keyword_bid, acos_7d)
        close_reason = ''
        
    elif acos_7d > 0.5 and acos_30d <= 0.36:
        new_bid = adjust_bid(keyword_bid, acos_7d)
        close_reason = ''
        
    elif clicks_7d >= 10 and sales_7d == 0 and acos_30d <= 0.36:
        new_bid = keyword_bid - 0.04
        close_reason = ''
    
    elif clicks_7d > 10 and sales_7d == 0 and acos_30d > 0.5:
        new_bid = '关闭'
        close_reason = '点击数10以上无销售且ACOS>0.5'
        
    elif acos_7d > 0.5 and acos_30d > 0.36:
        new_bid = '关闭'
        close_reason = 'ACOS最近7天和30天均大于0.36'
    
    elif sales_30d == 0 and total_cost_30d >= 5:
        new_bid = '关闭'
        close_reason = '30天无销售且花费大于等于5'
    
    elif sales_30d == 0 and row['total_clicks_30d'] >= 15 and clicks_7d > 0:
        new_bid = '关闭'
        close_reason = '30天无销售点击15以上，且7天点击数大于0'
    
    else:
        continue  # Skip rows not matching any criteria
    
    row['New_keywordBid'] = new_bid
    row['Reason'] = close_reason
    df_filtered = df_filtered.append(row, ignore_index=True)

# Save the result to a new CSV file
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\商品投放优化\提问策略\手动_ASIN_劣质商品投放_v1_1_DELOMO_IT_2024-07-09.csv'
df_filtered.to_csv(output_file_path, index=False)

print('结果已保存至:', output_file_path)