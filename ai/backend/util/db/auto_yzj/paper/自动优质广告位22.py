# filename: increase_bid_based_on_performance.py

import pandas as pd

# Load data
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\预处理.csv'
data = pd.read_csv(file_path)

# Definition for identifying good performing ad placements
def is_good_performance(ad_group):
    conditions_met = (
        # Minimal ACOS conditions
        ad_group['ACOS_7d'].min() > 0 and ad_group['ACOS_7d'].min() <= 0.24 and
        ad_group['ACOS_3d'].min() > 0 and ad_group['ACOS_3d'].min() <= 0.24 and 
        # Lowest ACOS but not highest clicks within the group
        ad_group.loc[ad_group['ACOS_7d'].idxmin(), 'total_clicks_7d'] != ad_group['total_clicks_7d'].max() and
        ad_group.loc[ad_group['ACOS_3d'].idxmin(), 'total_clicks_3d'] != ad_group['total_clicks_3d'].max()
    )
    return conditions_met

def calculate_new_bid(bid, increase_percent=5, max_increase_percent=50):
    new_bid = bid * (1 + increase_percent / 100)
    max_bid = bid * (1 + max_increase_percent / 100)
    return min(new_bid, max_bid)

good_performance_ads = []

# Group by campaignId and check for good performing ads
grouped_data = data.groupby('campaignId')
for campaignId, ad_group in grouped_data:
    if is_good_performance(ad_group):
        min_acos_ad_7d_idx = ad_group['ACOS_7d'].idxmin()
        min_acos_ad_3d_idx = ad_group['ACOS_3d'].idxmin()
        
        # Use idx for both 7d, and 3d as identified above
        if min_acos_ad_7d_idx == min_acos_ad_3d_idx:
            ad = ad_group.loc[min_acos_ad_7d_idx]
            new_bid = calculate_new_bid(ad['bid'])
            ad_details = {
                'campaignName': ad['campaignName'],
                'campaignId': ad['campaignId'],
                'placementClassification': ad['placementClassification'],
                'ACOS_7d': ad['ACOS_7d'],
                'ACOS_3d': ad['ACOS_3d'],
                'total_clicks_7d': ad['total_clicks_7d'],
                'total_clicks_3d': ad['total_clicks_3d'],
                'bid': ad['bid'],
                'new_bid': new_bid,
                'reason': 'High performance as per specified conditions'
            }
            good_performance_ads.append(ad_details)

# Create a DataFrame for results
result_df = pd.DataFrame(good_performance_ads)

# Save to CSV
output_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\自动sp广告\广告位优化\提问策略\自动_优质广告位_v1_1_ES_2024-06-21.csv'
result_df.to_csv(output_path, index=False)
print("Ad positions with good performance have been identified and saved to the specified CSV file.")