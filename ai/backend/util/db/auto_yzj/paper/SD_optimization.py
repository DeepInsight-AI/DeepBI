# filename: SD_optimization.py

import pandas as pd

# Load the CSV file
file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\预处理.csv'
try:
    data = pd.read_csv(file_path)
    print("CSV file loaded successfully.")
except Exception as e:
    print("Error loading CSV file:", e)
    exit(1)

# Filter definitions function
def filter_conditions(data):
    try:
        # Condition for 定义一
        condition_1 = (
            (data['ACOS7d'] > 0.24) &
            (data['ACOSYesterday'] > 0.24) &
            (data['costYesterday'] > 5.5) &
            (data['ACOS30d'] > data['countryAvgACOS1m'])
        )
        
        data_condition_1 = data[condition_1].copy()
        data_condition_1['Reason'] = '定义一'
        
        # Adjust budget based on 定义一
        data_condition_1['New Budget'] = data_condition_1['campaignBudget'].apply(lambda x: max(x-5, 8) if x > 13 else x)
        
        # Condition for 定义二
        condition_2 = (
            (data['ACOS30d'] > 0.24) &
            (data['ACOS30d'] > data['countryAvgACOS1m']) &
            (data['totalSales7d'] == 0) &
            (data['totalCost7d'] > 10)
        )
        
        data_condition_2 = data[condition_2].copy()
        data_condition_2['Reason'] = '定义二'
        
        # Adjust budget based on 定义二
        data_condition_2['New Budget'] = data_condition_2['campaignBudget'].apply(lambda x: max(x-5, 5))
        
        # Combine the filtered data
        combined_data = pd.concat([data_condition_1, data_condition_2])
        
        return combined_data
    
    except Exception as e:
        print("Error during filtering conditions:", e)
        exit(1)

# Filter the data based on the conditions
try:
    filtered_data = filter_conditions(data)
    print("Data filtered successfully.")
except Exception as e:
    print("Error filtering data:", e)
    exit(1)

# Select necessary columns for output
output_columns = [
    'campaignId', 'campaignName', 'campaignBudget', 'New Budget',
    'clicksYesterday', 'ACOSYesterday', 'ACOS7d', 'totalClicks7d',
    'totalSales7d', 'ACOS30d', 'totalClicks30d', 'totalSales30d',
    'countryAvgACOS1m', 'Reason'
]

try:
    output_data = filtered_data[output_columns]
    print("Selected necessary columns successfully.")
except Exception as e:
    print("Error selecting columns:", e)
    exit(1)

# Define output path
output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\sd广告\预算优化\提问策略\SD_劣质sd广告活动_v1_1_LAPASA_DE_2024-07-15.csv'

# Save to CSV
try:
    output_data.to_csv(output_file_path, index=False, encoding='utf-8-sig')
    print("Filtered and adjusted ad campaigns saved to:", output_file_path)
except Exception as e:
    print("Error saving to CSV file:", e)
    exit(1)