# filename: increase_campaign_budget.py
import pandas as pd

def main():
    try:
        # Load the data from the CSV file
        file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\预处理.csv'
        data = pd.read_csv(file_path)

        # Define the conditions for well-performing campaigns
        condition_7d_acos = data['ACOS_7d'] < 0.24
        condition_yesterday_acos = data['ACOS_yesterday'] < 0.24
        condition_yesterday_cost = data['cost_yesterday'] > (0.8 * data['Budget'])

        # Combine conditions to identify qualifying campaigns
        condition = condition_7d_acos & condition_yesterday_acos & condition_yesterday_cost

        # Create a filtered dataframe with only the well-performing campaigns
        filtered_data = data[condition].copy()

        # Adjust the budget
        def adjust_budget(row):
            new_budget = min(row['Budget'] * 1.2, 50)
            return new_budget

        filtered_data['New_Budget'] = filtered_data.apply(adjust_budget, axis=1)

        # Add the reason for budget increase
        filtered_data['Reason'] = (
            'ACOS_7d < 0.24, ACOS_yesterday < 0.24, cost_yesterday > 0.8 * Budget'
        )

        # Select the required columns for the output
        output_columns = [
            'campaignId', 
            'campaignName', 
            'Budget', 
            'New_Budget', 
            'cost_yesterday', 
            'clicks_yesterday', 
            'ACOS_yesterday', 
            'ACOS_7d',
            'ACOS_30d',
            'total_clicks_30d',
            'total_sales14d_30d',
            'Reason'
        ]
        output_df = filtered_data[output_columns]

        # Save the result to a new CSV file
        output_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\手动sp广告\预算优化\提问策略\手动_优质广告活动_v1_1_ES_2024-06-14.csv'
        output_df.to_csv(output_file_path, index=False)

        print("The CSV file has been created successfully with the well-performing campaign information.")
    
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()