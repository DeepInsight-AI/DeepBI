# filename: detect_acos_anomalies_v4.py
import pandas as pd
import os

def main():
    try:
        print("Step 1: Checking file path")
        # Step 1: Ensure the file path exists and is accessible
        file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告活动\预处理.csv'
        if not os.path.isfile(file_path):
            print(f"File path does not exist: {file_path}")
            return
        print("File path exists and accessible")

        # Attempt to read the data from CSV file
        data = pd.read_csv(file_path)
        print("Step 1: Data successfully read from CSV file")
    except Exception as e:
        print(f"Failed to read data from CSV file: {e}")
        return

    try:
        print("Step 2: Defining anomaly detection function")
        # Step 2: Define a function to detect anomalies
        def detect_anomalies(row):
            anomalies = []

            # Extract necessary values
            try:
                acos_yesterday = row['ACOS_yesterday']
                acos_7d = row['ACOS_7d']
                acos_30d = row['ACOS_30d']
            except KeyError as e:
                print(f"Missing key in data row: {e}")
                return []

            # Check if yesterday's ACOS is empty or zero
            if pd.isna(acos_yesterday) or acos_yesterday == 0:
                if acos_7d < 25:
                    anomalies.append('Yesterday no sales, ACOS in last 7 days is very good')
                elif acos_30d < 25:
                    anomalies.append('Yesterday no sales, ACOS in last 30 days is very good')

                if acos_7d < 20:
                    anomalies.append('Yesterday no sales, ACOS in last 7 days is extremely good')
                elif acos_30d < 20:
                    anomalies.append('Yesterday no sales, ACOS in last 30 days is extremely good')
            else:
                if acos_7d != 0 and abs(acos_yesterday - acos_7d) / acos_7d > 0.30:
                    anomalies.append('Yesterday ACOS deviated more than 30% from last 7 days average ACOS')
                
                if acos_30d != 0 and abs(acos_yesterday - acos_30d) / acos_30d > 0.30:
                    anomalies.append('Yesterday ACOS deviated more than 30% from last 30 days average ACOS')
            
            return anomalies

        print("Step 2: Anomaly detection function defined")
    except Exception as e:
        print(f"Failed to define the anomaly detection function: {e}")
        return

    try:
        print("Step 3: Applying anomaly detection to each row")
        # Step 3: Apply the anomaly detection to each row
        data['Anomalies'] = data.apply(detect_anomalies, axis=1)
        filtered_data = data.explode('Anomalies').dropna(subset=['Anomalies'])
        print("Step 3: Anomaly detection applied to each row")
    except Exception as e:
        print(f"Failed to apply anomaly detection: {e}")
        return

    try:
        print("Step 4: Preparing data for output")
        # Step 4: Preparing data for output
        output_data = filtered_data[['campaignId', 'campaignName', 'Anomalies', 'ACOS_yesterday', 'ACOS_7d', 'ACOS_30d']]
        print("Step 4: Data prepared for output")
    except Exception as e:
        print(f"Failed to prepare data for output: {e}")
        return

    try:
        print("Step 5: Verifying output directory")
        # Step 5: Save the output to a new CSV file
        output_directory = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告活动\提问策略'
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)
            print(f"Output directory created: {output_directory}")
        else:
            print("Output directory exists")

        output_file_path = os.path.join(output_directory, '异常检测_广告活动_ACOS异常_v1_0_LAPASA_US_2024-07-14.csv')
        output_data.to_csv(output_file_path, index=False)
        print(f"Step 5: Anomaly detection complete. Results saved to {output_file_path}")
    except Exception as e:
        print(f"Failed to save the output to CSV file: {e}")

if __name__ == "__main__":
    main()