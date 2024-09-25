# filename: detect_anomalies.py
import os
import pandas as pd
import numpy as np
from ai.backend.util.db.auto_process.tools_db_new_sp import DbNewSpTools
from ai.backend.util.db.auto_process.summary.db_tool.tools_db import AmazonMysqlRagUitl
from datetime import datetime


def main(path, brand, cur_time, country):
    # Define the file paths
    input_file = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告位\预处理.csv'
    file_name = "异常检测_placement_异常" + '_' + brand + '_' + country + '_' + cur_time + '.csv'
    output_file_path = os.path.join(path, file_name)

    # Read the CSV file
    df = pd.read_csv(input_file)

    # Replace missing ACOS values (assuming missing values mean infinity)
    df['ACOS'].replace(np.nan, np.inf, inplace=True)

    # Filter data for anomalies
    anomalies = df[((df['ACOS'] > 0.24) | (df['ACOS'] == np.inf)) & (df['bid'] > 0)]

    # Prepare the output data with an additional column for the reason
    anomalies['reason'] = anomalies.apply(
        lambda row: f"Yesterday ACOS value was {row['ACOS']}, but placement bid was {row['bid']}", axis=1
    )

    # Select the required columns
    output_data = anomalies[['campaignName', 'placementClassification', 'reason']]

    # Save the output to CSV
    output_data.to_csv(output_file_path, index=False)

    print("Anomaly detection completed and results saved.")
