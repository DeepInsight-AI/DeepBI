# filename: anomaly_detection.py
import os
import pandas as pd
import numpy as np
from ai.backend.util.db.auto_process.tools_db_new_sp import DbNewSpTools
from ai.backend.util.db.auto_process.summary.db_tool.tools_db import AmazonMysqlRagUitl
from datetime import datetime


def main(path, brand, cur_time, country):
    # Step 1: 读取CSV数据
    file_path = 'C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/异常定位检测/商品/预处理.csv'
    file_name = "异常检测_商品_异常" + '_' + brand + '_' + country + '_' + cur_time + '.csv'
    output_file_path = os.path.join(path, file_name)
    data = pd.read_csv(file_path)


    # Step 2: Group by advertisedSku and perform anomaly checks
    anomalies = []

    for sku in data['advertisedSku'].unique():
        sku_data = data[data['advertisedSku'] == sku]

        clicks_30d = sku_data['clicks30d'].sum()
        sales_30d = sku_data['sales30d'].sum()
        purchases_30d = sku_data['purchases30d'].sum()
        cost_7d = sku_data['cost30d'].sum()
        sales_7d = sku_data['sales30d'].sum()
        acos_7d = sku_data['ACOS'].max()  # Assuming there is one ACOS value per record

        reason = []

        if clicks_30d > 13 and sales_30d == 0:
            reason.append('最近30天clicks > 13但无销售额')
        if purchases_30d >= 8 and sales_7d == 0:
            reason.append('最近30天订单数 >= 8，但近7天无销售额')
        if cost_7d >= 5 and sales_7d == 0:
            reason.append('近7天花费 >= 5，但无销售额')
        if acos_7d > 0.36:
            reason.append('近7天ACOS值 > 0.36')

        if reason:
            anomalies.append({
                "advertisedSku": sku,
                "campaignName": sku_data['campaignName'].iloc[0],
                "adGroupName": sku_data['adGroupName'].iloc[0],
                "reason": "; ".join(reason)
            })

    # Step 3: 输出结果到CSV文件
    if anomalies:
        output_df = pd.DataFrame(anomalies)
        output_df.to_csv(output_file_path, index=False)
        print(f"Anomalies saved to {output_file_path}")
    else:
        print("No anomalies found.")
