# filename: detect_acos_anomalies.py
import os
import pandas as pd
import numpy as np
from ai.backend.util.db.auto_process.tools_db_new_sp import DbNewSpTools
from ai.backend.util.db.auto_process.summary.db_tool.tools_db import AmazonMysqlRagUitl
from datetime import datetime


def main(path, brand, cur_time, country):
    # 读取CSV文件
    file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告活动\预处理.csv'
    file_name = "异常检测_campaign_ACOS异常好" + '_' + brand + '_' + country + '_' + cur_time + '.csv'
    output_file_path = os.path.join(path, file_name)
    data = pd.read_csv(file_path)

    # 过滤符合异常条件的记录
    anomalies = data[(data['purchases'] >= 2) & (data['ACOS'] < 0.1)]

    # 创建异常原因描述
    anomalies['原因'] = f"昨天订单数为{anomalies['purchases']}，ACOS为{anomalies['ACOS']}"

    # 选择需要输出的字段
    output_data = anomalies[['campaignName', '原因']]

    # 输出结果到新的CSV文件
    output_data.to_csv(output_file_path, index=False, encoding='utf-8')

    print(f"Anomaly detection completed and the results are saved to {output_file_path}.")

#main('C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/输出结果/LAPASA_FR_2024-07-23','LAPASA','2024-07-23','FR')
