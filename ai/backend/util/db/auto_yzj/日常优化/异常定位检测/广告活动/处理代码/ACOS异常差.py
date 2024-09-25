# filename: detect_acos_anomaly.py
import os
import pandas as pd
import numpy as np
from ai.backend.util.db.auto_process.tools_db_new_sp import DbNewSpTools
from ai.backend.util.db.auto_process.summary.db_tool.tools_db import AmazonMysqlRagUitl
from datetime import datetime


def main(path, brand, cur_time, country):
    # 读取CSV文件
    csv_file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告活动\预处理.csv'
    file_name = "异常检测_campaign_ACOS异常差" + '_' + brand + '_' + country + '_' + cur_time + '.csv'
    output_file_path = os.path.join(path, file_name)
    data = pd.read_csv(csv_file_path)

    # 筛选出ACOS异常的广告活动
    anomaly_data = data[data['ACOS'] > 0.36].copy()

    # 添加异常原因列
    anomaly_data['异常原因'] = '昨天ACOS值为' + anomaly_data['ACOS'].astype(str)

    # 选取需要的列
    result_data = anomaly_data[['campaignName', '异常原因']]

    # 保存结果到CSV文件
    result_data.to_csv(output_file_path, index=False)

    print("异常检测处理完毕并已保存到文件中。")
