# filename: detect_campaign_anomalies.py
import os
import pandas as pd
import numpy as np
from ai.backend.util.db.auto_process.tools_db_new_sp import DbNewSpTools
from ai.backend.util.db.auto_process.summary.db_tool.tools_db import AmazonMysqlRagUitl
from datetime import datetime


def main(path, brand, cur_time, country):
    # 定义文件路径
    file_path = r'C:\Users\admin\PycharmProjects\DeepBI\ai\backend\util\db\auto_yzj\日常优化\异常定位检测\广告活动\预处理.csv'
    file_name = "异常检测_campaign_花费异常" + '_' + brand + '_' + country + '_' + cur_time + '.csv'
    output_file_path = os.path.join(path, file_name)


    # 加载数据
    data = pd.read_csv(file_path)

    # 初始化异常列表
    anomalies = []

    # 检查每条广告活动的异常情况
    for index, row in data.iterrows():
        reasons = []
        if row['cost'] > 5 and row['sales'] == 0:
            reasons.append(f"花费为{row['cost']}但销售额为0")
        if row['cost'] > row['campaignBudgetAmount']:
            reasons.append(f"花费{row['cost']}超出预算{row['campaignBudgetAmount']}")

        if reasons:
            anomalies.append({
                "campaignName": row['campaignName'],
                "异常原因": "，".join(reasons)
            })

    # 输出结果到CSV文件
    anomalies_df = pd.DataFrame(anomalies)
    anomalies_df.to_csv(output_file_path, index=False, encoding='utf-8-sig')

    print("异常数据已保存到CSV文件中。")
