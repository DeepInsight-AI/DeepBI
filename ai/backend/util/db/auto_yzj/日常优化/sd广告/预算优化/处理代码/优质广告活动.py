import os
import pandas as pd
import numpy as np
from ai.backend.util.db.auto_process.tools_db_new_sp import DbNewSpTools
from ai.backend.util.db.auto_process.summary.db_tool.tools_db import AmazonMysqlRagUitl
from datetime import datetime


def main(path, brand, cur_time, country):
    # 加载数据集
    file_path = "C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/sd广告/预算优化/预处理.csv"
    file_name = "SD_优质广告sd活动" + '_' + brand + '_' + country + '_' + cur_time + '.csv'
    output_file_path = os.path.join(path, file_name)
    data = pd.read_csv(file_path)

    # 过滤符合条件的广告活动
    filtered_data = data[
        (data['ACOS7d'] < 0.24) &
        (data['ACOSYesterday'] < 0.24) &
        (data['costYesterday'] > 0.8 * data['campaignBudget'])
    ]

    # 计算新的预算并生成输出所需的列
    filtered_data['New_Budget'] = filtered_data['campaignBudget'] + 5
    filtered_data['New_Budget'] = filtered_data['New_Budget'].round(2)
    filtered_data['bid_adjust'] = 5

    filtered_data['Reason'] = '定义一'

    # 选择导出列
    output_columns = [
        'campaignId', 'campaignName', 'campaignBudget', 'New_Budget', 'costYesterday',
        'ACOSYesterday', 'ACOS7d', 'Reason', 'bid_adjust'
    ]

    output_data = filtered_data[output_columns]
    output_data.replace({np.nan: None}, inplace=True)
    api = DbNewSpTools(brand,country)
    for index, row in output_data.iterrows():
        api.create_budget_info(country, brand, '日常优化', 'SD_优质', row['campaignId'], row['campaignName'],
                               row['campaignBudget'], row['New_Budget'], row['costYesterday'], None,
                               row['ACOSYesterday'], None, None, row['ACOS7d'], None,
                               None, None, row['Reason'], None,row['bid_adjust'], cur_time,
                               datetime.now(), 0)
    # 保存到新的CSV文件
    # 保存结果到CSV文件
    output_data.to_csv(output_file_path, index=False)

    print("任务完成，数据已保存至：", output_file_path)


#main('C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/auto_yzj/日常优化/输出结果/LAPASA_US_2024-07-18','LAPASA','2024-07-18','US')
