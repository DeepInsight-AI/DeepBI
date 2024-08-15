import json
import os

import pymysql
import pandas as pd
from datetime import datetime
import warnings
from ai.backend.util.db.configuration.path import get_config_path

# 忽略特定类型的警告
warnings.filterwarnings("ignore", category=UserWarning)


def get_timestamp():
    # 获取当前时间
    current_time = datetime.now()
    timestamp = int(current_time.timestamp())
    date_string = current_time.strftime("%Y-%m-%d")
    # 组合日期和时间戳
    date_timestamp_string = f"{date_string}_{timestamp}"
    return date_timestamp_string

class AmazonMysqlRagUitl:

    def __init__(self, brand,market):
        self.db_info = self.load_db_info(brand,market)
        self.conn = self.connect(self.db_info)

    def load_db_info(self, brand, country=None):
        # 从 JSON 文件加载数据库信息
        db_info_path = os.path.join(get_config_path(), 'db_info.json')
        with open(db_info_path, 'r') as f:
            db_info_json = json.load(f)

        if brand not in db_info_json:
            raise ValueError(f"Unknown brand '{brand}'")

        brand_info = db_info_json[brand]

        if country and country in brand_info:
            return brand_info[country]

        return brand_info.get('default', {})

    def connect(self, db_info):
        try:
            conn = pymysql.connect(**db_info)
            print("Connected to amazon_mysql database!")
            return conn
        except Exception as error:
            print("Error while connecting to amazon_mysql:", error)
            return None

    def connect_close(self):
        try:
            self.conn.close()
        except Exception as error:
            print("Error while connecting to amazon_mysql:", error)
            return None

    def get_select_campaign(self, query, date, bid_adjust):
        try:
            conn = self.conn
            df3 = pd.read_sql(query, con=conn)
            df3['bid_adjust'] = bid_adjust
            output_filename = f'{date}_select_campaign.csv'
            df3.to_csv(output_filename, index=False, encoding='utf-8-sig')
            # return df
            return output_filename

        except Exception as error:
            print("Error while inserting data:", error)
