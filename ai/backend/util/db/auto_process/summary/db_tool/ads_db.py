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

    def get_scan_campaign_sp(self, market, date):
        try:
            conn = self.conn

            query1 = """
SELECT COUNT(*) as count
FROM amazon_campaign_reports_sp
WHERE date = '{}'- INTERVAL 1 DAY
AND campaignStatus = 'ENABLED'
AND market = '{}';
                    """.format(date, market)
            df1 = pd.read_sql(query1, con=conn)
            count = df1.loc[0, 'count']
            # return df
            return count
        except Exception as e:
            print("Error while get_new_create_campaign:", e)

    def get_scan_campaign_sd(self, market, date):
        try:
            conn = self.conn

            query1 = """
SELECT COUNT(*) as count
FROM amazon_campaign_reports_sd
WHERE date = '{}'- INTERVAL 1 DAY
AND campaignStatus = 'ENABLED'
AND market = '{}';
                    """.format(date, market)
            df1 = pd.read_sql(query1, con=conn)
            count = df1.loc[0, 'count']
            # return df
            return count
        except Exception as e:
            print("Error while get_new_create_campaign:", e)

    def get_scan_campaign_sb(self, market, date):
        try:
            conn = self.conn

            query1 = """
SELECT COUNT(*) as count
FROM amazon_campaign_reports_sb
WHERE date = '{}'- INTERVAL 1 DAY
AND campaignStatus = 'ENABLED'
AND market = '{}';
                    """.format(date, market)
            df1 = pd.read_sql(query1, con=conn)
            count = df1.loc[0, 'count']
            # return df
            return count
        except Exception as e:
            print("Error while get_new_create_campaign:", e)
