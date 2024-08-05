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


class CheckRecordsWithin24Hours:

    def __init__(self, brand):
        self.db_info = self.load_db_info(brand)
        self.conn = self.connect(self.db_info)

    def load_db_info(self, brand):
        # 从 JSON 文件加载数据库信息
        db_info_log_path = os.path.join(get_config_path(), 'db_info_log.json')
        with open(db_info_log_path, 'r') as f:
            db_info_json = json.load(f)

        if brand in db_info_json:
            return db_info_json[brand]
        else:
            raise ValueError(f"Unknown brand '{brand}'")

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

    def check_campaign(self):
        try:
            conn = self.conn

            query1 = """
SELECT DISTINCT campaign_id
FROM amazon_campaign_update
WHERE change_type = 'budget'
AND update_time >= NOW() - INTERVAL 1 DAY
AND update_time <= NOW()
AND status = 'success'
                    """
            df1 = pd.read_sql(query1, con=conn)
            # return df
            return df1["campaign_id"].tolist()
        except Exception as e:
            print("Error while check_campaign:", e)

    def check_campaign_placement(self):
        try:
            conn = self.conn

            query1 = """
SELECT DISTINCT campaignId,placement
FROM amazon_campaign_placement_update
WHERE update_time >= NOW() - INTERVAL 1 DAY
AND update_time <= NOW()
AND status = 'success'
                    """
            df1 = pd.read_sql(query1, con=conn)
            # return df
            return df1["campaignId"].tolist(), df1["placement"].tolist()
        except Exception as e:
            print("Error while check_campaign_placement:", e)

    def check_keyword(self):
        try:
            conn = self.conn

            query1 = """
SELECT DISTINCT keywordId FROM amazon_keyword_update
WHERE create_time >= NOW() - INTERVAL 1 DAY
AND create_time <= NOW()
AND operation_state = 'success'
                    """
            df1 = pd.read_sql(query1, con=conn)
            # return df
            return df1["keywordId"].tolist()
        except Exception as e:
            print("Error while check_keyword:", e)

    def check_targeting(self):
        try:
            conn = self.conn

            query1 = """
SELECT DISTINCT expression FROM amazon_targeting_update
WHERE update_time >= NOW() - INTERVAL 1 DAY
AND update_time <= NOW()
AND targetingState = 'success'
                    """
            df1 = pd.read_sql(query1, con=conn)
            # return df
            return df1["expression"].tolist()
        except Exception as e:
            print("Error while check_targeting:", e)
