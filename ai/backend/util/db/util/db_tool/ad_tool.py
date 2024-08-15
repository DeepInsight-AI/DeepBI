import json
import os
from datetime import datetime
import pandas as pd
import pymysql
from ai.backend.util.db.configuration.path import get_config_path

class DbSpTools:
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

    def get_profileId(self, market):
        # 低于 平均ACOS值 30% 以上的  campaign 广告活动
        # 建议执行的操作：预算提升30%
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            query = f"""
SELECT DISTINCT profileId,region FROM amazon_profile
WHERE countryCode = '{market}'
             """
            df = pd.read_sql(query, con=conn)
            print("get_profileId Data successfully!")
            return df.loc[0,'profileId'], df.loc[0,'region']

        except Exception as error:
            print("get_profileId Error while query data:", error)
