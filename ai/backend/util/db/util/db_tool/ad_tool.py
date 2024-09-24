import json
import os
from datetime import datetime
import pandas as pd
import pymysql
from ai.backend.util.db.configuration.path import get_config_path
from ai.backend.util.db.auto_process.db_api import BaseDb

class DbSpTools(BaseDb):
    def __init__(self, db, brand, market):
        super().__init__(db, brand, market)

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
