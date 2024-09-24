import json
import os
from datetime import datetime

import pymysql
from ai.backend.util.db.configuration.path import get_config_path


class BaseDb:
    def __init__(self, db, brand, market, log=False):
        self.brand = brand
        self.market = market
        self.db = db
        if log:
            self.db_info = self.load_log_db_info()
        else:
            self.db_info = self.load_db_info()
        self.conn = self.connect(self.db_info)

    def load_db_info(self):
        # 从 JSON 文件加载数据库信息
        db_info_path = os.path.join(get_config_path(), 'db_info.json')
        with open(db_info_path, 'r') as f:
            db_info_json = json.load(f)

        if self.db not in db_info_json:
            raise ValueError(f"Unknown db '{self.db}'")

        brand_info = db_info_json[self.db][self.brand]

        # 如果指定了国家
        if self.market:
            # 检查国家是否在品牌信息中
            if self.market in brand_info:
                return brand_info[self.market]
            # 如果没有找到具体国家的信息，检查是否有默认信息
            if 'default' in brand_info:
                return brand_info['default']

        # 返回默认信息
        return brand_info.get('default', {})

    def load_log_db_info(self):
        # 从 JSON 文件加载数据库信息
        db_info_path = os.path.join(get_config_path(), 'db_info_log.json')
        with open(db_info_path, 'r') as f:
            db_info_json = json.load(f)

        if self.db not in db_info_json:
            raise ValueError(f"Unknown db '{self.db}'")

        brand_info = db_info_json[self.db][self.brand]

        # 如果指定了国家
        if self.market:
            # 检查国家是否在品牌信息中
            if self.market in brand_info:
                return brand_info[self.market]
            # 如果没有找到具体国家的信息，检查是否有默认信息
            if 'default' in brand_info:
                return brand_info['default']

        # 返回默认信息
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

    def get_timestamp(self):
        # 获取当前时间
        current_time = datetime.now()
        timestamp = int(current_time.timestamp())
        date_string = current_time.strftime("%Y-%m-%d")
        # 组合日期和时间戳
        date_timestamp_string = f"{date_string}_{timestamp}"
        return date_timestamp_string

    def log(self, message):
        # 一个简单的日志记录方法
        print(message)
