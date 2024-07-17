import json

import pymysql
import pandas as pd
from datetime import datetime
import warnings

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

    def __init__(self, brand):
        self.db_info = self.load_db_info(brand)
        self.conn = self.connect(self.db_info)

    def load_db_info(self, brand):
        # 从 JSON 文件加载数据库信息
        with open('C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/db_info_log.json', 'r') as f:
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

    def get_scan_campaign(self, market, date):
        try:
            conn = self.conn

            query1 = """
SELECT COUNT(*) as count
FROM amazon_campaign_create
WHERE DATE(create_time) = '{}'
AND operation_state = 'success'
AND market = '{}';
                    """.format(date, market)
            df1 = pd.read_sql(query1, con=conn)
            count = df1.loc[0, 'count']
            # return df
            return count
        except Exception as e:
            print("Error while get_new_create_campaign:", e)

    def get_new_create_campaign(self, market, date):
        try:
            conn = self.conn

            query1 = """
SELECT COUNT(*) as count
FROM amazon_campaign_create
WHERE DATE(create_time) = '{}'
AND operation_state = 'success'
AND market = '{}';
                    """.format(date,market)
            df1 = pd.read_sql(query1, con=conn)
            count = df1.loc[0, 'count']
            # return df
            return count
        except Exception as e:
            print("Error while get_new_create_campaign:", e)

    def get_update_budget(self, market, date):
        try:
            conn = self.conn

            query1 = """
SELECT COUNT(*) as count
FROM amazon_campaign_update
WHERE DATE(update_time) = '{}'
AND status = 'success'
AND market = '{}';
                    """.format(date,market)
            df1 = pd.read_sql(query1, con=conn)
            count = df1.loc[0, 'count']
            # return df
            return count
        except Exception as e:
            print("Error while get_update_budget:", e)

    def get_update_targeting_group(self, market, date):
        try:
            conn = self.conn

            query1 = """
SELECT COUNT(*) as count
FROM amazon_campaign_placement_update
WHERE DATE(update_time) = '{}'
AND status = 'success'
AND market = '{}';
                    """.format(date, market)
            df1 = pd.read_sql(query1, con=conn)
            count = df1.loc[0, 'count']
            # return df
            return count
        except Exception as e:
            print("Error while get_update_targeting_group:", e)

    def get_update_keyword(self, market, date):
        try:
            conn = self.conn

            query1 = """
SELECT COUNT(*) as count
FROM amazon_keyword_create
WHERE DATE(create_time) = '{}'
AND operation_state = 'success'
AND market = '{}';
                    """.format(date, market)
            df1 = pd.read_sql(query1, con=conn)
            count1 = df1.loc[0, 'count']

            query2 = """
            SELECT COUNT(*) as count
            FROM amazon_keyword_update
            WHERE DATE(create_time) = '{}'
            AND operation_state = 'success'
            AND market = '{}';
                                """.format(date,market)
            df2 = pd.read_sql(query2, con=conn)
            count2 = df2.loc[0, 'count']

            count = count1 + count2
            # return df
            return count
        except Exception as e:
            print("Error while get_update_keyword:", e)

    def get_update_sku(self, market, date):
        try:
            conn = self.conn

            query1 = """
SELECT COUNT(*) as count
FROM amazon_product_update
WHERE DATE(update_time) = '{}'
AND status = 'success'
AND market = '{}';
                    """.format(date, market)
            df1 = pd.read_sql(query1, con=conn)
            count = df1.loc[0, 'count']
            # return df
            return count
        except Exception as e:
            print("Error while get_update_targeting_group:", e)

    def get_operated_campaign(self, market, date):
        try:
            conn = self.conn

            query1 = """
SELECT DISTINCT campaign_id FROM `amazon_campaign_update`
WHERE change_type = 'budget'
AND status = 'success'
AND market = '{}'
AND  update_time >= ('{}' - INTERVAL 4 DAY)
                    """.format(market, date)
            df1 = pd.read_sql(query1, con=conn)
            count = df1['campaign_id'].tolist()
            # return df
            return count
        except Exception as e:
            print("Error while get_operated_campaign:", e)

    def get_operated_campaign_placement(self, market, date):
        try:
            conn = self.conn

            query1 = """
SELECT
    campaignId,
    CASE
        WHEN placement = 'PLACEMENT_PRODUCT_PAGE' THEN 'Detail Page on-Amazon'
        WHEN placement = 'PLACEMENT_REST_OF_SEARCH' THEN 'Other on-Amazon'
        WHEN placement = 'PLACEMENT_TOP' THEN 'Top of Search on-Amazon'
        ELSE placement  -- 如果有其他值，可以选择保留原始值或者做其他处理
    END AS mapped_placement
FROM `amazon_campaign_placement_update`
WHERE status = 'success'
    AND market = '{}'
    AND update_time >= ('{}' - INTERVAL 4 DAY)
                    """.format(market, date)
            df1 = pd.read_sql(query1, con=conn)
            # return df
            return df1['campaignId'].tolist(), df1['mapped_placement'].tolist()
        except Exception as e:
            print("Error while get_operated_campaign_placement:", e)







# # 实例化AmazonMysqlRagUitl类
# util = AmazonMysqlRagUitl('LAPASA')
#
# # 调用方法进行测试
# #result = util.get_new_create_campaign('2024-06-24')
# #result = util.get_update_keyword('JP','2024-06-25')
# #result = util.get_new_sspu(market1, market2, startdate, enddate)
# result = util.get_operated_campaign('FR','2024-07-15')
#
# # 打印结果
# print(result)
