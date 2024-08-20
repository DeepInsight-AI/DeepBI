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

    def get_scan_sku(self, market, date):
        """从数据库获取手动操作的sku的总数并上传数据库"""
        try:
            conn = self.conn

            query1 = f"""
SELECT
    COUNT(*) as count
FROM
    amazon_advertising_change_history
LEFT JOIN
    amazon_campaigns_list_sp ON amazon_advertising_change_history.campaignId = amazon_campaigns_list_sp.campaignId
LEFT JOIN
    amazon_sp_productads_list ON amazon_advertising_change_history.entityId = amazon_sp_productads_list.adId
WHERE
    changeType = 'STATUS'
    AND entityType = 'AD'
    AND amazon_advertising_change_history.market = '{market}'
    AND FROM_UNIXTIME(amazon_advertising_change_history.TIMESTAMP / 1000, '%Y-%m-%d') = '{date}'
    AND previousValue IS NOT NULL
    AND amazon_campaigns_list_sp.campaign_name LIKE 'DeepBI_%'
                    """
            df1 = pd.read_sql(query1, con=conn)
            count = df1.loc[0, 'count']
            # return df
            return count
        except Exception as e:
            print("Error while get_scan_sku:", e)

    def get_scan_keyword(self, market, date):
        """从数据库获取手动操作的关键词的总数并上传数据库"""
        try:
            conn = self.conn

            query1 = f"""
SELECT
	COUNT(*) as count
FROM
	amazon_advertising_change_history
LEFT JOIN
	amazon_campaigns_list_sp ON amazon_advertising_change_history.campaignId = amazon_campaigns_list_sp.campaignId
WHERE
	changeType = 'BID_AMOUNT'
	AND entityType = 'KEYWORD'
	AND amazon_advertising_change_history.market = '{market}'
	AND FROM_UNIXTIME( TIMESTAMP / 1000, '%Y-%m-%d' ) = '{date}'
	AND previousValue != 'null'
	AND amazon_campaigns_list_sp.campaign_name LIKE 'DeepBI_%'
                    """
            df1 = pd.read_sql(query1, con=conn)
            count = df1.loc[0, 'count']
            # return df
            return count
        except Exception as e:
            print("Error while get_scan_keyword:", e)

    def get_automatic_targeting_bid_info(self, market, date):
        """从数据库获取手动操作的自动定位组改价信息并上传线上"""
        try:
            conn = self.conn

            query1 = f"""
SELECT
    ROUND(newValue - previousValue, 2) AS bid_adjust,
    amazon_campaigns_list_sp.campaign_name,
    CASE
        WHEN expression LIKE '%QUERY_BROAD_REL_MATCHES%' THEN 'loose-match'
        WHEN expression LIKE '%QUERY_HIGH_REL_MATCHES%' THEN 'close-match'
        WHEN expression LIKE '%ASIN_SUBSTITUTE_RELATED%' THEN 'substitutes'
        WHEN expression LIKE '%ASIN_ACCESSORY_RELATED%' THEN 'complements'
        ELSE expression
    END AS expression
FROM
    amazon_advertising_change_history
LEFT JOIN
    amazon_campaigns_list_sp ON amazon_advertising_change_history.campaignId = amazon_campaigns_list_sp.campaignId
LEFT JOIN
    amazon_targets_list_sp ON amazon_advertising_change_history.entityId = amazon_targets_list_sp.targetId
WHERE
    changeType = 'BID_AMOUNT'
    AND entityType = 'PRODUCT_TARGETING'
    AND productTargetingType = 'PREDEFINED'
    AND amazon_advertising_change_history.market = '{market}'
    AND FROM_UNIXTIME(TIMESTAMP / 1000, '%Y-%m-%d') = '{date}'
    AND previousValue != 'null'
    AND amazon_campaigns_list_sp.campaign_name LIKE 'DeepBI_%'
GROUP BY
    amazon_advertising_change_history.id
                    """
            df1 = pd.read_sql(query1, con=conn)
            # return df
            return df1['campaign_name'].tolist(), df1['expression'].tolist(), df1['bid_adjust'].tolist()
        except Exception as e:
            print("Error while get_automatic_targeting_bid_info:", e)

    def get_product_target_bid_info(self, market, date):
        """从数据库获取手动操作的商品投放改价信息并上传线上"""
        try:
            conn = self.conn

            query1 = f"""
SELECT
	 ROUND(newValue-previousValue,2) AS bid_adjust,
	 amazon_campaigns_list_sp.campaign_name,
	 targetingExpression
FROM
	amazon_advertising_change_history
LEFT JOIN
	amazon_campaigns_list_sp ON amazon_advertising_change_history.campaignId = amazon_campaigns_list_sp.campaignId
WHERE
	changeType = 'BID_AMOUNT'
	AND entityType = 'PRODUCT_TARGETING'
	AND productTargetingType = 'EXPRESSION'
	AND amazon_advertising_change_history.market = '{market}'
	AND FROM_UNIXTIME( TIMESTAMP / 1000, '%Y-%m-%d' ) = '{date}'
	AND previousValue != 'null'
	AND amazon_campaigns_list_sp.campaign_name LIKE 'DeepBI_%'
GROUP BY
amazon_advertising_change_history.id
                    """
            df1 = pd.read_sql(query1, con=conn)
            # return df
            return df1['campaign_name'].tolist(), df1['targetingExpression'].tolist(), df1['bid_adjust'].tolist()
        except Exception as e:
            print("Error while get_product_target_bid_info:", e)

    def get_keyword_bid_info(self, market, date):
        """从数据库获取手动操作的关键词改价信息并上传线上"""
        try:
            conn = self.conn

            query1 = f"""
SELECT
	 ROUND(newValue-previousValue,2) AS bid_adjust,
	 amazon_campaigns_list_sp.campaign_name,
	 keyword,
	 keywordType
FROM
	amazon_advertising_change_history
LEFT JOIN
	amazon_campaigns_list_sp ON amazon_advertising_change_history.campaignId = amazon_campaigns_list_sp.campaignId
WHERE
	changeType = 'BID_AMOUNT'
	AND entityType = 'KEYWORD'
	AND amazon_advertising_change_history.market = '{market}'
	AND FROM_UNIXTIME( TIMESTAMP / 1000, '%Y-%m-%d' ) = '{date}'
	AND previousValue != 'null'
	AND amazon_campaigns_list_sp.campaign_name LIKE 'DeepBI_%'
GROUP BY
amazon_advertising_change_history.id
                    """
            df1 = pd.read_sql(query1, con=conn)
            # return df
            return df1['campaign_name'].tolist(), df1['keyword'].tolist(), df1['keywordType'].tolist(), df1['bid_adjust'].tolist()
        except Exception as e:
            print("Error while get_keyword_bid_info:", e)

    def get_sku_state_info(self, market, date):
        """从数据库获取手动操作的sku修改状态信息并上传线上"""
        try:
            conn = self.conn

            query1 = f"""
SELECT
	 newValue AS bid_adjust,
	 amazon_campaigns_list_sp.campaign_name,
	 amazon_sp_productads_list.sku
FROM
	amazon_advertising_change_history
LEFT JOIN
	amazon_campaigns_list_sp ON amazon_advertising_change_history.campaignId = amazon_campaigns_list_sp.campaignId
LEFT JOIN
	amazon_sp_productads_list ON amazon_advertising_change_history.entityId = amazon_sp_productads_list.adId
WHERE
	changeType = 'STATUS'
	AND entityType = 'AD'
	AND amazon_advertising_change_history.market = '{market}'
	AND FROM_UNIXTIME( TIMESTAMP / 1000, '%Y-%m-%d' ) = '{date}'
	AND previousValue != 'null'
	AND amazon_campaigns_list_sp.campaign_name LIKE 'DeepBI_%'
GROUP BY
amazon_advertising_change_history.id
                    """
            df1 = pd.read_sql(query1, con=conn)
            # return df
            return df1['campaign_name'].tolist(), df1['sku'].tolist(), df1['bid_adjust'].tolist()
        except Exception as e:
            print("Error while get_sku_state_info:", e)


# res = AmazonMysqlRagUitl('LAPASA', 'DE').get_scan_keyword('DE','2024-08-15')
# print(res)
