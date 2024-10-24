import json
import os

import pymysql
import pandas as pd
from datetime import datetime
import warnings
from ai.backend.util.db.configuration.path import get_config_path
from ai.backend.util.db.auto_process.db_api import BaseDb

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

class AmazonMysqlRagUitl(BaseDb):

    def __init__(self, db, brand, market):
        super().__init__(db, brand, market)

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
        finally:
            self.connect_close()

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
        finally:
            self.connect_close()

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
        finally:
            self.connect_close()

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
        finally:
            self.connect_close()

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
        finally:
            self.connect_close()

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
        finally:
            self.connect_close()

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
        finally:
            self.connect_close()

    def get_product_target_bid_info_batch(self, market, date):
        """从数据库获取手动操作的商品投放改价信息并上传线上"""
        try:
            conn = self.conn

            query1 = f"""
SELECT
     ROUND(newValue-previousValue,2) AS bid_adjust,
     amazon_campaigns_list_sp.campaign_name,
     targetingExpression,
     amazon_advertising_change_history.entityId
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
            return df1['campaign_name'].tolist(), df1['targetingExpression'].tolist(), df1['bid_adjust'].tolist(), df1['entityId'].tolist()
        except Exception as e:
            print("Error while get_product_target_bid_info:", e)
        finally:
            self.connect_close()

    def get_automatic_targeting_bid_info_batch(self, market, date):
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
    END AS expression,
    entityId
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
            return df1['campaign_name'].tolist(), df1['expression'].tolist(), df1['bid_adjust'].tolist(), df1['entityId'].tolist()
        except Exception as e:
            print("Error while get_automatic_targeting_bid_info:", e)
        finally:
            self.connect_close()

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
        finally:
            self.connect_close()

    def get_keyword_bid_info_batch(self, market, date):
        """从数据库获取手动操作的关键词改价信息并上传线上"""
        try:
            conn = self.conn

            query1 = f"""
SELECT
     ROUND(newValue-previousValue,2) AS bid_adjust,
     amazon_campaigns_list_sp.campaign_name,
     keyword,
     keywordType,
     entityId
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
            return df1['campaign_name'].tolist(), df1['keyword'].tolist(), df1['keywordType'].tolist(), df1['bid_adjust'].tolist(), df1['entityId'].tolist()
        except Exception as e:
            print("Error while get_keyword_bid_info:", e)
        finally:
            self.connect_close()

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
        finally:
            self.connect_close()

    def get_sku_state_info_batch(self, market, date):
        """从数据库获取手动操作的sku修改状态信息并上传线上"""
        try:
            conn = self.conn

            query1 = f"""
SELECT
     newValue AS bid_adjust,
     amazon_campaigns_list_sp.campaign_name,
     amazon_sp_productads_list.sku,
     amazon_sp_productads_list.adId
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
            return df1['campaign_name'].tolist(), df1['sku'].tolist(), df1['bid_adjust'].tolist(), df1['adId'].tolist()
        except Exception as e:
            print("Error while get_sku_state_info:", e)
        finally:
            self.connect_close()

    def get_summarize_data_info_one_country(self, query=None):
        """从数据库获取汇总信息并上传线上"""
        try:
            conn = self.conn

            query1 = f"""
SELECT
        '{query['country']}' as 国家,
        all_order.event_date as 总销售日期,
        ad_order.广告总销售额,
        ad_order.广告总花费,
        CONCAT(ROUND(ad_order.广告总ACOS * 100, 2), '%') AS 广告总ACOS,
        COALESCE(deepbi_order.DeepBI计划花费, 0) AS DeepBI计划花费,
        COALESCE(deepbi_order.DeepBI计划销量, 0) AS DeepBI计划销量,
        CONCAT(ROUND(COALESCE(deepbi_order.新开计划acos, 0) * 100, 2), '%') AS 新开计划acos,
        CONCAT(ROUND(COALESCE(((deepbi_order.DeepBI计划销量 / ad_order.广告总销售额) * 100), 0), 2), '%') AS 新开计划销量占比,
        ROUND((ad_order.广告总销售额 - COALESCE(deepbi_order.DeepBI计划销量, 0)), 2) AS 旧计划销售额,
        ROUND((ad_order.广告总花费 - COALESCE(deepbi_order.DeepBI计划花费, 0)), 2) AS 旧计划花费,
        CONCAT(ROUND(((ad_order.广告总花费 - COALESCE(deepbi_order.DeepBI计划花费, 0)) / (ad_order.广告总销售额 - COALESCE(deepbi_order.DeepBI计划销量, 0))) * 100, 2), '%') AS 旧计划acos,
        CONCAT(ROUND(((1 - COALESCE(deepbi_order.DeepBI计划销量, 0) / ad_order.广告总销售额) * 100), 2), '%') AS 旧计划销量占比,
        all_order.总销售额,
        ad_order.广告总销售额 AS 广告销售额,
        ROUND((all_order.总销售额 - ad_order.广告总销售额),2) AS 自然销售额,
        CONCAT(ROUND(((1 - ad_order.广告总销售额 / all_order.总销售额) * 100), 2),'%') AS 自然销售额比例,
        CONCAT(ROUND(((COALESCE(ad_order.广告总花费, 0) / all_order.总销售额) * 100), 2), '%') AS tacos
    FROM
    (
        -- 计算总销售额
        SELECT
            sales_channel AS 国家,
            DATE(CONVERT_TZ(purchase_date ,'+08:00', '{query['timezone_offset']}')) AS event_date,
            ROUND(SUM(item_price), 2) AS 总销售额
        FROM
            amazon_get_flat_file_all_orders_data_by_last_update_general
        WHERE
            CONVERT_TZ(purchase_date ,'+08:00', '{query['timezone_offset']}') >= DATE_SUB(CURDATE(), INTERVAL 16 DAY)
            AND CONVERT_TZ(purchase_date ,'+08:00', '{query['timezone_offset']}') < CURDATE()
            AND sales_channel = '{query['sales_channel']}'
        GROUP BY
            sales_channel,
            event_date
        ORDER BY
            event_date
    ) AS all_order

    LEFT JOIN
    (
        -- 计算广告数据
        SELECT
            sp.market AS 国家,
            sp.DATE,
            (COALESCE(sp.sum_cost, 0) + COALESCE(sd.sum_cost, 0) + COALESCE(sb.sum_cost, 0)) AS 广告总花费,
            (COALESCE(sp.sum_sales, 0) + COALESCE(sd.sum_sales, 0) + COALESCE(sb.sum_sales, 0)) AS 广告总销售额,
            ROUND(
                (COALESCE(sp.sum_cost, 0) + COALESCE(sd.sum_cost, 0) + COALESCE(sb.sum_cost, 0)) /
                (COALESCE(sp.sum_sales, 0) + COALESCE(sd.sum_sales, 0) + COALESCE(sb.sum_sales, 0)),
                4
            ) AS 广告总ACOS
        FROM
            (
                SELECT
                    market,
                    DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales7d) AS sum_sales
                FROM
                    amazon_campaign_reports_sp
                WHERE
                    DATE >= DATE_SUB(CURDATE(), INTERVAL 16 DAY)
                    AND DATE < CURDATE()
                GROUP BY
                    market,
                    DATE
            ) AS sp
        LEFT JOIN (
                SELECT
                    market,
                    DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales) AS sum_sales
                FROM
                    amazon_campaign_reports_sd
                WHERE
                    DATE >= DATE_SUB(CURDATE(), INTERVAL 16 DAY)
                    AND DATE < CURDATE()
                GROUP BY
                    market,
                    DATE
            ) AS sd ON sd.market = sp.market AND sd.DATE = sp.DATE
        LEFT JOIN (
                SELECT
                    market,
                    DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales) AS sum_sales
                FROM
                    amazon_campaign_reports_sb
                WHERE
                    DATE >= DATE_SUB(CURDATE(), INTERVAL 16 DAY)
                    AND DATE < CURDATE()
                GROUP BY
                    market,
                    DATE
            ) AS sb ON sb.market = sp.market AND sb.DATE = sp.DATE
        WHERE
            sp.market = '{query['country']}'
        ORDER BY
            sp.DATE
    ) AS ad_order ON all_order.event_date = ad_order.DATE

		 LEFT JOIN
    (
        -- 计算广告数据
        SELECT
            sp.market AS 国家,
            sp.DATE,
            (COALESCE(sp.sum_cost, 0) + COALESCE(sd.sum_cost, 0) + COALESCE(sb.sum_cost, 0)) AS DeepBI计划花费,
            (COALESCE(sp.sum_sales, 0) + COALESCE(sd.sum_sales, 0) + COALESCE(sb.sum_sales, 0)) AS DeepBI计划销量,
            ROUND(
                (COALESCE(sp.sum_cost, 0) + COALESCE(sd.sum_cost, 0) + COALESCE(sb.sum_cost, 0)) /
                (COALESCE(sp.sum_sales, 0) + COALESCE(sd.sum_sales, 0) + COALESCE(sb.sum_sales, 0)),
                4
            ) AS 新开计划acos
        FROM
            (
                SELECT
                    market,
                    DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales7d) AS sum_sales
                FROM
                    amazon_campaign_reports_sp
                WHERE
                    DATE >= DATE_SUB(CURDATE(), INTERVAL 16 DAY)
                    AND DATE < CURDATE()
                    AND campaignName LIKE 'DeepBI_%'
                GROUP BY
                    market,
                    DATE
            ) AS sp
        LEFT JOIN (
                SELECT
                    market,
                    DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales) AS sum_sales
                FROM
                    amazon_campaign_reports_sd
                WHERE
                    DATE >= DATE_SUB(CURDATE(), INTERVAL 16 DAY)
                    AND DATE < CURDATE()
										AND campaignName LIKE 'DeepBI_%'
                GROUP BY
                    market,
                    DATE
            ) AS sd ON sd.market = sp.market AND sd.DATE = sp.DATE
        LEFT JOIN (
                SELECT
                    market,
                    DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales) AS sum_sales
                FROM
                    amazon_campaign_reports_sb
                WHERE
                    DATE >= DATE_SUB(CURDATE(), INTERVAL 16 DAY)
                    AND DATE < CURDATE()
										AND campaignName LIKE 'DeepBI_%'
                GROUP BY
                    market,
                    DATE
            ) AS sb ON sb.market = sp.market AND sb.DATE = sp.DATE
        WHERE
            sp.market = '{query['country']}'
        ORDER BY
            sp.DATE
    ) AS deepbi_order ON all_order.event_date = deepbi_order.DATE;
                    """
            df = pd.read_sql(query1, con=conn)
            # 计算汇总数据
            total_ad_sales = df['广告总销售额'].sum()
            total_ad_cost = df['广告总花费'].sum()
            total_new_cost = df['DeepBI计划花费'].sum()
            total_new_sales = df['DeepBI计划销量'].sum()
            total_old_sales = df['旧计划销售额'].sum()
            total_old_cost = df['旧计划花费'].sum()
            total_sales = df['总销售额'].sum()
            total_nature_sales = df['自然销售额'].sum()

            # 计算汇总行的各项指标
            ad_acos = (total_ad_cost / total_ad_sales) * 100 if total_ad_sales > 0 else 0
            new_ad_acos = (total_new_cost / total_new_sales) * 100 if total_new_sales > 0 else 0
            new_ad_scale = (total_new_sales / total_ad_sales) * 100 if total_ad_sales > 0 else 0
            old_ad_acos = (total_old_cost / total_old_sales) * 100 if total_old_sales > 0 else 0
            old_ad_scale = (total_old_sales / total_ad_sales) * 100 if total_ad_sales > 0 else 0
            nature_scale = (total_nature_sales / total_sales) * 100 if total_sales > 0 else 0
            tacos = (total_ad_cost / total_sales) * 100 if total_sales > 0 else 0

            # 创建汇总数据行
            summary_data = {
                '国家': query['country'],
                '总销售日期': '汇总',
                '广告总销售额': round(total_ad_sales, 2),
                '广告总花费': round(total_ad_cost, 2),
                '广告总ACOS': f'{ad_acos:.2f}%',
                'DeepBI计划花费': round(total_new_cost, 2),
                'DeepBI计划销量': round(total_new_sales, 2),
                '新开计划acos': f'{new_ad_acos:.2f}%',
                '新开计划销量占比': f'{new_ad_scale:.2f}%',
                '旧计划销售额': round(total_old_sales, 2),
                '旧计划花费': round(total_old_cost, 2),
                '旧计划acos': f'{old_ad_acos:.2f}%',
                '旧计划销量占比': f'{old_ad_scale:.2f}%',
                '总销售额': round(total_sales, 2),
                '广告销售额': round(total_ad_sales, 2),
                '自然销售额': round(total_nature_sales, 2),
                '自然销售额比例': f'{nature_scale:.2f}%',
                'tacos': f'{tacos:.2f}%'
            }

            # 将汇总数据行添加到 DataFrame
            summary_df = pd.DataFrame([summary_data])
            df = pd.concat([df, summary_df], ignore_index=True)
            # return df
            return df
        except Exception as e:
            print("Error while get_summarize_data_info:", e)
        finally:
            self.connect_close()

    def get_summarize_data_info_summarize_country(self, query=None):
        """从数据库获取汇总信息并上传线上"""
        try:
            conn = self.conn

            query1 = f"""
SELECT
        '{query['region']}' as 国家,
        all_order.event_date as 总销售日期,
        ad_order.广告总销售额,
        ad_order.广告总花费,
        CONCAT(ROUND(ad_order.广告总ACOS * 100, 2), '%%') AS 广告总ACOS,
        COALESCE(deepbi_order.DeepBI计划花费, 0) AS DeepBI计划花费,
        COALESCE(deepbi_order.DeepBI计划销量, 0) AS DeepBI计划销量,
        CONCAT(ROUND(COALESCE(deepbi_order.新开计划acos, 0) * 100, 2), '%%') AS 新开计划acos,
        CONCAT(ROUND(COALESCE(((deepbi_order.DeepBI计划销量 / ad_order.广告总销售额) * 100), 0), 2), '%%') AS 新开计划销量占比,
        ROUND((ad_order.广告总销售额 - COALESCE(deepbi_order.DeepBI计划销量, 0)), 2) AS 旧计划销售额,
        ROUND((ad_order.广告总花费 - COALESCE(deepbi_order.DeepBI计划花费, 0)), 2) AS 旧计划花费,
        CONCAT(ROUND(((ad_order.广告总花费 - COALESCE(deepbi_order.DeepBI计划花费, 0)) / (ad_order.广告总销售额 - COALESCE(deepbi_order.DeepBI计划销量, 0))) * 100, 2), '%%') AS 旧计划acos,
        CONCAT(ROUND(((1 - COALESCE(deepbi_order.DeepBI计划销量, 0) / ad_order.广告总销售额) * 100), 2), '%%') AS 旧计划销量占比,
        all_order.总销售额,
        ad_order.广告总销售额 AS 广告销售额,
        ROUND((all_order.总销售额 - ad_order.广告总销售额),2) AS 自然销售额,
        CONCAT(ROUND(((1 - ad_order.广告总销售额 / all_order.总销售额) * 100), 2),'%%') AS 自然销售额比例,
        CONCAT(ROUND(((COALESCE(ad_order.广告总花费, 0) / all_order.总销售额) * 100), 2), '%%') AS tacos
    FROM
    (
        -- 计算总销售额
        SELECT
            sales_channel AS 国家,
            DATE(CONVERT_TZ(purchase_date ,'+08:00', '{query['timezone_offset']}')) AS event_date,
            ROUND(SUM(item_price), 2) AS 总销售额
        FROM
            amazon_get_flat_file_all_orders_data_by_last_update_general
        WHERE
            CONVERT_TZ(purchase_date ,'+08:00', '{query['timezone_offset']}') >= DATE_SUB(CURDATE(), INTERVAL 16 DAY)
            AND CONVERT_TZ(purchase_date ,'+08:00', '{query['timezone_offset']}') < CURDATE()
            AND sales_channel IN %(column1_values1)s
        GROUP BY
            event_date
        ORDER BY
            event_date
    ) AS all_order

    LEFT JOIN
    (
        -- 计算广告数据
        SELECT
            sp.market AS 国家,
            sp.DATE,
            (COALESCE(sp.sum_cost, 0) + COALESCE(sd.sum_cost, 0) + COALESCE(sb.sum_cost, 0)) AS 广告总花费,
            (COALESCE(sp.sum_sales, 0) + COALESCE(sd.sum_sales, 0) + COALESCE(sb.sum_sales, 0)) AS 广告总销售额,
            ROUND(
                (COALESCE(sp.sum_cost, 0) + COALESCE(sd.sum_cost, 0) + COALESCE(sb.sum_cost, 0)) /
                (COALESCE(sp.sum_sales, 0) + COALESCE(sd.sum_sales, 0) + COALESCE(sb.sum_sales, 0)),
                4
            ) AS 广告总ACOS
        FROM
            (
                SELECT
                    market,
                    DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales7d) AS sum_sales
                FROM
                    amazon_campaign_reports_sp
                WHERE
                    DATE >= DATE_SUB(CURDATE(), INTERVAL 16 DAY)
                    AND DATE < CURDATE()
										AND market IN %(column1_values2)s
                GROUP BY
                    DATE
            ) AS sp
        LEFT JOIN (
                SELECT
                    market,
                    DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales) AS sum_sales
                FROM
                    amazon_campaign_reports_sd
                WHERE
                    DATE >= DATE_SUB(CURDATE(), INTERVAL 16 DAY)
                    AND DATE < CURDATE()
										AND market IN %(column1_values2)s
                GROUP BY
                    DATE
            ) AS sd ON sd.DATE = sp.DATE
        LEFT JOIN (
                SELECT
                    market,
                    DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales) AS sum_sales
                FROM
                    amazon_campaign_reports_sb
                WHERE
                    DATE >= DATE_SUB(CURDATE(), INTERVAL 16 DAY)
                    AND DATE < CURDATE()
                    AND market IN %(column1_values2)s
                GROUP BY
                    DATE
            ) AS sb ON sb.DATE = sp.DATE
        WHERE
            sp.market IN %(column1_values2)s
        ORDER BY
            sp.DATE
    ) AS ad_order ON all_order.event_date = ad_order.DATE

		 LEFT JOIN
    (
        -- 计算广告数据
        SELECT
            sp.market AS 国家,
            sp.DATE,
            (COALESCE(sp.sum_cost, 0) + COALESCE(sd.sum_cost, 0) + COALESCE(sb.sum_cost, 0)) AS DeepBI计划花费,
            (COALESCE(sp.sum_sales, 0) + COALESCE(sd.sum_sales, 0) + COALESCE(sb.sum_sales, 0)) AS DeepBI计划销量,
            ROUND(
                (COALESCE(sp.sum_cost, 0) + COALESCE(sd.sum_cost, 0) + COALESCE(sb.sum_cost, 0)) /
                (COALESCE(sp.sum_sales, 0) + COALESCE(sd.sum_sales, 0) + COALESCE(sb.sum_sales, 0)),
                4
            ) AS 新开计划acos
        FROM
            (
                SELECT
                    market,
                    DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales7d) AS sum_sales
                FROM
                    amazon_campaign_reports_sp
                WHERE
                    DATE >= DATE_SUB(CURDATE(), INTERVAL 16 DAY)
                    AND DATE < CURDATE()
										AND market IN %(column1_values2)s
										AND campaignName LIKE 'DeepBI_%%'
                GROUP BY
                    DATE
            ) AS sp
        LEFT JOIN (
                SELECT
                    market,
                    DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales) AS sum_sales
                FROM
                    amazon_campaign_reports_sd
                WHERE
                    DATE >= DATE_SUB(CURDATE(), INTERVAL 16 DAY)
                    AND DATE < CURDATE()
										AND market IN %(column1_values2)s
										AND campaignName LIKE 'DeepBI_%%'
                GROUP BY
                    DATE
            ) AS sd ON sd.DATE = sp.DATE
        LEFT JOIN (
                SELECT
                    market,
                    DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales) AS sum_sales
                FROM
                    amazon_campaign_reports_sb
                WHERE
                    DATE >= DATE_SUB(CURDATE(), INTERVAL 16 DAY)
                    AND DATE < CURDATE()
										AND market IN %(column1_values2)s
										AND campaignName LIKE 'DeepBI_%%'
                GROUP BY
                    DATE
            ) AS sb ON sb.DATE = sp.DATE
        WHERE
            sp.market IN %(column1_values2)s
        ORDER BY
            sp.DATE
    ) AS deepbi_order ON all_order.event_date = deepbi_order.DATE;
                    """
            df = pd.read_sql(query1, con=conn, params={'column1_values1': query['sales_channel'], 'column1_values2': query['country']})
            # 计算汇总数据
            total_ad_sales = df['广告总销售额'].sum()
            total_ad_cost = df['广告总花费'].sum()
            total_new_cost = df['DeepBI计划花费'].sum()
            total_new_sales = df['DeepBI计划销量'].sum()
            total_old_sales = df['旧计划销售额'].sum()
            total_old_cost = df['旧计划花费'].sum()
            total_sales = df['总销售额'].sum()
            total_nature_sales = df['自然销售额'].sum()

            # 计算汇总行的各项指标
            ad_acos = (total_ad_cost / total_ad_sales) * 100 if total_ad_sales > 0 else 0
            new_ad_acos = (total_new_cost / total_new_sales) * 100 if total_new_sales > 0 else 0
            new_ad_scale = (total_new_sales / total_ad_sales) * 100 if total_ad_sales > 0 else 0
            old_ad_acos = (total_old_cost / total_old_sales) * 100 if total_old_sales > 0 else 0
            old_ad_scale = (total_old_sales / total_ad_sales) * 100 if total_ad_sales > 0 else 0
            nature_scale = (total_nature_sales / total_sales) * 100 if total_sales > 0 else 0
            tacos = (total_ad_cost / total_sales) * 100 if total_sales > 0 else 0

            # 创建汇总数据行
            summary_data = {
                '国家': query['region'],
                '总销售日期': '汇总',
                '广告总销售额': round(total_ad_sales, 2),
                '广告总花费': round(total_ad_cost, 2),
                '广告总ACOS': f'{ad_acos:.2f}%',
                'DeepBI计划花费': round(total_new_cost, 2),
                'DeepBI计划销量': round(total_new_sales, 2),
                '新开计划acos': f'{new_ad_acos:.2f}%',
                '新开计划销量占比': f'{new_ad_scale:.2f}%',
                '旧计划销售额': round(total_old_sales, 2),
                '旧计划花费': round(total_old_cost, 2),
                '旧计划acos': f'{old_ad_acos:.2f}%',
                '旧计划销量占比': f'{old_ad_scale:.2f}%',
                '总销售额': round(total_sales, 2),
                '广告销售额': round(total_ad_sales, 2),
                '自然销售额': round(total_nature_sales, 2),
                '自然销售额比例': f'{nature_scale:.2f}%',
                'tacos': f'{tacos:.2f}%'
            }

            # 将汇总数据行添加到 DataFrame
            summary_df = pd.DataFrame([summary_data])
            df = pd.concat([df, summary_df], ignore_index=True)
            # return df
            return df
        except Exception as e:
            print("Error while get_summarize_data_info_summarize_country:", e)
        finally:
            self.connect_close()

    def get_summarize_parent_asins_data_info_one_country(self, query=None):
        """从数据库获取汇总信息并上传线上"""
        try:
            conn = self.conn

            query1 = f"""
			SELECT
        '{query['country']}' as 国家1,
        all_order.event_date as 总销售日期1,
        COALESCE(ad_order.广告总销售额, 0) AS 广告总销售额1,
        COALESCE(ad_order.广告总花费, 0) AS 广告总花费1,
        CONCAT(ROUND(ad_order.广告总ACOS * 100, 2), '%%') AS 广告总ACOS1,
        COALESCE(deepbi_order.DeepBI计划花费, 0) AS DeepBI计划花费1,
        COALESCE(deepbi_order.DeepBI计划销量, 0) AS DeepBI计划销量1,
        CONCAT(ROUND(COALESCE(deepbi_order.新开计划acos, 0) * 100, 2), '%%') AS 新开计划acos1,
        CONCAT(ROUND(COALESCE(((deepbi_order.DeepBI计划销量 / ad_order.广告总销售额) * 100), 0), 2), '%%') AS 新开计划销量占比1,
        ROUND((ad_order.广告总销售额 - COALESCE(deepbi_order.DeepBI计划销量, 0)), 2) AS 旧计划销售额1,
        ROUND((ad_order.广告总花费 - COALESCE(deepbi_order.DeepBI计划花费, 0)), 2) AS 旧计划花费1,
        CONCAT(ROUND(((ad_order.广告总花费 - COALESCE(deepbi_order.DeepBI计划花费, 0)) / (ad_order.广告总销售额 - COALESCE(deepbi_order.DeepBI计划销量, 0))) * 100, 2), '%%') AS 旧计划acos1,
        CONCAT(ROUND(((1 - COALESCE(deepbi_order.DeepBI计划销量, 0) / ad_order.广告总销售额) * 100), 2), '%%') AS 旧计划销量占比1,
        all_order.总销售额 as 总销售额1,
        COALESCE(ad_order.广告总销售额, 0) AS 广告销售额1,
        ROUND((all_order.总销售额 - COALESCE(ad_order.广告总销售额, 0)),2) AS 自然销售额1,
        CONCAT(ROUND(((1 - COALESCE(ad_order.广告总销售额, 0) / all_order.总销售额) * 100), 2),'%%') AS 自然销售额比例1,
        CONCAT(ROUND(((COALESCE(ad_order.广告总花费, 0) / all_order.总销售额) * 100), 2), '%%') AS tacos1
    FROM
    (
			SELECT
											sales_channel AS 国家,
											CASE WHEN apie.parent_asins = '' THEN CONCAT(apie.asin,'(asin)') ELSE apie.parent_asins END AS parent_asins_or_asin,
											DATE(CONVERT_TZ(purchase_date,'+08:00', '{query['timezone_offset']}'
)) AS event_date,
											ROUND(SUM(item_price), 2) AS 总销售额
			FROM
											amazon_get_flat_file_all_orders_data_by_last_update_general agffa
			LEFT JOIN
											amazon_product_info_extended apie ON agffa.asin = apie.asin
			WHERE
											CONVERT_TZ(purchase_date,'+08:00', '{query['timezone_offset']}'
) >= DATE_SUB(CURDATE(), INTERVAL 16 DAY)
											AND CONVERT_TZ(purchase_date,'+08:00', '{query['timezone_offset']}'
) < CURDATE()
											AND sales_channel = '{query['sales_channel']}'
											AND apie.market = '{query['country']}'

											AND apie.asin IN %(column1_values1)s
			GROUP BY
											sales_channel,
											event_date
			ORDER BY
											event_date
    ) AS all_order

    LEFT JOIN
    (
        -- 计算广告数据
        SELECT
            sp.market AS 国家,
            sp.parent_asins_or_asin,
						sp.DATE,
            (COALESCE(sp.sum_cost, 0) + COALESCE(sd.sum_cost, 0)) AS 广告总花费,
            (COALESCE(sp.sum_sales, 0) + COALESCE(sd.sum_sales, 0)) AS 广告总销售额,
            ROUND(
                (COALESCE(sp.sum_cost, 0) + COALESCE(sd.sum_cost, 0)) /
                (COALESCE(sp.sum_sales, 0) + COALESCE(sd.sum_sales, 0)),
                4
            ) AS 广告总ACOS
        FROM
            (
                SELECT
										CASE WHEN apie.parent_asins = '' THEN CONCAT(apie.asin,'(asin)') ELSE apie.parent_asins END AS parent_asins_or_asin,
                    adpr.market,
                    adpr.DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales7d) AS sum_sales
                FROM
                    amazon_advertised_product_reports_sp adpr
								LEFT JOIN
										amazon_product_info_extended apie ON adpr.advertisedAsin = apie.asin AND adpr.market = apie.market
                WHERE
                    adpr.DATE >= DATE_SUB(CURDATE(), INTERVAL 16 DAY)
                    AND adpr.DATE < CURDATE()
										AND adpr.market = '{query['country']}'

										AND apie.asin IN %(column1_values1)s
                GROUP BY
                    adpr.market,
                    DATE
            ) AS sp
        LEFT JOIN (
                SELECT
										CASE WHEN apie.parent_asins = '' THEN CONCAT(apie.asin,'(asin)') ELSE apie.parent_asins END AS parent_asins_or_asin,
                    adpr.market,
                    adpr.DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales) AS sum_sales
                FROM
                    amazon_advertised_product_reports_sd adpr
								LEFT JOIN
										amazon_product_info_extended apie ON adpr.promotedAsin = apie.asin AND adpr.market = apie.market
                WHERE
                    adpr.DATE >= DATE_SUB(CURDATE(), INTERVAL 16 DAY)
                    AND adpr.DATE < CURDATE()
										AND apie.asin IN %(column1_values1)s
                GROUP BY
                    adpr.market,
                    DATE
            ) AS sd ON sd.market = sp.market AND sd.DATE = sp.DATE
        WHERE
            sp.market = '{query['country']}'

        ORDER BY
            sp.DATE
    ) AS ad_order ON all_order.event_date = ad_order.DATE
    LEFT JOIN
    (
        -- 计算广告数据
        SELECT
            sp.market AS 国家,
            sp.parent_asins_or_asin,
						sp.DATE,
            (COALESCE(sp.sum_cost, 0) + COALESCE(sd.sum_cost, 0)) AS DeepBI计划花费,
            (COALESCE(sp.sum_sales, 0) + COALESCE(sd.sum_sales, 0)) AS DeepBI计划销量,
            ROUND(
                (COALESCE(sp.sum_cost, 0) + COALESCE(sd.sum_cost, 0)) /
                (COALESCE(sp.sum_sales, 0) + COALESCE(sd.sum_sales, 0)),
                4
            ) AS 新开计划acos
        FROM
            (
                SELECT
										CASE WHEN apie.parent_asins = '' THEN CONCAT(apie.asin,'(asin)') ELSE apie.parent_asins END AS parent_asins_or_asin,
                    adpr.market,
                    adpr.DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales7d) AS sum_sales
                FROM
                    amazon_advertised_product_reports_sp adpr
								LEFT JOIN
										amazon_product_info_extended apie ON adpr.advertisedAsin = apie.asin AND adpr.market = apie.market
                WHERE
                    adpr.DATE >= DATE_SUB(CURDATE(), INTERVAL 16 DAY)
                    AND adpr.DATE < CURDATE()
										AND adpr.market = '{query['country']}'

										AND adpr.campaignName LIKE 'DeepBI_%%'
										AND apie.asin IN %(column1_values1)s
                GROUP BY
                    adpr.market,
                    DATE
            ) AS sp
        LEFT JOIN (
                SELECT
										CASE WHEN apie.parent_asins = '' THEN CONCAT(apie.asin,'(asin)') ELSE apie.parent_asins END AS parent_asins_or_asin,
                    adpr.market,
                    adpr.DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales) AS sum_sales
                FROM
                    amazon_advertised_product_reports_sd adpr
								LEFT JOIN
										amazon_product_info_extended apie ON adpr.promotedAsin = apie.asin AND adpr.market = apie.market
                WHERE
                    adpr.DATE >= DATE_SUB(CURDATE(), INTERVAL 16 DAY)
                    AND adpr.DATE < CURDATE()
										AND adpr.campaignName LIKE 'DeepBI_%%'
										AND apie.asin IN %(column1_values1)s
                GROUP BY
                    adpr.market,
                    DATE
            ) AS sd ON sd.market = sp.market AND sd.DATE = sp.DATE
        WHERE
            sp.market = '{query['country']}'

        ORDER BY
            sp.DATE
    ) AS deepbi_order ON all_order.event_date = deepbi_order.DATE;
                    """
            df = pd.read_sql(query1, con=conn, params={'column1_values1': query['parent_asins']})
            # 计算汇总数据
            total_ad_sales = df['广告总销售额1'].sum()
            total_ad_cost = df['广告总花费1'].sum()
            total_new_cost = df['DeepBI计划花费1'].sum()
            total_new_sales = df['DeepBI计划销量1'].sum()
            total_old_sales = df['旧计划销售额1'].sum()
            total_old_cost = df['旧计划花费1'].sum()
            total_sales = df['总销售额1'].sum()
            total_nature_sales = df['自然销售额1'].sum()

            # 计算汇总行的各项指标
            ad_acos = (total_ad_cost / total_ad_sales) * 100 if total_ad_sales > 0 else 0
            new_ad_acos = (total_new_cost / total_new_sales) * 100 if total_new_sales > 0 else 0
            new_ad_scale = (total_new_sales / total_ad_sales) * 100 if total_ad_sales > 0 else 0
            old_ad_acos = (total_old_cost / total_old_sales) * 100 if total_old_sales > 0 else 0
            old_ad_scale = (total_old_sales / total_ad_sales) * 100 if total_ad_sales > 0 else 0
            nature_scale = (total_nature_sales / total_sales) * 100 if total_sales > 0 else 0
            tacos = (total_ad_cost / total_sales) * 100 if total_sales > 0 else 0

            # 创建汇总数据行
            summary_data = {
                '国家1': query['country'],
                '总销售日期1': '汇总',
                '广告总销售额1': round(total_ad_sales, 2),
                '广告总花费1': round(total_ad_cost, 2),
                '广告总ACOS1': f'{ad_acos:.2f}%',
                'DeepBI计划花费1': round(total_new_cost, 2),
                'DeepBI计划销量1': round(total_new_sales, 2),
                '新开计划acos1': f'{new_ad_acos:.2f}%',
                '新开计划销量占比1': f'{new_ad_scale:.2f}%',
                '旧计划销售额1': round(total_old_sales, 2),
                '旧计划花费1': round(total_old_cost, 2),
                '旧计划acos1': f'{old_ad_acos:.2f}%',
                '旧计划销量占比1': f'{old_ad_scale:.2f}%',
                '总销售额1': round(total_sales, 2),
                '广告销售额1': round(total_ad_sales, 2),
                '自然销售额1': round(total_nature_sales, 2),
                '自然销售额比例1': f'{nature_scale:.2f}%',
                'tacos1': f'{tacos:.2f}%'
            }

            # 将汇总数据行添加到 DataFrame
            summary_df = pd.DataFrame([summary_data])
            df = pd.concat([df, summary_df], ignore_index=True)

            # return df
            return df
        except Exception as e:
            print("Error while get_summarize_parent_asins_data_info_one_country:", e)
        finally:
            self.connect_close()
# res = AmazonMysqlRagUitl('LAPASA', 'DE').get_scan_keyword('DE','2024-08-15')
# print(res)
