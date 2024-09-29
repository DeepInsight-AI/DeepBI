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
    def __init__(self, db, brand, market, log=True):
        super().__init__(db, brand, market, log)

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
        finally:
            self.connect_close()

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
        finally:
            self.connect_close()

    def get_update_budget(self, market, date):
        try:
            conn = self.conn

            query1 = """
SELECT COUNT(*) as count
FROM amazon_campaign_update
WHERE DATE(update_time) = '{}'
AND change_type = 'budget'
AND status = 'success'
AND market = '{}';
                    """.format(date,market)
            df1 = pd.read_sql(query1, con=conn)
            count = df1.loc[0, 'count']
            # return df
            return count
        except Exception as e:
            print("Error while get_update_budget:", e)
        finally:
            self.connect_close()

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
        finally:
            self.connect_close()

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
        finally:
            self.connect_close()

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
        finally:
            self.connect_close()

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
        finally:
            self.connect_close()

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
        finally:
            self.connect_close()

    def get_data_campaign(self, market, date):
        """查找广告活动预算的中间信息上传线上数据库"""
        try:
            conn = self.conn

            query1 = f"""
SELECT
	campaignName,
	bid_adjust
FROM
	budget_info
WHERE
	date = '{date}'
    AND market = '{market}'
GROUP BY
	market,
	brand,
	strategy,
	type,
	campaignId,
	campaignName,
	Reason,
	date
HAVING
	bid_adjust IS NOT NULL
                    """
            df1 = pd.read_sql(query1, con=conn)
            # return df
            return df1['campaignName'].tolist(), df1['bid_adjust'].tolist()
        except Exception as e:
            print("Error while get_data_campaign:", e)
        finally:
            self.connect_close()

    def get_data_sku(self, market, date):
        """查找广告活动预算的中间信息上传线上数据库"""
        try:
            conn = self.conn

            query1 = f"""
SELECT
	campaignName,
	advertisedSku,
	type
FROM
	sku_info
WHERE
	date = '{date}'
    AND market = '{market}'
GROUP BY
	market,
	brand,
	strategy,
	type,
	campaignName,
	adGroupName,
	adId,
	Reason,
	date
                    """
            df1 = pd.read_sql(query1, con=conn)
            # return df
            return df1['campaignName'].tolist(), df1['advertisedSku'].tolist(), df1['type'].tolist()
        except Exception as e:
            print("Error while get_data_sku:", e)
        finally:
            self.connect_close()

    def get_data_campaign_placement(self, market, date):
        """查找广告活动预算的中间信息上传线上数据库"""
        try:
            conn = self.conn

            query1 = f"""
SELECT
	campaignName,
	placementClassification,
	bid_adjust
FROM
	campaign_placement_info
WHERE
	date = '{date}'
    AND market = '{market}'
GROUP BY
	market,
	brand,
	strategy,
	type,
	campaignName,
	campaignId,
	placementClassification,
	Reason,
	date
	HAVING
	bid_adjust IS NOT NULL
                    """
            df1 = pd.read_sql(query1, con=conn)
            # return df
            return df1['campaignName'].tolist(), df1['placementClassification'].tolist(), df1['bid_adjust'].tolist()
        except Exception as e:
            print("Error while get_data_campaign_placement:", e)
        finally:
            self.connect_close()

    def get_data_keyword(self, market, date):
        """查找广告活动预算的中间信息上传线上数据库"""
        try:
            conn = self.conn

            query1 = f"""
SELECT
	campaignName,
	matchType,
	keyword,
	bid_adjust
FROM
	keyword_info
WHERE
	date = '{date}'
    AND market = '{market}'
GROUP BY
	market,
	brand,
	strategy,
	type,
	keyword,
	keywordId,
	campaignName,
	adGroupName,
	matchType,
	reason,
	date
	HAVING
	bid_adjust IS NOT NULL
                    """
            df1 = pd.read_sql(query1, con=conn)
            # return df
            return df1['campaignName'].tolist(), df1['matchType'].tolist(), df1['keyword'].tolist(), df1['bid_adjust'].tolist()
        except Exception as e:
            print("Error while get_data_keyword:", e)
        finally:
            self.connect_close()

    def get_data_automatic_targeting(self, market, date):
        """查找广告活动预算的中间信息上传线上数据库"""
        try:
            conn = self.conn

            query1 = f"""
SELECT
	campaignName,
	keyword,
	bid_adjust
FROM
	automatic_targeting_info
WHERE
	date = '{date}'
    AND market = '{market}'
GROUP BY
	market,
	brand,
	strategy,
	type,
	keyword,
	keywordId,
	campaignName,
	adGroupName,
	reason,
	date
	HAVING
	bid_adjust IS NOT NULL
                    """
            df1 = pd.read_sql(query1, con=conn)
            # return df
            return df1['campaignName'].tolist(), df1['keyword'].tolist(), df1['bid_adjust'].tolist()
        except Exception as e:
            print("Error while get_data_automatic_targeting:", e)
        finally:
            self.connect_close()

    def get_data_product_targets(self, market, date):
        """查找广告活动预算的中间信息上传线上数据库"""
        try:
            conn = self.conn

            query1 = f"""
SELECT
	campaignName,
	keyword,
	bid_adjust
FROM
	product_targets_info
WHERE
	date = '{date}'
    AND market = '{market}'
GROUP BY
	market,
	brand,
	strategy,
	type,
	keyword,
	keywordId,
	campaignName,
	adGroupName,
	reason,
	date
	HAVING
	bid_adjust IS NOT NULL AND
	campaignName IS NOT NULL
                    """
            df1 = pd.read_sql(query1, con=conn)
            # return df
            return df1['campaignName'].tolist(), df1['keyword'].tolist(), df1['bid_adjust'].tolist()
        except Exception as e:
            print("Error while get_data_product_targets:", e)
        finally:
            self.connect_close()

    def get_create_campaign(self, market, date):
        """查找广告活动创建的信息上传线上数据库"""
        try:
            conn = self.conn

            query1 = f"""
SELECT campaign_name,campaign_type,budget FROM amazon_campaign_create
WHERE DATE(create_time) = '{date}'
AND operation_state = 'success'
AND market = '{market}'
                    """
            df1 = pd.read_sql(query1, con=conn)
            # return df
            return df1['campaign_name'].tolist(), df1['campaign_type'].tolist(), df1['budget'].tolist()
        except Exception as e:
            print("Error while get_create_campaign:", e)
        finally:
            self.connect_close()

    def get_create_campaign_batch(self, market, date):
        """查找广告活动创建的信息上传线上数据库"""
        try:
            conn = self.conn

            query1 = f"""
    SELECT campaign_name,campaign_type,budget,campaign_id FROM amazon_campaign_create
    WHERE DATE(create_time) = '{date}'
    AND operation_state = 'success'
    AND market = '{market}'
                        """
            df1 = pd.read_sql(query1, con=conn)
            # return df
            return df1['campaign_name'].tolist(), df1['campaign_type'].tolist(), df1['budget'].tolist(), df1['campaign_id'].tolist()
        except Exception as e:
            print("Error while get_create_campaign:", e)
        finally:
            self.connect_close()

    def get_create_adgroup(self, market, date):
        """查找广告活动创建的信息上传线上数据库"""
        try:
            conn = self.conn

            query1 = f"""
SELECT
	amazon_campaign_create.campaign_name,
	adGroupName,
	defaultBid
FROM
	amazon_adgroups_create
	LEFT JOIN amazon_campaign_create ON amazon_adgroups_create.campaignId = amazon_campaign_create.campaign_id
WHERE
	DATE( update_time ) = '{date}'
	AND adGroupState = 'success'
	AND amazon_adgroups_create.market = '{market}'
                    """
            df1 = pd.read_sql(query1, con=conn)
            # return df
            return df1['campaign_name'].tolist(), df1['adGroupName'].tolist(), df1['defaultBid'].tolist()
        except Exception as e:
            print("Error while get_create_adgroup:", e)
        finally:
            self.connect_close()

    def get_create_adgroup_batch(self, market, date):
        """查找广告活动创建的信息上传线上数据库"""
        try:
            conn = self.conn

            query1 = f"""
SELECT
    amazon_campaign_create.campaign_name,
    adGroupName,
    adGroupId,
    defaultBid
FROM
    amazon_adgroups_create
    LEFT JOIN amazon_campaign_create ON amazon_adgroups_create.campaignId = amazon_campaign_create.campaign_id
WHERE
    DATE( update_time ) = '{date}'
    AND adGroupState = 'success'
    AND amazon_adgroups_create.market = '{market}'
                    """
            df1 = pd.read_sql(query1, con=conn)
            # return df
            return df1['campaign_name'].tolist(), df1['adGroupName'].tolist(), df1['defaultBid'].tolist(), df1['adGroupId'].tolist()
        except Exception as e:
            print("Error while get_create_adgroup:", e)
        finally:
            self.connect_close()

    def get_create_sku(self, market, date):
        """查找广告活动创建的信息上传线上数据库"""
        try:
            conn = self.conn

            query1 = f"""
SELECT
	amazon_campaign_create.campaign_name,
	sku
FROM
	amazon_product_create
	LEFT JOIN amazon_campaign_create ON amazon_product_create.campaignId = amazon_campaign_create.campaign_id
WHERE
	DATE( update_time ) = '{date}'
	AND amazon_product_create.status = 'success'
	AND amazon_product_create.market = '{market}'
                    """
            df1 = pd.read_sql(query1, con=conn)
            # return df
            return df1['campaign_name'].tolist(), df1['sku'].tolist()
        except Exception as e:
            print("Error while get_create_sku:", e)
        finally:
            self.connect_close()

    def get_create_sku_batch(self, market, date):
        """查找广告活动创建的信息上传线上数据库"""
        try:
            conn = self.conn

            query1 = f"""
SELECT
    amazon_campaign_create.campaign_name,
    sku,
    adId
FROM
    amazon_product_create
    LEFT JOIN amazon_campaign_create ON amazon_product_create.campaignId = amazon_campaign_create.campaign_id
WHERE
    DATE( update_time ) = '{date}'
    AND amazon_product_create.status = 'success'
    AND amazon_product_create.market = '{market}'
                    """
            df1 = pd.read_sql(query1, con=conn)
            # return df
            return df1['campaign_name'].tolist(), df1['sku'].tolist(), df1['adId'].tolist()
        except Exception as e:
            print("Error while get_create_sku:", e)
        finally:
            self.connect_close()

    def get_create_keyword(self, market, date):
        """查找广告活动创建的信息上传线上数据库"""
        try:
            conn = self.conn

            query1 = f"""
SELECT
	amazon_campaign_create.campaign_name,
	keywordText_new,
	matchType,
	bid
FROM
	amazon_keyword_create
	LEFT JOIN amazon_campaign_create ON amazon_keyword_create.campaignId = amazon_campaign_create.campaign_id
WHERE
	DATE( amazon_keyword_create.create_time ) = '{date}'
	AND amazon_keyword_create.operation_state = 'success'
	AND amazon_keyword_create.market = '{market}'
                    """
            df1 = pd.read_sql(query1, con=conn)
            # return df
            return df1['campaign_name'].tolist(), df1['matchType'].tolist(), df1['keywordText_new'].tolist(), df1['bid'].tolist()
        except Exception as e:
            print("Error while get_create_keyword:", e)
        finally:
            self.connect_close()

    def get_create_keyword_batch(self, market, date):
        """查找广告活动创建的信息上传线上数据库"""
        try:
            conn = self.conn

            query1 = f"""
SELECT
    amazon_campaign_create.campaign_name,
    keywordText_new,
    matchType,
    bid,
    keywordId
FROM
    amazon_keyword_create
    LEFT JOIN amazon_campaign_create ON amazon_keyword_create.campaignId = amazon_campaign_create.campaign_id
WHERE
    DATE( amazon_keyword_create.create_time ) = '{date}'
    AND amazon_keyword_create.operation_state = 'success'
    AND amazon_keyword_create.market = '{market}'
                    """
            df1 = pd.read_sql(query1, con=conn)
            # return df
            return df1['campaign_name'].tolist(), df1['matchType'].tolist(), df1['keywordText_new'].tolist(), df1['bid'].tolist(), df1['keywordId'].tolist()
        except Exception as e:
            print("Error while get_create_keyword:", e)
        finally:
            self.connect_close()

    def get_create_product_targets(self, market, date):
        """查找广告活动创建的信息上传线上数据库"""
        try:
            conn = self.conn

            query1 = f"""
SELECT
	amazon_campaign_create.campaign_name,
	expression,
	bid
FROM
	amazon_targeting_create
	LEFT JOIN amazon_adgroups_create ON amazon_targeting_create.adGroupId = amazon_adgroups_create.adGroupId
	LEFT JOIN amazon_campaign_create ON amazon_adgroups_create.campaignId = amazon_campaign_create.campaign_id
WHERE
	DATE( amazon_targeting_create.update_time ) = '{date}'
	AND amazon_targeting_create.targetingState = 'success'
	AND amazon_targeting_create.market = '{market}'
                    """
            df1 = pd.read_sql(query1, con=conn)
            # return df
            return df1['campaign_name'].tolist(), df1['expression'].tolist(), df1['bid'].tolist()
        except Exception as e:
            print("Error while get_create_product_targets:", e)
        finally:
            self.connect_close()

    def get_create_product_targets_batch(self, market, date):
        """查找广告活动创建的信息上传线上数据库"""
        try:
            conn = self.conn

            query1 = f"""
SELECT
	amazon_campaign_create.campaign_name,
	expression,
	bid,
	targetId
FROM
	amazon_targeting_create
	LEFT JOIN amazon_adgroups_create ON amazon_targeting_create.adGroupId = amazon_adgroups_create.adGroupId
	LEFT JOIN amazon_campaign_create ON amazon_adgroups_create.campaignId = amazon_campaign_create.campaign_id
WHERE
	DATE( amazon_targeting_create.update_time ) = '{date}'
	AND amazon_targeting_create.targetingState = 'success'
	AND amazon_targeting_create.market = '{market}'
                    """
            df1 = pd.read_sql(query1, con=conn)
            # return df
            return df1['campaign_name'].tolist(), df1['expression'].tolist(), df1['bid'].tolist(), df1['targetId'].tolist()
        except Exception as e:
            print("Error while get_create_product_targets:", e)
        finally:
            self.connect_close()
# # 实例化AmazonMysqlRagUitl类
# util = AmazonMysqlRagUitl('LAPASA')
#
# # 调用方法进行测试
# #result = util.get_new_create_campaign('2024-06-24')
# #result = util.get_update_keyword('JP','2024-06-25')
# #result = util.get_new_sspu(market1, market2, startdate, enddate)
# result = util.get_data_campaign('FR','2024-07-25')
#
# # 打印结果
# print(result)
