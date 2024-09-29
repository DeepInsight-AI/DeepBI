import json

import pymysql
import pandas as pd
from datetime import datetime
import warnings
import os
from ai.backend.util.db.auto_process.db_api import BaseDb
from ai.backend.util.db.configuration.path import get_config_path
from ai.backend.util.db.auto_yzj.utils.trans_to import csv_to_json
from ai.backend.util.db.auto_yzj.path import get_auto_path
from ai.backend.util.db.auto_yzj.日常优化.手动sp广告.SKU优化.query import skuquery_manual
from ai.backend.util.db.auto_yzj.日常优化.手动sp广告.复开SKU.query import reopenSkuQueryManual
from ai.backend.util.db.auto_yzj.日常优化.自动sp广告.SKU优化.query import skuQueryAuto
from ai.backend.util.db.auto_yzj.日常优化.自动sp广告.复开SKU.query import reopenSkuQueryAuto
from ai.backend.util.db.auto_yzj.日常优化.手动sp广告.商品投放搜索词优化.query import ProductTargetsSearchTermQuery
from ai.backend.util.db.auto_yzj.日常优化.手动sp广告.商品投放优化.query import ProductTargetsQuery
from ai.backend.util.db.auto_yzj.日常优化.手动sp广告.预算优化.query import BudgetQueryManual
from ai.backend.util.db.auto_yzj.日常优化.手动sp广告.广告位优化.query import TargetingGroupQueryManual
from ai.backend.util.db.auto_yzj.日常优化.自动sp广告.预算优化.query import BudgetQueryAuto
from ai.backend.util.db.auto_yzj.日常优化.自动sp广告.广告位优化.query import TargetingGroupQueryAuto
from ai.backend.util.db.auto_yzj.日常优化.自动sp广告.自动定位组优化.query import AutomaticTargetingQuery
from ai.backend.util.db.auto_yzj.日常优化.自动sp广告.搜索词优化.query import SearchTermQueryAuto
from ai.backend.util.db.auto_yzj.日常优化.手动sp广告.关键词优化.query import KeywordQuery
from ai.backend.util.db.auto_yzj.日常优化.手动sp广告.搜索词优化.query import SearchTermQueryManual
from ai.backend.util.db.auto_yzj.日常优化.sd广告.预算优化.query import BudgetQuerySD
from ai.backend.util.db.auto_yzj.日常优化.sd广告.关闭SKU.query import SkuQuerySD
from ai.backend.util.db.auto_yzj.日常优化.sd广告.复开SKU.query import reopenSkuQuerySD
from ai.backend.util.db.auto_yzj.日常优化.sd广告.商品投放优化.query import ProductTargetsQuerySD


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

    def preprocessing_sku(self, country, cur_time, version=1.5):

        """"""
        try:
            conn = self.conn
            api = skuquery_manual()
            if version == 1.0:
                query = api.get_query_v1_0(cur_time, country)
            elif version == 1.1:
                query = api.get_query_v1_1(cur_time, country)
            elif version == 1.2:
                query = api.get_query_v1_2(cur_time, country)
            elif version == 1.3:
                query = api.get_query_v1_3(cur_time, country)
            elif version == 1.4:
                query = api.get_query_v1_4(cur_time, country)
            elif version == 1.5:
                query = api.get_query_v1_5(cur_time, country)
            else:
                query = None
            df1 = pd.read_sql(query, con=conn)

            output_filename = '.\日常优化\手动sp广告\SKU优化\预处理.csv'
            df1.to_csv(output_filename, index=False, encoding='utf-8-sig')
            csv_to_json(output_filename)
            # return df
            return print("查询已完成，请查看文件： " + output_filename)

        except Exception as error:
            print("Error while inserting data:", error)

    def preprocessing_sku_reopen(self, country, cur_time, version=1.0):

        """"""
        try:
            conn = self.conn
            api = reopenSkuQueryManual()
            if version == 1.0:
                query = api.get_query_v1_0(cur_time, country)
            elif version == 1.1:
                query = api.get_query_v1_1(cur_time, country)
            else:
                query = None
            df1 = pd.read_sql(query, con=conn)

            output_filename = '.\日常优化\手动sp广告\复开SKU\预处理.csv'
            df1.to_csv(output_filename, index=False, encoding='utf-8-sig')
            csv_to_json(output_filename)
            # return df
            return print("查询已完成，请查看文件： " + output_filename)

        except Exception as error:
            print("Error while inserting data:", error)

    def preprocessing_keyword(self, country, cur_time, version=1.3):

        """"""
        try:
            conn = self.conn
            api = KeywordQuery()
            if version == 1.0:
                query = api.get_query_v1_0(cur_time, country)
            elif version == 1.1:
                query = api.get_query_v1_1(cur_time, country)
            elif version == 1.2:
                query = api.get_query_v1_2(cur_time, country)
            elif version == 1.3 or version == '初阶':
                query = api.get_query_v1_3(cur_time, country)
            else:
                query = None
            df1 = pd.read_sql(query, con=conn)
            output_filename = '.\日常优化\手动sp广告\关键词优化\预处理.csv'
            df1.to_csv(output_filename, index=False, encoding='utf-8-sig')
            csv_to_json(output_filename)
            # return df
            return print("查询已完成，请查看文件： " + output_filename)

        except Exception as error:
            print("Error while inserting data:", error)

    def preprocessing_targeting_group(self, country, cur_time, version=1.3):
        """"""
        try:
            conn = self.conn
            api = TargetingGroupQueryManual()
            if version == 1.0:
                query = api.get_query_v1_0(cur_time, country)
            elif version == 1.1:
                query = api.get_query_v1_1(cur_time, country)
            elif version == 1.2:
                query = api.get_query_v1_2(cur_time, country)
            elif version == 1.3 or version == '初阶':
                query = api.get_query_v1_3(cur_time, country)
            else:
                query = None
            df1 = pd.read_sql(query, con=conn)
            output_filename = '.\日常优化\手动sp广告\广告位优化\预处理.csv'
            df1.to_csv(output_filename, index=False, encoding='utf-8-sig')
            csv_to_json(output_filename)
            # return df
            return print("查询已完成，请查看文件： " + output_filename)

        except Exception as error:
            print("Error while inserting data:", error)

    def preprocessing_search_term(self, country, cur_time, version=1.6):

        """"""
        try:
            conn = self.conn
            api = SearchTermQueryManual()
            if version == 1.0:
                query = api.get_query_v1_0(cur_time, country)
            elif version == 1.1:
                query = api.get_query_v1_1(cur_time, country)
            elif version == 1.2:
                query = api.get_query_v1_2(cur_time, country)
            elif version == 1.3:
                query = api.get_query_v1_3(cur_time, country)
            elif version == 1.4:
                query = api.get_query_v1_4(cur_time, country)
            elif version == 1.5 or version == '初阶':
                query = api.get_query_v1_5(cur_time, country)
            elif version == 1.6:
                query = api.get_query_v1_6(cur_time, country)
            else:
                query = None
            df1 = pd.read_sql(query, con=conn)
            output_filename = os.path.join(get_auto_path(), '日常优化\手动sp广告\搜索词优化\预处理.csv')
            #output_filename = '.\日常优化\手动sp广告\搜索词优化\预处理.csv'
            df1.to_csv(output_filename, index=False, encoding='utf-8-sig')
            csv_to_json(output_filename)
            # return df
            return print("查询已完成，请查看文件： " + output_filename)

        except Exception as error:
            print("Error while inserting data:", error)

    def preprocessing_spkeyword(self, country, cur_time, version=1.3):

        """"""
        try:
            conn = self.conn

            if version == 1.0:
                query = """
WITH a AS (
  SELECT
    campaignName,
    adGroupName,
    SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 15 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales_15d,
    SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_7d
  FROM
    amazon_targeting_reports_sp
  WHERE
    date BETWEEN DATE_SUB('{}', INTERVAL 15 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY)
    AND (campaignName LIKE '%MAN%' OR campaignName LIKE '%手动%' OR campaignName LIKE '%Man%' OR campaignName LIKE '%man%')
    AND market = '{}'
  GROUP BY
    campaignName,
    adGroupName
)
SELECT
  a.campaignName,
  a.adGroupName,
  a.total_sales_15d,
  a.total_clicks_7d,
  b.keyword,
  b.matchType,
  b.keywordBid,
  b.keywordId
FROM
  a
JOIN
  amazon_targeting_reports_sp b ON a.adGroupName = b.adGroupName and a.campaignName=b.campaignName
WHERE
  b.keywordId IN (SELECT keywordId FROM amazon_targeting_reports_sp WHERE campaignStatus = 'ENABLED' AND date = '{}' - INTERVAL 1 DAY AND market = '{}' )
    and b.date= '{}' - INTERVAL 1 DAY
    AND b.keywordId NOT IN (
    SELECT DISTINCT entityId
    FROM amazon_advertising_change_history
    WHERE timestamp >= (UNIX_TIMESTAMP(NOW(3)) - 4 * 24 * 60 * 60) * 1000
    AND entityType = 'KEYWORD'
    AND market = '{}'
  )
        group by b.keywordId;
                """.format(cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,country,cur_time,country,cur_time,country)
            if version == 1.1:
                query = """
            WITH a AS (
              SELECT
                campaignName,
                adGroupName,
                SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 15 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales_15d,
                SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_7d
              FROM
                amazon_targeting_reports_sp b
              JOIN
                amazon_campaigns_list_sp c ON b.campaignId = c.campaignId -- 联接广告活动表，获取广告活动类型
              WHERE
                b.date BETWEEN DATE_SUB('{}', INTERVAL 15 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY)
                AND c.targetingType like '%MAN%'
                AND b.market = '{}'
              GROUP BY
                b.campaignName,
                b.adGroupName
            )
            SELECT
              a.campaignName,
              a.adGroupName,
              a.total_sales_15d,
              a.total_clicks_7d,
              b.keyword,
              b.matchType,
              b.keywordBid,
              b.keywordId
            FROM
              a
            JOIN
              amazon_targeting_reports_sp b ON a.adGroupName = b.adGroupName and a.campaignName=b.campaignName
            WHERE
              b.keywordId IN (SELECT keywordId FROM amazon_targeting_reports_sp WHERE campaignStatus = 'ENABLED' AND date = '{}' - INTERVAL 1 DAY)
                    and b.date= '{}' - INTERVAL 1 DAY
              AND b.keywordId NOT IN (
                SELECT DISTINCT entityId
                FROM amazon_advertising_change_history
                WHERE timestamp >= (UNIX_TIMESTAMP(NOW(3)) - 4 * 24 * 60 * 60) * 1000
                AND entityType = 'KEYWORD'
              )
                    group by b.keywordId;
                            """.format(cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, country, cur_time,
                                       cur_time)
            if version == 1.2:
                query = """
            WITH a AS (
              SELECT
                campaignName,
                adGroupName,
                SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 15 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales_15d,
                SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_7d
              FROM
                amazon_targeting_reports_sp b
              JOIN
                amazon_campaigns_list_sp c ON b.campaignId = c.campaignId -- 联接广告活动表，获取广告活动类型
              WHERE
                b.date BETWEEN DATE_SUB('{}', INTERVAL 15 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY)
                AND c.targetingType like '%MAN%'
                AND b.market = '{}'
              GROUP BY
                b.campaignName,
                b.adGroupName
            )
            SELECT
              a.campaignName,
              a.adGroupName,
              a.total_sales_15d,
              a.total_clicks_7d,
              b.keyword,
              b.matchType,
              b.keywordBid,
              b.keywordId
            FROM
              a
            JOIN
              amazon_targeting_reports_sp b ON a.adGroupName = b.adGroupName and a.campaignName=b.campaignName
            WHERE
              b.keywordId IN (SELECT keywordId FROM amazon_targeting_reports_sp WHERE campaignStatus = 'ENABLED' AND date = '{}' - INTERVAL 1 DAY AND market = '{}' AND campaignName NOT LIKE '%_overstock%')
                    and b.date= '{}' - INTERVAL 1 DAY
                    AND b.matchType not in ('TARGETING_EXPRESSION')
                    group by b.keywordId;
                            """.format(cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, country, cur_time, country,
                                       cur_time, country)
            if version == 1.3:
                query = """
WITH a AS (
SELECT
        b.campaignName,
        b.campaignId,
        b.adGroupName,
        b.adGroupId,
        b.keyword,
        b.keywordId,
        b.matchType,
        purchases7d,
        clicks,
        cost,
        bid,
        date
FROM
        amazon_targeting_reports_sp b
        JOIN amazon_keywords_list_sp c ON b.keywordId = c.keywordId
WHERE
        b.market = '{}'
        AND b.date BETWEEN DATE_SUB( '{}', INTERVAL 15 DAY )
        AND DATE_SUB( '{}', INTERVAL 1 DAY )
 UNION
SELECT
        NULL AS campaignName,
        akl.campaignId,
        NULL AS adGroupName,
        akl.adGroupId,
        akl.keywordText AS keyword,
        akl.keywordId,
        akl.matchType,
        0 AS purchases7d,
        0 AS clicks,
        0 AS cost,
        bid,
        DATE_SUB( '{}', INTERVAL 1 DAY ) AS date
FROM
        amazon_keywords_list_sp akl
WHERE
        akl.market = '{}'
        AND akl.state = 'ENABLED'
        AND akl.extendedData_servingStatus NOT IN ( 'CAMPAIGN_PAUSED', 'AD_GROUP_PAUSED', 'TARGETING_CLAUSE_PAUSED' )
        AND akl.keywordId NOT IN (
        SELECT
                b.keywordId
        FROM
                amazon_targeting_reports_sp b
        WHERE
                b.market = '{}'
                AND b.date BETWEEN DATE_SUB( '{}', INTERVAL 15 DAY )
        AND DATE_SUB( '{}', INTERVAL 1 DAY )
        )
        ORDER BY
        campaignId,
        adGroupId
),
b AS(
SELECT
        campaignName,
        campaignId,
        adGroupName,
        adGroupId,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 15 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS total_purchases7d_15d,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_7d,
                                SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_7d
        FROM
        a
        GROUP BY
        campaignId,
        adGroupId
)
SELECT
        a.campaignName,
        a.campaignId,
        a.adGroupName,
        a.adGroupId,
        a.keywordId,
        a.keyword,
        b.total_purchases7d_15d,
        b.total_clicks_7d,
        b.total_cost_7d,
        a.matchType,
        a.bid
        FROM a
        JOIN b  ON a.campaignId = b.campaignId AND a.adGroupId = b.adGroupId
        GROUP BY
        a.keywordId
        ORDER BY
        a.campaignId,
        a.adGroupId

                            """.format(country,cur_time, cur_time, cur_time,country,country, cur_time, cur_time, cur_time, cur_time,
                                       cur_time, cur_time, cur_time, cur_time)
            else:
                query = None
            df1 = pd.read_sql(query, con=conn)
            output_filename = '.\日常优化\手动sp广告\特殊关键词\预处理.csv'
            df1.to_csv(output_filename, index=False, encoding='utf-8-sig')
            csv_to_json(output_filename)
            # return df
            return print("查询已完成，请查看文件： " + output_filename)

        except Exception as error:
            print("Error while inserting data:", error)

    def preprocessing_budget(self, country, cur_time, version=1.3):
        """"""
        try:
            conn = self.conn
            api = BudgetQueryManual()
            if version == 1.0:
                query = api.get_query_v1_0(cur_time, country)
            elif version == 1.1:
                query = api.get_query_v1_1(cur_time, country)
            elif version == 1.2:
                query = api.get_query_v1_2(cur_time, country)
            elif version == 1.3 or version == '初阶':
                query = api.get_query_v1_3(cur_time, country)
            else:
                query = None
            #print(query)
            df1 = pd.read_sql(query, con=conn)
            output_filename = '.\日常优化\手动sp广告\预算优化\预处理.csv'
            df1.to_csv(output_filename, index=False, encoding='utf-8-sig')
            csv_to_json(output_filename)
            # return df
            return print("查询已完成，请查看文件： " + output_filename)

        except Exception as error:
            print("Error while inserting data:", error)

    def preprocessing_product_targets(self, country, cur_time, version=1.1):

        """"""
        try:
            conn = self.conn
            api = ProductTargetsQuery()
            if version == 1.0:
                query = api.get_query_v1_0(cur_time, country)
            elif version == 1.1 or version == '初阶':
                query = api.get_query_v1_1(cur_time, country)
            else:
                query = None
            # print(query)
            df1 = pd.read_sql(query, con=conn)
            output_filename = '.\日常优化\手动sp广告\商品投放优化\预处理.csv'
            df1.to_csv(output_filename, index=False, encoding='utf-8-sig')
            csv_to_json(output_filename)
            # return df
            return print("查询已完成，请查看文件： " + output_filename)

        except Exception as error:
            print("Error while inserting data:", error)

    def preprocessing_sp_product_targets(self, country, cur_time, version=1.1):

        """"""
        try:
            conn = self.conn

            if version == 1.0:
                query = """
WITH a AS (
    SELECT
        campaignName,
        adGroupName,
        -- 过去15天的总销售额
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 15 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales_15d,
        -- 过去7天的总点击量
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_7d
    FROM
        amazon_targeting_reports_sp b
    JOIN
        amazon_campaigns_list_sp c ON b.campaignId = c.campaignId -- 联接广告活动表，获取广告活动类型
    WHERE
        b.date BETWEEN DATE_SUB('{}', INTERVAL 15 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY)
        AND c.targetingType LIKE '%MAN%' -- 筛选出手动广告
        AND b.market = '{}'
    GROUP BY
        b.campaignName,
        b.adGroupName
)

-- 从CTE结果中选择数据
SELECT
    a.campaignName,
    a.adGroupName,
    a.total_sales_15d,
    a.total_clicks_7d,
    b.keyword,
    b.matchType,
    b.keywordBid,
    b.keywordId
FROM
    a
JOIN
    amazon_targeting_reports_sp b ON a.adGroupName = b.adGroupName
    AND a.campaignName = b.campaignName -- 联接CTE结果和amazon_targeting_reports_sp表
WHERE
    b.keywordId IN (
        SELECT keywordId
        FROM amazon_targeting_reports_sp
        WHERE campaignStatus = 'ENABLED'
        AND date = '{}' - INTERVAL 1 DAY
        AND campaignName LIKE '%_overstock%'
    )
    AND b.market = '{}'
    AND b.date = '{}' - INTERVAL 1 DAY -- 选择固定日期前一天的数据
    -- 筛选 keyword 或 targeting 中包含 asin 或 category 的数据行
    AND (
        b.keyword LIKE '%asin%'
        OR b.targeting LIKE '%asin%'
        OR b.keyword LIKE '%category%'
        OR b.targeting LIKE '%category%'
        OR a.campaignName LIKE '%ASIN%'
    )
    -- 排除在 amazon_targeting_reports_sd 中的 campaignId
    AND b.campaignId NOT IN (
        SELECT DISTINCT campaignId
        FROM amazon_targeting_reports_sd
    )
GROUP BY
    b.keywordId;
                """.format(cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, country, cur_time, country, cur_time, country)
            if version == 1.1:
                query =f"""
WITH a AS (
    SELECT
        b.campaignName,
        b.campaignId,
        b.adGroupName,
        b.adGroupId,
        b.keyword,
        b.keywordId,
        purchases7d,
        clicks,
        cost,
        bid,
        date
    FROM
        amazon_targeting_reports_sp b
        JOIN amazon_targets_list_sp c ON b.keywordId = c.targetId
    WHERE
        b.market = '{country}'
        AND b.date BETWEEN DATE_SUB('{cur_time}', INTERVAL 15 DAY)
        AND DATE_SUB('{cur_time}', INTERVAL 1 DAY)
        AND expressionType = 'MANUAL'
    UNION
    SELECT
        NULL AS campaignName,
        akl.campaignId,
        NULL AS adGroupName,
        akl.adGroupId,
        akl.expression AS keyword,
        akl.targetId AS keywordId,
        0 AS purchases7d,
        0 AS clicks,
        0 AS cost,
        bid,
        DATE_SUB('{cur_time}', INTERVAL 1 DAY) AS date
    FROM
        amazon_targets_list_sp akl
    WHERE
        akl.market = '{country}'
        AND akl.state = 'ENABLED'
        AND akl.servingStatus NOT IN ('CAMPAIGN_PAUSED', 'AD_GROUP_PAUSED', 'TARGETING_CLAUSE_PAUSED')
        AND expressionType = 'MANUAL'
        AND akl.targetId NOT IN (
            SELECT
                b.keywordId
            FROM
                amazon_targeting_reports_sp b
            WHERE
                b.market = '{country}'
                AND b.date BETWEEN DATE_SUB('{cur_time}', INTERVAL 15 DAY)
                AND DATE_SUB('{cur_time}', INTERVAL 1 DAY)
        )
    ORDER BY
        campaignId,
        adGroupId
),
b AS (
    SELECT
        campaignName,
        campaignId,
        adGroupName,
        adGroupId,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}', INTERVAL 15 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS total_purchases7d_15d,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}', INTERVAL 7 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_7d,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}', INTERVAL 7 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_7d
    FROM
        a
    GROUP BY
        campaignId,
        adGroupId
)
SELECT
    a.campaignName,
    a.campaignId,
    a.adGroupName,
    a.adGroupId,
    a.keywordId,
    CASE
        WHEN a.keyword LIKE "%'type': 'ASIN_SAME_AS', 'value': %" THEN CONCAT('asin="', TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(a.keyword, "'", -2), "'", 1)), '"')
        WHEN a.keyword LIKE "%'type': 'ASIN_EXPANDED_FROM', 'value': %" THEN CONCAT('asin-expanded="', TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(a.keyword, "'", -2), "'", 1)), '"')
        WHEN a.keyword LIKE "%'type': 'ASIN_CATEGORY_SAME_AS', 'value': %" THEN CONCAT('category="', TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(a.keyword, "'", -2), "'", 1)), '"')
        WHEN a.keyword LIKE "%'type': 'ASIN_CATEGORY_SAME_AS', 'value': %" AND a.keyword LIKE "%, 'type': 'ASIN_BRAND_SAME_AS', 'value': %" THEN
            CONCAT('category="', TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(a.keyword, "'", -2), "'", 2)), '" ',
                   'brand="', TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(a.keyword, "'", -8), "'", 2)), '"')
        ELSE a.keyword
    END AS keyword,
    b.total_purchases7d_15d,
    b.total_clicks_7d,
    b.total_cost_7d,
    a.bid
FROM
    a
JOIN b ON a.campaignId = b.campaignId AND a.adGroupId = b.adGroupId
GROUP BY
    a.keywordId
ORDER BY
    a.campaignId,
    a.adGroupId;
                """
            else:
                query = None
            # print(query)
            df1 = pd.read_sql(query, con=conn)
            output_filename = '.\日常优化\手动sp广告\特殊商品投放\预处理.csv'
            df1.to_csv(output_filename, index=False, encoding='utf-8-sig')
            csv_to_json(output_filename)
            # return df
            return print("查询已完成，请查看文件： " + output_filename)

        except Exception as error:
            print("Error while inserting data:", error)

    def preprocessing_product_targets_search_term(self, country, cur_time, version=1.2):
        """"""
        try:
            conn = self.conn
            api = ProductTargetsSearchTermQuery()
            if version == 1.0:
                query = api.get_query_v1_0(cur_time, country)
            elif version == 1.1:
                query = api.get_query_v1_1(cur_time, country)
            elif version == 1.2 or version == '初阶':
                query = api.get_query_v1_2(cur_time, country)
            else:
                query = None
            # print(query)
            df1 = pd.read_sql(query, con=conn)
            output_filename = os.path.join(get_auto_path(), '日常优化\手动sp广告\商品投放搜索词优化\预处理.csv')
            # output_filename = '.\日常优化\手动sp广告\商品投放搜索词优化\预处理.csv'
            df1.to_csv(output_filename, index=False, encoding='utf-8-sig')
            csv_to_json(output_filename)
            # return df
            return print("查询已完成，请查看文件： " + output_filename)

        except Exception as error:
            print("Error while inserting data:", error)

    def preprocessing_sku_auto(self, country, cur_time, version=1.3):

        """"""
        try:
            conn = self.conn
            api = skuQueryAuto()
            if version == 1.0:
                query = api.get_query_v1_0(cur_time, country)
            elif version == 1.1:
                query = api.get_query_v1_1(cur_time, country)
            elif version == 1.2:
                query = api.get_query_v1_2(cur_time, country)
            elif version == 1.3:
                query = api.get_query_v1_3(cur_time, country)
            elif version == 1.4:
                query = api.get_query_v1_4(cur_time, country)
            else:
                query = None
            df1 = pd.read_sql(query, con=conn)

            output_filename = '.\日常优化\自动sp广告\SKU优化\预处理.csv'
            df1.to_csv(output_filename, index=False, encoding='utf-8-sig')
            csv_to_json(output_filename)
            # return df
            return print("查询已完成，请查看文件： " + output_filename)

        except Exception as error:
            print("Error while inserting data:", error)

    def preprocessing_sku_auto_reopen(self, country, cur_time, version=1.0):

        """"""
        try:
            conn = self.conn
            api = reopenSkuQueryAuto()
            if version == 1.0:
                query = api.get_query_v1_0(cur_time, country)
            elif version == 1.1:
                query = api.get_query_v1_1(cur_time, country)
            else:
                query = None
            df1 = pd.read_sql(query, con=conn)

            output_filename = '.\日常优化\自动sp广告\复开SKU\预处理.csv'
            df1.to_csv(output_filename, index=False, encoding='utf-8-sig')
            csv_to_json(output_filename)
            # return df
            return print("查询已完成，请查看文件： " + output_filename)

        except Exception as error:
            print("Error while inserting data:", error)

    def preprocessing_targeting_group_auto(self, country, cur_time, version=1.3):

        """"""
        try:
            conn = self.conn
            api = TargetingGroupQueryAuto()
            if version == 1.0:
                query = api.get_query_v1_0(cur_time, country)
            elif version == 1.1:
                query = api.get_query_v1_1(cur_time, country)
            elif version == 1.2:
                query = api.get_query_v1_2(cur_time, country)
            elif version == 1.3 or version == '初阶':
                query = api.get_query_v1_3(cur_time, country)
            else:
                query = None
            df1 = pd.read_sql(query, con=conn)
            output_filename = '.\日常优化\自动sp广告\广告位优化\预处理.csv'
            df1.to_csv(output_filename, index=False, encoding='utf-8-sig')
            csv_to_json(output_filename)
            # return df
            return print("查询已完成，请查看文件： " + output_filename)

        except Exception as error:
            print("Error while inserting data:", error)

    def preprocessing_search_term_auto(self, country, cur_time, version=1.3):

        """"""
        try:
            conn = self.conn

            api = SearchTermQueryAuto()
            if version == 1.0:
                query = api.get_query_v1_0(cur_time, country)
            elif version == 1.1:
                query = api.get_query_v1_1(cur_time, country)
            elif version == 1.2:
                query = api.get_query_v1_2(cur_time, country)
            elif version == 1.3 or version == '初阶':
                query = api.get_query_v1_3(cur_time, country)
            else:
                query = None
            df1 = pd.read_sql(query, con=conn)

            output_filename = os.path.join(get_auto_path(), '日常优化\自动sp广告\搜索词优化\预处理.csv')
            # output_filename = '.\日常优化\自动sp广告\搜索词优化\预处理.csv'
            df1.to_csv(output_filename, index=False, encoding='utf-8-sig')
            csv_to_json(output_filename)
            # return df
            return print("查询已完成，请查看文件： " + output_filename)

        except Exception as error:
            print("Error while inserting data:", error)

    def preprocessing_automatic_targeting_auto(self, country, cur_time, version=1.3):

        """"""
        try:
            conn = self.conn
            api = AutomaticTargetingQuery()
            if version == 1.0:
                query = api.get_query_v1_0(cur_time, country)
            elif version == 1.1:
                query = api.get_query_v1_1(cur_time, country)
            elif version == 1.2:
                query = api.get_query_v1_2(cur_time, country)
            elif version == 1.3 or version == '初阶':
                query = api.get_query_v1_3(cur_time, country)
            else:
                query = None
            df1 = pd.read_sql(query, con=conn)
            output_filename = '.\日常优化\自动sp广告\自动定位组优化\预处理.csv'
            df1.to_csv(output_filename, index=False, encoding='utf-8-sig')
            csv_to_json(output_filename)
            # return df
            return print("查询已完成，请查看文件： " + output_filename)

        except Exception as error:
            print("Error while inserting data:", error)

    def preprocessing_sp_automatic_targeting_auto(self, country, cur_time, version=1.2):

        """"""
        try:
            conn = self.conn

            if version == 1.0:
                query = """
WITH a AS (
  SELECT
    campaignName,
    keywordId,
    adGroupName,
    adGroupId,
    SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 15 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales_15d,
    SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_7d
  FROM
    amazon_targeting_reports_sp
  WHERE
    date BETWEEN DATE_SUB('{}', INTERVAL 15 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY)
    AND (campaignName LIKE '%MAN%' OR campaignName LIKE '%手动%' OR campaignName LIKE '%Man%' OR campaignName LIKE '%man%')
    AND market = '{}'
  GROUP BY
    campaignName,
    adGroupName
)
SELECT
  a.campaignName,
  a.keywordId,
  a.adGroupName,
  a.adGroupId,
  a.total_sales_15d,
  a.total_clicks_7d,
  b.keyword,
  b.matchType,
  b.keywordBid,
  b.keywordId
FROM
  a
JOIN
  amazon_targeting_reports_sp b ON a.adGroupName = b.adGroupName and a.campaignName=b.campaignName
WHERE
  b.keywordId IN (SELECT keywordId FROM amazon_targeting_reports_sp WHERE campaignStatus = 'ENABLED' AND date = '{}' - INTERVAL 1 DAY)
        and b.date= '{}' - INTERVAL 1 DAY
  AND b.keywordId NOT IN (
    SELECT DISTINCT entityId
    FROM amazon_advertising_change_history
    WHERE timestamp >= (UNIX_TIMESTAMP(NOW(3)) - 4 * 24 * 60 * 60) * 1000
    AND entityType = 'KEYWORD'
    AND market = '{}'
  )
        group by b.keywordId;
                """.format(cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, country, cur_time,
                           country)
            if version == 1.1:
                query = """
            WITH a AS (
        SELECT
        campaignName,
        adGroupName,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 15 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales_15d,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_7d
        FROM
                amazon_targeting_reports_sp b
                JOIN amazon_campaigns_list_sp c ON b.campaignId = c.campaignId
        WHERE
                b.date BETWEEN DATE_SUB( '{}', INTERVAL 15 DAY )
                AND DATE_SUB( '{}', INTERVAL 1 DAY )
                AND c.targetingType LIKE '%AUT%'
                AND b.market = '{}'
        GROUP BY
                b.campaignName,
                b.adGroupName
        ) SELECT
        a.campaignName,
        a.adGroupName,
        a.total_sales_15d,
        a.total_clicks_7d,
        b.keyword,
        b.matchType,
        b.keywordBid,
        b.keywordId
FROM
        a
        JOIN amazon_targeting_reports_sp b ON a.adGroupName = b.adGroupName
        AND a.campaignName = b.campaignName
WHERE
        b.keywordId IN ( SELECT keywordId FROM amazon_targeting_reports_sp WHERE campaignStatus = 'ENABLED' AND date = '{}' - INTERVAL 1 DAY AND market = '{}' AND campaignName NOT LIKE '%_overstock%')
        AND b.date = '{}' - INTERVAL 1 DAY
GROUP BY
        b.keywordId;
                            """.format(cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, country,
                                       cur_time, country, cur_time, country)
            if version == 1.2:
                query = """
WITH a AS (
SELECT
        b.campaignName,
        b.campaignId,
        b.adGroupName,
        b.adGroupId,
        b.keyword,
        b.keywordId,
        purchases7d,
        clicks,
        cost,
        bid,
        date
FROM
        amazon_targeting_reports_sp b
        JOIN amazon_targets_list_sp c ON b.keywordId = c.targetId
WHERE
        b.market = '{}'
        AND b.date BETWEEN DATE_SUB( '{}', INTERVAL 15 DAY )
        AND DATE_SUB( '{}', INTERVAL 1 DAY )
                                and expressionType='AUTO'
 UNION
SELECT
        NULL AS campaignName,
        akl.campaignId,
        NULL AS adGroupName,
        akl.adGroupId,
         (CASE
        WHEN resolvedExpression like '%QUERY_HIGH_REL_MATCHES%' THEN 'close match'
        WHEN resolvedExpression like '%QUERY_BROAD_REL_MATCHES%' THEN 'loose match'
        WHEN resolvedExpression like '%ASIN_ACCESSORY_RELATED%' THEN 'complements'
        WHEN resolvedExpression like '%ASIN_SUBSTITUTE_RELATED%' THEN 'substitute'
    END) AS keyword,
        akl.targetId as keywordId,
        0 AS purchases7d,
        0 AS clicks,
        0 AS cost,
        bid,
        DATE_SUB( '{}', INTERVAL 1 DAY ) AS date
FROM
        amazon_targets_list_sp akl
WHERE
        akl.market = '{}'
        AND akl.state = 'ENABLED'
        AND akl.servingStatus NOT IN ( 'CAMPAIGN_PAUSED', 'AD_GROUP_PAUSED', 'TARGETING_CLAUSE_PAUSED' )
                                and expressionType='AUTO'
        AND akl.targetId NOT IN (
        SELECT
                b.keywordId
        FROM
                amazon_targeting_reports_sp b
        WHERE
                b.market = '{}'
                AND b.date BETWEEN DATE_SUB( '{}', INTERVAL 15 DAY )
        AND DATE_SUB( '{}', INTERVAL 1 DAY )
        )
        ORDER BY
        campaignId,
        adGroupId
),
b AS(
SELECT
        campaignName,
        campaignId,
        adGroupName,
        adGroupId,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 15 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS total_purchases7d_15d,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_7d,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_7d
        FROM
        a
        GROUP BY
        campaignId,
        adGroupId
)
SELECT
        a.campaignName,
        a.campaignId,
        a.adGroupName,
        a.adGroupId,
        a.keywordId,
        a.keyword,
        b.total_purchases7d_15d,
        b.total_clicks_7d,
        b.total_cost_7d,
        a.bid

        FROM a
        JOIN b  ON a.campaignId = b.campaignId AND a.adGroupId = b.adGroupId
        GROUP BY
        a.keywordId
        ORDER BY
        a.campaignId,
        a.adGroupId


                """.format(country,cur_time, cur_time, cur_time,country,country, cur_time, cur_time, cur_time, cur_time,
                                       cur_time, cur_time, cur_time, cur_time)
            else:
                query = None
            df1 = pd.read_sql(query, con=conn)
            output_filename = '.\日常优化\自动sp广告\特殊自动定位组\预处理.csv'
            df1.to_csv(output_filename, index=False, encoding='utf-8-sig')
            csv_to_json(output_filename)
            # return df
            return print("查询已完成，请查看文件： " + output_filename)

        except Exception as error:
            print("Error while inserting data:", error)

    def preprocessing_budget_auto(self, country, cur_time, version=1.3):

        """"""
        try:
            conn = self.conn

            api = BudgetQueryAuto()
            if version == 1.0:
                query = api.get_query_v1_0(cur_time, country)
            elif version == 1.1:
                query = api.get_query_v1_1(cur_time, country)
            elif version == 1.2:
                query = api.get_query_v1_2(cur_time, country)
            elif version == 1.3 or version == '初阶':
                query = api.get_query_v1_3(cur_time, country)
            else:
                query = None
            df1 = pd.read_sql(query, con=conn)
            output_filename = '.\日常优化\自动sp广告\预算优化\预处理.csv'
            df1.to_csv(output_filename, index=False, encoding='utf-8-sig')
            csv_to_json(output_filename)
            # return df
            return print("查询已完成，请查看文件： " + output_filename)

        except Exception as error:
            print("Error while inserting data:", error)

    def preprocessing_campaign_anomaly_detection(self, country, cur_time, version=1.1):

        """"""
        try:
            conn = self.conn
            if version == 1.0:
                query = """
    WITH a AS (
    SELECT
        campaignName,
        campaignId,
        SUM( CASE WHEN date BETWEEN DATE_SUB( '{}' - INTERVAL 1 DAY, INTERVAL 29 DAY ) AND '{}'- INTERVAL 1 DAY THEN sales14d ELSE 0 END ) AS total_sales14d_1m,
        SUM( CASE WHEN date BETWEEN DATE_SUB( '{}' - INTERVAL 1 DAY, INTERVAL 6 DAY ) AND '{}'- INTERVAL 1 DAY THEN sales14d ELSE 0 END ) AS total_sales14d_7d,
        SUM( CASE WHEN date BETWEEN DATE_SUB( '{}' - INTERVAL 1 DAY, INTERVAL 29 DAY ) AND '{}'- INTERVAL 1 DAY THEN cost ELSE 0 END ) AS total_cost_30d,
        SUM( CASE WHEN date BETWEEN DATE_SUB( '{}' - INTERVAL 1 DAY, INTERVAL 6 DAY ) AND '{}'- INTERVAL 1 DAY THEN cost ELSE 0 END ) AS total_cost_7d,
        SUM( CASE WHEN date BETWEEN DATE_SUB( '{}' - INTERVAL 1 DAY, INTERVAL 29 DAY ) AND '{}'- INTERVAL 1 DAY THEN cost ELSE 0 END ) / NULLIF(
            SUM( CASE WHEN date BETWEEN DATE_SUB( '{}' - INTERVAL 1 DAY, INTERVAL 29 DAY ) AND '{}'- INTERVAL 1 DAY THEN sales14d ELSE 0 END ),
            0
        ) AS ACOS_30d,
        SUM( CASE WHEN date BETWEEN DATE_SUB( '{}' - INTERVAL 1 DAY, INTERVAL 6 DAY ) AND '{}'- INTERVAL 1 DAY THEN cost ELSE 0 END ) / NULLIF(
            SUM( CASE WHEN date BETWEEN DATE_SUB( '{}' - INTERVAL 1 DAY, INTERVAL 6 DAY ) AND '{}'- INTERVAL 1 DAY THEN sales14d ELSE 0 END ),
            0
        ) AS ACOS_7d,
        SUM( CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END ) / NULLIF( SUM( CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END ), 0 ) AS ACOS_yesterday
    FROM
        amazon_campaign_reports_sp
    where campaignId in (
    SELECT campaignId from amazon_campaign_reports_sp
    where campaignStatus = 'ENABLED'
    and date = '{}'- INTERVAL 1 DAY
    and market = '{}'
    )
    and date BETWEEN '{}'- INTERVAL 30 DAY and '{}'- INTERVAL 1 DAY
    and market = '{}'

    GROUP BY
        campaignName)
    SELECT
      a.*,
        b.impressions AS impressions_yesterday,
        b.clicks AS clicks_yesterday,
        b.cost AS cost_yesterday,
        b.campaignBudgetAmount,
        b.sales14d AS sales14d_yesterday

    FROM
        a
    LEFT JOIN
        amazon_campaign_reports_sp b ON a.campaignId = b.campaignId
    WHERE
        b.date = DATE_SUB('{}', INTERVAL 2 DAY)
                    """.format(cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,country,cur_time,cur_time,country,cur_time)
            elif version == 1.1:
                query ="""
                SELECT
        campaignName,
  campaignId,
        cost,
        campaignBudgetAmount,
        sales14d as sales,
        ROUND((cost/sales14d), 2) as ACOS,
        purchases14d as purchases
FROM
        amazon_campaign_reports_sp
WHERE
        campaignStatus = 'ENABLED' AND
        date = '{}' AND
        market = '{}'
                """.format(cur_time,country)

            df1 = pd.read_sql(query, con=conn)

            output_filename = '.\日常优化\异常定位检测\广告活动\预处理.csv'
            df1.to_csv(output_filename, index=False, encoding='utf-8-sig')
            csv_to_json(output_filename)
            # return df
            return print("查询已完成，请查看文件： " + output_filename)

        except Exception as error:
            print("Error while inserting data:", error)

    def preprocessing_targeting_group_anomaly_detection(self, country, cur_time, version=1.1):

        """"""
        try:
            conn = self.conn

            if version == 1.0:
                query = """
    WITH a AS (
    SELECT
        campaignName,
        campaignId,
        placementClassification,
        SUM( CASE WHEN date BETWEEN DATE_SUB( '{}' - INTERVAL 1 DAY, INTERVAL 29 DAY ) AND '{}'- INTERVAL 1 DAY THEN sales14d ELSE 0 END ) AS total_sales14d_1m,
        SUM( CASE WHEN date BETWEEN DATE_SUB( '{}' - INTERVAL 1 DAY, INTERVAL 6 DAY ) AND '{}'- INTERVAL 1 DAY THEN sales14d ELSE 0 END ) AS total_sales14d_7d,
        SUM( CASE WHEN date BETWEEN DATE_SUB( '{}' - INTERVAL 1 DAY, INTERVAL 29 DAY ) AND '{}'- INTERVAL 1 DAY THEN cost ELSE 0 END ) AS total_cost_30d,
        SUM( CASE WHEN date BETWEEN DATE_SUB( '{}' - INTERVAL 1 DAY, INTERVAL 6 DAY ) AND '{}'- INTERVAL 1 DAY THEN cost ELSE 0 END ) AS total_cost_7d,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_30d,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_7d,
        SUM( CASE WHEN date BETWEEN DATE_SUB( '{}' - INTERVAL 1 DAY, INTERVAL 29 DAY ) AND '{}'- INTERVAL 1 DAY THEN cost ELSE 0 END ) / NULLIF(
        SUM( CASE WHEN date BETWEEN DATE_SUB( '{}' - INTERVAL 1 DAY, INTERVAL 29 DAY ) AND '{}'- INTERVAL 1 DAY THEN sales14d ELSE 0 END ),
            0
        ) AS ACOS_30d,
        SUM( CASE WHEN date BETWEEN DATE_SUB( '{}' - INTERVAL 1 DAY, INTERVAL 6 DAY ) AND '{}'- INTERVAL 1 DAY THEN cost ELSE 0 END ) / NULLIF(
            SUM( CASE WHEN date BETWEEN DATE_SUB( '{}' - INTERVAL 1 DAY, INTERVAL 6 DAY ) AND '{}'- INTERVAL 1 DAY THEN sales14d ELSE 0 END ),
            0
        ) AS ACOS_7d,
        SUM( CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END ) / NULLIF( SUM( CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END ), 0 ) AS ACOS_yesterday
    FROM
        amazon_campaign_placement_reports_sp
    where campaignId in (
    SELECT campaignId from amazon_campaign_reports_sp
    where campaignStatus = 'ENABLED'
    and date = '{}'- INTERVAL 1 DAY
    and market = '{}'
    )
    and date BETWEEN '{}'- INTERVAL 30 DAY and '{}'- INTERVAL 1 DAY
    and market = '{}'

    GROUP BY
        campaignName,
        placementClassification)
    SELECT
      a.*,
        b.impressions AS impressions_yesterday,
        b.clicks AS clicks_yesterday,
        b.cost AS cost_yesterday,
        b.campaignBudgetAmount,
        b.sales14d AS sales14d_yesterday

    FROM
        a
    LEFT JOIN
        amazon_campaign_placement_reports_sp b ON a.campaignId = b.campaignId AND a.placementClassification = b.placementClassification
    WHERE
        b.date = DATE_SUB('{}', INTERVAL 2 DAY)
                    """.format(cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,country,cur_time,cur_time,country,cur_time)
            elif version == 1.1:
                query = """
SELECT
    a.campaignName,
    a.campaignId,
    a.placementClassification,
    ROUND(SUM(a.cost) / SUM(a.sales14d), 2) as ACOS, -- 计算最近七天的ACOS并保留两位小数
    SUM(a.sales14d) as total_sales, -- 计算最近七天的总销售额
    COALESCE(
        CASE
            WHEN a.placementClassification = 'Detail Page on-Amazon' THEN c.dynamicBidding_placementProductPage_percentage
            WHEN a.placementClassification = 'Other on-Amazon' THEN c.dynamicBidding_placementRestOfSearch_percentage
            WHEN a.placementClassification = 'Top of Search on-Amazon' THEN c.dynamicBidding_placementTop_percentage
        END,
        0
    ) AS bid
FROM
    (
        SELECT
            campaignName,
            campaignId,
            placementClassification,
            cost,
            sales14d
        FROM
            amazon_campaign_placement_reports_sp
        WHERE
            market = '{}'
            AND date >= DATE_SUB('{}', INTERVAL 7 DAY) -- 最近七天的日期条件
            AND date <= '{}' -- 指定日期及之前的数据
    ) a
JOIN
    (
        SELECT
            campaignId,
            targetingType,
            dynamicBidding_placementTop_percentage,
            dynamicBidding_placementProductPage_percentage,
            dynamicBidding_placementRestOfSearch_percentage
        FROM
            amazon_campaigns_list_sp
    ) c ON a.campaignId = c.campaignId
WHERE
    a.campaignId IN (
        SELECT
            campaignId
        FROM
            amazon_advertised_product_reports_sp
        WHERE
            campaignStatus = 'ENABLED'
            AND date >= DATE_SUB('{}', INTERVAL 7 DAY) -- 最近七天的日期条件
            AND date <= '{}' -- 指定日期及之前的数据
    )
GROUP BY
    a.campaignName,
    a.campaignId,
    a.placementClassification,
    c.dynamicBidding_placementTop_percentage,
    c.dynamicBidding_placementProductPage_percentage,
    c.dynamicBidding_placementRestOfSearch_percentage;
                            """.format(country,cur_time,cur_time,cur_time,cur_time)

            df1 = pd.read_sql(query, con=conn)
            output_filename = '.\日常优化\异常定位检测\广告位\预处理.csv'
            df1.to_csv(output_filename, index=False, encoding='utf-8-sig')
            csv_to_json(output_filename)
            # return df
            return print("查询已完成，请查看文件： " + output_filename)

        except Exception as error:
            print("Error while inserting data:", error)

    def preprocessing_sku_anomaly_detection(self, country, cur_time,campaign_ids=None, version=1.1):

        """"""
        try:
            conn = self.conn

            if version == 1.0:
                query = """
    WITH a AS (
    SELECT
        campaignName,
        campaignId,
        adGroupName,
        advertisedSku,
        SUM( CASE WHEN date BETWEEN DATE_SUB( '{}' - INTERVAL 1 DAY, INTERVAL 29 DAY ) AND '{}'- INTERVAL 1 DAY THEN sales14d ELSE 0 END ) AS total_sales14d_1m,
        SUM( CASE WHEN date BETWEEN DATE_SUB( '{}' - INTERVAL 1 DAY, INTERVAL 6 DAY ) AND '{}'- INTERVAL 1 DAY THEN sales14d ELSE 0 END ) AS total_sales14d_7d,
        SUM( CASE WHEN date BETWEEN DATE_SUB( '{}' - INTERVAL 1 DAY, INTERVAL 29 DAY ) AND '{}'- INTERVAL 1 DAY THEN cost ELSE 0 END ) AS total_cost_30d,
        SUM( CASE WHEN date BETWEEN DATE_SUB( '{}' - INTERVAL 1 DAY, INTERVAL 6 DAY ) AND '{}'- INTERVAL 1 DAY THEN cost ELSE 0 END ) AS total_cost_7d,
            SUM( CASE WHEN date BETWEEN DATE_SUB( '{}' - INTERVAL 1 DAY, INTERVAL 29 DAY ) AND '{}'- INTERVAL 1 DAY THEN purchases7d ELSE 0 END ) AS total_purchases7d_30d,
        SUM( CASE WHEN date BETWEEN DATE_SUB( '{}' - INTERVAL 1 DAY, INTERVAL 6 DAY ) AND '{}'- INTERVAL 1 DAY THEN purchases7d ELSE 0 END ) AS total_purchases7d_7d,
          SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_30d,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_7d,
        SUM( CASE WHEN date BETWEEN DATE_SUB( '{}' - INTERVAL 1 DAY, INTERVAL 29 DAY ) AND '{}'- INTERVAL 1 DAY THEN cost ELSE 0 END ) / NULLIF(
        SUM( CASE WHEN date BETWEEN DATE_SUB( '{}' - INTERVAL 1 DAY, INTERVAL 29 DAY ) AND '{}'- INTERVAL 1 DAY THEN sales14d ELSE 0 END ),
            0
        ) AS ACOS_30d,
        SUM( CASE WHEN date BETWEEN DATE_SUB( '{}' - INTERVAL 1 DAY, INTERVAL 6 DAY ) AND '{}'- INTERVAL 1 DAY THEN cost ELSE 0 END ) / NULLIF(
            SUM( CASE WHEN date BETWEEN DATE_SUB( '{}' - INTERVAL 1 DAY, INTERVAL 6 DAY ) AND '{}'- INTERVAL 1 DAY THEN sales14d ELSE 0 END ),
            0
        ) AS ACOS_7d,
        SUM( CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END ) / NULLIF( SUM( CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END ), 0 ) AS ACOS_yesterday
    FROM
        amazon_advertised_product_reports_sp
    where campaignId in {}
    and date BETWEEN '{}'- INTERVAL 30 DAY and '{}'- INTERVAL 1 DAY
    and market = '{}'

    GROUP BY
        campaignName,
        adGroupName,
        advertisedSku)
    SELECT
      a.*,
        b.impressions AS impressions_yesterday,
        b.clicks AS clicks_yesterday,
        b.cost AS cost_yesterday,
        b.purchases7d AS purchases7d_yesterday,
        b.sales14d AS sales14d_yesterday

    FROM
        a
    LEFT JOIN
        amazon_advertised_product_reports_sp b ON a.campaignId = b.campaignId AND a.adGroupName = b.adGroupName AND a.advertisedSku = b.advertisedSku
    WHERE
        b.date = DATE_SUB('{}', INTERVAL 2 DAY)
                    """.format(cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,campaign_ids,cur_time,cur_time,country,cur_time)
            elif version == 1.1:
                query = """
SELECT DISTINCT
       campaignName,
       adGroupName,
       advertisedSku,
       sum(clicks) as clicks30d,
        sum(cost) as cost30d,
        sum(sales14d) as sales30d,
        ROUND((sum(cost) / sum(sales14d)), 2) as ACOS,
        sum(purchases14d) as purchases30d
FROM
        amazon_advertised_product_reports_sp
WHERE
        campaignStatus = 'ENABLED' AND
        date between  '{}'- INTERVAL 30 DAY and '{}'- INTERVAL 1 DAY
         AND
        market = '{}'
GROUP BY
        campaignName,
        adGroupName,
        advertisedSku
                                        """.format(cur_time, cur_time, country)

            df1 = pd.read_sql(query, con=conn)
            output_filename = '.\日常优化\异常定位检测\商品\预处理.csv'
            df1.to_csv(output_filename, index=False, encoding='utf-8-sig')
            csv_to_json(output_filename)
            # return df
            return print("查询已完成，请查看文件： " + output_filename)

        except Exception as error:
            print("Error while inserting data:", error)

    def preprocessing_targeting_anomaly_detection(self, country, cur_time,campaign_ids, version=1.1):

        """"""
        try:
            conn = self.conn

            if version == 1.0:
                query = """
    WITH a AS (
    SELECT
        campaignName,
        campaignId,
        adGroupName,
        targeting,
        matchType,
        SUM( CASE WHEN date BETWEEN DATE_SUB( '{}' - INTERVAL 1 DAY, INTERVAL 29 DAY ) AND '{}'- INTERVAL 1 DAY THEN sales14d ELSE 0 END ) AS total_sales14d_1m,
        SUM( CASE WHEN date BETWEEN DATE_SUB( '{}' - INTERVAL 1 DAY, INTERVAL 6 DAY ) AND '{}'- INTERVAL 1 DAY THEN sales14d ELSE 0 END ) AS total_sales14d_7d,
        SUM( CASE WHEN date BETWEEN DATE_SUB( '{}' - INTERVAL 1 DAY, INTERVAL 29 DAY ) AND '{}'- INTERVAL 1 DAY THEN cost ELSE 0 END ) AS total_cost_30d,
        SUM( CASE WHEN date BETWEEN DATE_SUB( '{}' - INTERVAL 1 DAY, INTERVAL 6 DAY ) AND '{}'- INTERVAL 1 DAY THEN cost ELSE 0 END ) AS total_cost_7d,
        SUM( CASE WHEN date BETWEEN DATE_SUB( '{}' - INTERVAL 1 DAY, INTERVAL 29 DAY ) AND '{}'- INTERVAL 1 DAY THEN purchases7d ELSE 0 END ) AS total_purchases7d_30d,
        SUM( CASE WHEN date BETWEEN DATE_SUB( '{}' - INTERVAL 1 DAY, INTERVAL 6 DAY ) AND '{}'- INTERVAL 1 DAY THEN purchases7d ELSE 0 END ) AS total_purchases7d_7d,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_30d,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_7d,
        SUM( CASE WHEN date BETWEEN DATE_SUB( '{}' - INTERVAL 1 DAY, INTERVAL 29 DAY ) AND '{}'- INTERVAL 1 DAY THEN cost ELSE 0 END ) / NULLIF(
        SUM( CASE WHEN date BETWEEN DATE_SUB( '{}' - INTERVAL 1 DAY, INTERVAL 29 DAY ) AND '{}'- INTERVAL 1 DAY THEN clicks ELSE 0 END ),
            0
        ) AS CPC_30d,
        SUM( CASE WHEN date BETWEEN DATE_SUB( '{}' - INTERVAL 1 DAY, INTERVAL 6 DAY ) AND '{}'- INTERVAL 1 DAY THEN cost ELSE 0 END ) / NULLIF(
            SUM( CASE WHEN date BETWEEN DATE_SUB( '{}' - INTERVAL 1 DAY, INTERVAL 6 DAY ) AND '{}'- INTERVAL 1 DAY THEN clicks ELSE 0 END ),
            0
        ) AS CPC_7d,
        SUM( CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END ) / NULLIF( SUM( CASE WHEN date = '{}' - INTERVAL 2 DAY THEN clicks ELSE 0 END ), 0 ) AS CPC_yesterday,
        SUM( CASE WHEN date BETWEEN DATE_SUB( '{}' - INTERVAL 1 DAY, INTERVAL 29 DAY ) AND '{}'- INTERVAL 1 DAY THEN cost ELSE 0 END ) / NULLIF(
        SUM( CASE WHEN date BETWEEN DATE_SUB( '{}' - INTERVAL 1 DAY, INTERVAL 29 DAY ) AND '{}'- INTERVAL 1 DAY THEN sales14d ELSE 0 END ),
            0
        ) AS ACOS_30d,
        SUM( CASE WHEN date BETWEEN DATE_SUB( '{}' - INTERVAL 1 DAY, INTERVAL 6 DAY ) AND '{}'- INTERVAL 1 DAY THEN cost ELSE 0 END ) / NULLIF(
            SUM( CASE WHEN date BETWEEN DATE_SUB( '{}' - INTERVAL 1 DAY, INTERVAL 6 DAY ) AND '{}'- INTERVAL 1 DAY THEN sales14d ELSE 0 END ),
            0
        ) AS ACOS_7d,
        SUM( CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END ) / NULLIF( SUM( CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END ), 0 ) AS ACOS_yesterday
    FROM
        amazon_targeting_reports_sp
    where campaignId in {}
    and date BETWEEN '{}'- INTERVAL 30 DAY and '{}'- INTERVAL 1 DAY
    and market = '{}'

    GROUP BY
        campaignName,
        adGroupName,
        targeting,
        matchType)
    SELECT
      a.*,
        b.impressions AS impressions_yesterday,
        b.clicks AS clicks_yesterday,
        b.cost AS cost_yesterday,
        b.purchases7d AS purchases7d_yesterday,
        b.sales14d AS sales14d_yesterday

    FROM
        a
    LEFT JOIN
        amazon_targeting_reports_sp b ON a.campaignId = b.campaignId AND a.adGroupName = b.adGroupName AND a.targeting = b.targeting AND a.matchType = b.matchType
    WHERE
        b.date = DATE_SUB('{}', INTERVAL 2 DAY)
                    """.format(cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,campaign_ids,cur_time,cur_time,country,cur_time)
            elif version == 1.1:
                query = """
            SELECT DISTINCT
                   campaignName,
                   adGroupName,
                   advertisedSku,
                   sum(clicks) as clicks30d,
                    sum(cost) as cost30d,
                    sum(sales14d) as sales30d,
                    ROUND((sum(cost) / sum(sales14d)), 2) as ACOS,
                    sum(purchases14d) as purchases30d
            FROM
                    amazon_advertised_product_reports_sp
            WHERE
                    campaignStatus = 'ENABLED' AND
                    date between  '{}'- INTERVAL 30 DAY and '{}'- INTERVAL 1 DAY
                     AND
                    market = '{}'
            GROUP BY
                    campaignName,
                    adGroupName,
                    advertisedSku
                                                    """.format(cur_time, cur_time, country)

            df1 = pd.read_sql(query, con=conn)
            output_filename = '.\日常优化\异常定位检测\投放词\预处理.csv'
            df1.to_csv(output_filename, index=False, encoding='utf-8-sig')
            csv_to_json(output_filename)
            # return df
            return print("查询已完成，请查看文件： " + output_filename)

        except Exception as error:
            print("Error while inserting data:", error)

    def preprocessing_sp_overstock_search_term(self, country, cur_time, version=1.0):

        """"""
        try:
            conn = self.conn

            if version == 1.0:
                query = """
                                            SELECT
                                            a.keyword,
                                            a.searchTerm,
                                            a.adGroupName,
                                            a.adGroupId,
                                            a.matchType,
                                            a.campaignName,
                                            a.campaignId,
                                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS ORDER_1m,
                                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS ORDER_7d,
                                            SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN purchases7d ELSE 0 END) AS ORDER_yesterday,
                                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.clicks ELSE 0 END) AS total_clicks_30d,
                                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.clicks ELSE 0 END) AS total_clicks_7d,
                                            SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN a.clicks ELSE 0 END) AS total_clicks_yesterday,
                                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.sales14d ELSE 0 END) AS total_sales14d_30d,
                                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.sales14d ELSE 0 END) AS total_sales14d_7d,
                                            SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN a.sales14d ELSE 0 END) AS total_sales14d_yesterday,
                                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) AS total_cost_30d,
                                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) AS total_cost_7d,
                                            SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN a.cost ELSE 0 END) AS total_cost_yesterday,
                                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.sales14d ELSE 0 END) AS ACOS_30d,
                                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.sales14d ELSE 0 END) AS ACOS_7d,
                                            SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) / SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN a.sales14d ELSE 0 END)  AS ACOS_yesterday

                            FROM
                            amazon_search_term_reports_sp a
                            JOIN
                                amazon_campaigns_list_sp c ON a.campaignId = c.campaignId -- 联接广告活动表，获取广告活动类型
                            WHERE
                                a.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND '{}'- INTERVAL 1 DAY
                                AND a.market = '{}'
                                and a.keywordId in (select keywordId from amazon_search_term_reports_sp where campaignStatus='ENABLED' and date='{}'-INTERVAL 1 DAY)
                                AND c.targetingType like '%MAN%' -- 筛选出手动广告
                                AND campaignName LIKE '%_overstock%'
                                AND (a.campaignName not LIKE '%%ASIN%%' and a.campaignName not LIKE '%%asin%%' and a.campaignName not LIKE '%%商品投放%%' and a.campaignName not LIKE '%%品类投放%%' and a.campaignName not LIKE '%%CATEGORY%%' and a.campaignName not LIKE '%%PRODUCT%%')
                                AND NOT EXISTS (
                            SELECT 1
                            FROM amazon_keywords_list_sp
                            WHERE keywordText = a.searchTerm
                              AND campaignId = a.campaignId
                              AND adGroupId = a.adGroupId
                        )
                            GROUP BY
                              a.adGroupName,
                              a.campaignId,
                              a.keyword,
                              a.searchTerm,
                              a.matchType
                            ORDER BY
                              a.adGroupName,
                              a.campaignId,
                              a.keyword,
                              a.searchTerm;
                                                            """.format(cur_time, cur_time, cur_time, cur_time, cur_time,
                                                                       cur_time,
                                                                       cur_time, cur_time,
                                                                       cur_time, cur_time, cur_time, cur_time, cur_time,
                                                                       cur_time,
                                                                       cur_time, cur_time, cur_time,
                                                                       cur_time, cur_time, cur_time, cur_time, cur_time,
                                                                       cur_time,
                                                                       cur_time, cur_time, cur_time,
                                                                       cur_time, cur_time, cur_time, cur_time, cur_time,
                                                                       cur_time,
                                                                       country,
                                                                       cur_time)
            else:
                query = None
            # print(query)
            df1 = pd.read_sql(query, con=conn)
            output_filename = '.\滞销品优化\手动sp广告\搜索词优化\预处理.csv'
            df1.to_csv(output_filename, index=False, encoding='utf-8-sig')
            csv_to_json(output_filename)
            # return df
            return print("查询已完成，请查看文件： " + output_filename)

        except Exception as error:
            print("Error while inserting data:", error)

    def preprocessing_sp_overstock_product_targets(self, country, cur_time, version=1.0):

        """"""
        try:
            conn = self.conn

            if version == 1.0:
                query = """
WITH a AS (
    SELECT
        keywordId,
        keyword,
        targeting,
        matchType,
        adGroupName,
        campaignName,
        -- 过去30天的总订单数
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS ORDER_1m,
        -- 过去30天的总点击量
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_30d,
        -- 过去7天的总点击量
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_7d,
        -- 昨天的总点击量
        SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN clicks ELSE 0 END) AS total_clicks_yesterday,
        -- 过去30天的总销售额
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_30d,
        -- 过去7天的总销售额
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_7d,
        -- 过去3天的总销售额
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_3d,
        -- 昨天的总销售额
        SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END) AS total_sales14d_yesterday,
        -- 过去30天的总成本
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_30d,
        -- 过去7天的总成本
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_7d,
        -- 过去3天的总成本
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_3d,
        -- 昨天的总成本
        SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) AS total_cost_yesterday,
        -- 过去30天的平均成本销售比（ACOS）
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) /
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_30d,
        -- 过去7天的平均成本销售比（ACOS）
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) /
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_7d,
         -- 过去3天的平均成本销售比（ACOS）
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) /
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_3d,
        -- 昨天的平均成本销售比（ACOS）
        SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) /
        SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END) AS ACOS_yesterday
    FROM
        amazon_targeting_reports_sp b
    JOIN
        amazon_campaigns_list_sp c ON b.campaignId = c.campaignId -- 联接广告活动表，获取广告活动类型
    WHERE
        b.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND '{}' - INTERVAL 1 DAY
        AND b.market = '{}'
        AND b.keywordId IN (
            SELECT keywordId
            FROM amazon_targeting_reports_sp
            WHERE campaignStatus = 'ENABLED' AND date = '{}' - INTERVAL 1 DAY
                                                AND campaignName LIKE '%_overstock%'
        )
        AND c.targetingType LIKE '%MAN%' -- 筛选出手动广告
        -- 排除最近4天内有变更的keywordId
        AND b.keywordId NOT IN (SELECT DISTINCT entityId
            FROM amazon_advertising_change_history
            WHERE timestamp >= (UNIX_TIMESTAMP(NOW(3)) - 4 * 24 * 60 * 60) * 1000
            AND entityType = 'KEYWORD'
            AND market = '{}')
        AND b.campaignId NOT IN (SELECT DISTINCT campaignId FROM amazon_targeting_reports_sd)
    GROUP BY
        b.adGroupName,
        b.campaignName,
        b.keyword,
        b.matchType,
        b.targeting,
        b.keywordId
    ORDER BY
        b.adGroupName,
        b.campaignName,
        b.keyword,
        b.matchType,
        b.keywordId
)
-- 从CTE结果中选择数据
SELECT
    a.*,
    d.keywordBid
FROM
    a
LEFT JOIN
    amazon_targeting_reports_sp d ON a.keywordId = d.keywordId
WHERE
    d.date = DATE_SUB('{}', INTERVAL 1 DAY)
    AND a.matchType in ('TARGETING_EXPRESSION');
                                        """.format(cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                           cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                           cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                           cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, country, cur_time, country, cur_time)

            else:
                query = None
            # print(query)
            df1 = pd.read_sql(query, con=conn)
            output_filename = '.\滞销品优化\手动sp广告\商品投放优化\预处理.csv'
            df1.to_csv(output_filename, index=False, encoding='utf-8-sig')
            csv_to_json(output_filename)
            # return df
            return print("查询已完成，请查看文件： " + output_filename)

        except Exception as error:
            print("Error while inserting data:", error)

    def preprocessing_sp_overstock_product_targets_search_term(self, country, cur_time, version=1.0):

        """"""
        try:
            conn = self.conn

            if version == 1.0:
                query = """
                        SELECT
                            a.keyword,
                            a.searchTerm,
                            a.adGroupName,
                            a.adGroupId,
                            a.matchType,
                            a.campaignName,
                            a.campaignId,
                            -- 过去30天的总点击量
                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.clicks ELSE 0 END) AS total_clicks_30d,
                            -- 过去7天的总点击量
                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.clicks ELSE 0 END) AS total_clicks_7d,
                            -- 昨天的总点击量
                            SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN a.clicks ELSE 0 END) AS total_clicks_yesterday,
                            -- 过去30天的总销售额
                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.sales14d ELSE 0 END) AS total_sales14d_30d,
                            -- 过去7天的总销售额
                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.sales14d ELSE 0 END) AS total_sales14d_7d,
                            -- 昨天的总销售额
                            SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN a.sales14d ELSE 0 END) AS total_sales14d_yesterday,
                            -- 过去30天的总成本
                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) AS total_cost_30d,
                            -- 过去7天的总成本
                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) AS total_cost_7d,
                            -- 昨天的总成本
                            SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN a.cost ELSE 0 END) AS total_cost_yesterday,
                            -- 过去30天的平均成本销售比（ACOS）
                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) /
                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.sales14d ELSE 0 END) AS ACOS_30d,
                            -- 过去7天的平均成本销售比（ACOS）
                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) /
                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.sales14d ELSE 0 END) AS ACOS_7d,
                            -- 昨天的平均成本销售比（ACOS）
                            SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN a.cost ELSE 0 END) /
                            SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN a.sales14d ELSE 0 END) AS ACOS_yesterday,
                            -- 过去30天的总订单数
                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS ORDER_1m,
                            -- 过去7天的总订单数
                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS ORDER_7d
                        FROM
                            amazon_search_term_reports_sp a
                        JOIN
                            amazon_campaigns_list_sp c ON a.campaignId = c.campaignId -- 联接广告活动表，获取广告活动类型
                        WHERE
                            a.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND '{}' - INTERVAL 1 DAY
                            AND a.market = '{}'
                            AND a.keywordId IN (
                                SELECT keywordId
                                FROM amazon_targeting_reports_sp
                                WHERE campaignStatus = 'ENABLED'
                                AND date = '{}' - INTERVAL 1 DAY
                                AND campaignName LIKE '%_overstock%'
                            )
                            AND c.targetingType LIKE '%MAN%' -- 筛选出手动广告
                            AND matchType in ('TARGETING_EXPRESSION')
                            AND LENGTH(a.searchTerm) = 10 -- searchTerm的长度是十位
                            AND LEFT(a.searchTerm, 2) = 'b0' -- searchTerm的开头两个字符是b0
                        GROUP BY
                            a.adGroupName,
                            a.campaignId,
                            a.keyword,
                            a.searchTerm,
                            a.matchType
                        ORDER BY
                            a.adGroupName,
                            a.campaignId,
                            a.keyword,
                            a.searchTerm;
                                        """.format(cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                   cur_time,
                                                   cur_time,
                                                   cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                   cur_time,
                                                   cur_time, cur_time, cur_time,
                                                   cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                   cur_time,
                                                   cur_time, cur_time,
                                                   cur_time, country, cur_time)

            else:
                query = None
            # print(query)
            df1 = pd.read_sql(query, con=conn)
            output_filename = '.\滞销品优化\手动sp广告\商品投放搜索词优化\预处理.csv'
            df1.to_csv(output_filename, index=False, encoding='utf-8-sig')
            csv_to_json(output_filename)
            # return df
            return print("查询已完成，请查看文件： " + output_filename)

        except Exception as error:
            print("Error while inserting data:", error)

    def preprocessing_sp_overstock_budget_manual(self, country, cur_time, version=1.0):

        """"""
        try:
            conn = self.conn

            if version == 1.0:
                query = """
                                        WITH Campaign_Stats AS (
                                 SELECT
                                    acr.campaignId,
                                    acr.campaignName,
                                    acr.campaignBudgetAmount AS Budget,
                                    acr.market,
                                    sum(CASE WHEN acr.date = DATE_SUB('{}', INTERVAL 2 DAY) THEN acr.cost ELSE 0 END) as cost_yesterday,
                                    sum(CASE WHEN acr.date = DATE_SUB('{}', INTERVAL 2 DAY) THEN acr.clicks ELSE 0 END) as clicks_yesterday,
                                    sum(CASE WHEN acr.date = DATE_SUB('{}', INTERVAL 2 DAY) THEN acr.sales14d ELSE 0 END) as sales_yesterday,
                                    SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN acr.cost ELSE 0 END) AS total_cost_7d,
                                    SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN acr.sales14d ELSE 0 END) AS total_sales14d_7d,
                                    SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN acr.cost ELSE 0 END) AS total_cost_30d,
                                    SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN acr.sales14d ELSE 0 END) AS total_sales14d_30d,
                                    SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN acr.clicks ELSE 0 END) AS total_clicks_30d,
                                    SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN acr.clicks ELSE 0 END) AS total_clicks_7d,
                                    SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN acr.cost ELSE 0 END) / NULLIF(SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN acr.sales14d ELSE 0 END), 0) AS ACOS_30d,
                                    SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN acr.cost ELSE 0 END) / NULLIF(SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN acr.sales14d ELSE 0 END), 0) AS ACOS_7d,
                                    SUM(CASE WHEN acr.date = '{}' - INTERVAL 2 DAY THEN acr.cost ELSE 0 END) / NULLIF(SUM(CASE WHEN acr.date = '{}' - INTERVAL 2 DAY THEN acr.sales14d ELSE 0 END), 0)  AS ACOS_yesterday
                                FROM
                                    amazon_campaign_reports_sp acr
                                JOIN
                                    amazon_campaigns_list_sp acl ON acr.campaignId = acl.campaignId
                                WHERE
                                    acr.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND ('{}'-INTERVAL 1 DAY)
                                    AND acr.campaignId IN (
                                        SELECT campaignId
                                        FROM amazon_campaign_reports_sp
                                        WHERE campaignStatus = 'ENABLED' AND date = '{}' - INTERVAL 1 DAY
                                        AND campaignName LIKE '%_overstock%')
                                    AND acl.targetingType LIKE '%MAN%'  -- 这里筛选手动广告
                                    AND acr.market = '{}'
                                GROUP BY
                                    acr.campaignId
                            ),
                            b as (SELECT
                                SUM(reports.cost)/SUM(reports.sales14d) AS country_avg_ACOS_1m,
                                reports.market
                            FROM
                                amazon_campaign_reports_sp AS reports
                            INNER JOIN
                                amazon_campaigns_list_sp AS campaigns ON reports.campaignId = campaigns.campaignId
                            WHERE
                                reports.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND ('{}'-INTERVAL 1 DAY)
                                            and campaigns.campaignId in ( SELECT campaignId
                                    FROM amazon_campaign_reports_sp
                                 WHERE campaignStatus = 'ENABLED' AND date = '{}' - INTERVAL 1 DAY)
                                AND campaigns.targetingType LIKE '%MAN%'  -- 筛选手动广告
                                AND reports.market = '{}'
                            GROUP BY
                                reports.market)

                              SELECT Campaign_Stats.*,b. country_avg_ACOS_1m
                                    from Campaign_Stats join b
                                    on Campaign_Stats.market =b.market
                                                        """.format(cur_time, cur_time, cur_time, cur_time, cur_time,
                                                                   cur_time, cur_time,
                                                                   cur_time, cur_time,
                                                                   cur_time, cur_time, cur_time, cur_time, cur_time,
                                                                   cur_time, cur_time,
                                                                   cur_time,
                                                                   cur_time, cur_time, cur_time, cur_time, cur_time,
                                                                   cur_time, cur_time,
                                                                   cur_time,
                                                                   cur_time, cur_time, cur_time, country, cur_time,
                                                                   cur_time, cur_time,
                                                                   country)
            else:
                query = None
            # print(query)
            df1 = pd.read_sql(query, con=conn)
            output_filename = '.\滞销品优化\手动sp广告\预算优化\预处理.csv'
            df1.to_csv(output_filename, index=False, encoding='utf-8-sig')
            csv_to_json(output_filename)
            # return df
            return print("查询已完成，请查看文件： " + output_filename)

        except Exception as error:
            print("Error while inserting data:", error)

    def preprocessing_sp_overstock_budget_auto(self, country, cur_time, version=1.0):

        """"""
        try:
            conn = self.conn

            if version == 1.0:
                query = """
                                           WITH Campaign_Stats AS (
                     SELECT
                        acr.campaignId,
                        acr.campaignName,
                        acr.campaignBudgetAmount AS Budget,
                        acr.market,
                        sum(CASE WHEN acr.date = DATE_SUB('{}', INTERVAL 2 DAY) THEN acr.cost ELSE 0 END) as cost_yesterday,
                        sum(CASE WHEN acr.date = DATE_SUB('{}', INTERVAL 2 DAY) THEN acr.clicks ELSE 0 END) as clicks_yesterday,
                        sum(CASE WHEN acr.date = DATE_SUB('{}', INTERVAL 2 DAY) THEN acr.sales14d ELSE 0 END) as sales_yesterday,
                        SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN acr.cost ELSE 0 END) AS total_cost_7d,
                        SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN acr.sales14d ELSE 0 END) AS total_sales14d_7d,
                        SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN acr.cost ELSE 0 END) AS total_cost_30d,
                        SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN acr.sales14d ELSE 0 END) AS total_sales14d_30d,
                        SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN acr.clicks ELSE 0 END) AS total_clicks_30d,
                        SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN acr.clicks ELSE 0 END) AS total_clicks_7d,
                        SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN acr.cost ELSE 0 END) / NULLIF(SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN acr.sales14d ELSE 0 END), 0) AS ACOS_30d,
                        SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN acr.cost ELSE 0 END) / NULLIF(SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN acr.sales14d ELSE 0 END), 0) AS ACOS_7d,
                        SUM(CASE WHEN acr.date = '{}' - INTERVAL 2 DAY THEN acr.cost ELSE 0 END) / NULLIF(SUM(CASE WHEN acr.date = '{}' - INTERVAL 2 DAY THEN acr.sales14d ELSE 0 END), 0)  AS ACOS_yesterday
                    FROM
                        amazon_campaign_reports_sp acr
                    JOIN
                        amazon_campaigns_list_sp acl ON acr.campaignId = acl.campaignId
                    WHERE
                        acr.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND ('{}'-INTERVAL 1 DAY)
                        AND acr.campaignId IN (
                            SELECT campaignId
                            FROM amazon_campaign_reports_sp
                            WHERE campaignStatus = 'ENABLED' AND date = '{}' - INTERVAL 1 DAY
                            AND campaignName LIKE '%_overstock%')
                        AND acl.targetingType LIKE '%AUT%'  -- 这里筛选手动广告
                        AND acr.market = '{}'
                    GROUP BY
                        acr.campaignName
                ),
                b as (SELECT
                    SUM(reports.cost)/SUM(reports.sales14d) AS country_avg_ACOS_1m,
                    reports.market
                FROM
                    amazon_campaign_reports_sp AS reports
                INNER JOIN
                    amazon_campaigns_list_sp AS campaigns ON reports.campaignId = campaigns.campaignId
                WHERE
                    reports.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND ('{}'-INTERVAL 1 DAY)
                                and campaigns.campaignId in ( SELECT campaignId
                        FROM amazon_campaign_reports_sp
                     WHERE campaignStatus = 'ENABLED' AND date = '{}' - INTERVAL 1 DAY )

                    AND campaigns.targetingType LIKE '%AUT%'  -- 筛选手动广告
                    AND reports.market = '{}'
                GROUP BY
                    reports.market)
                  SELECT Campaign_Stats.*,b. country_avg_ACOS_1m
                        from Campaign_Stats join b
                        on Campaign_Stats.market =b.market
                                                            """.format(cur_time, cur_time, cur_time, cur_time, cur_time,
                                                                       cur_time,
                                                                       cur_time, cur_time, cur_time, cur_time,
                                                                       cur_time, cur_time, cur_time, cur_time, cur_time,
                                                                       cur_time,
                                                                       cur_time,
                                                                       cur_time, cur_time, cur_time, cur_time, cur_time,
                                                                       cur_time,
                                                                       cur_time, cur_time, cur_time, cur_time, cur_time,
                                                                       country,
                                                                       cur_time, cur_time, cur_time, country)
            else:
                query = None
            # print(query)
            df1 = pd.read_sql(query, con=conn)
            output_filename = '.\滞销品优化\自动sp广告\预算优化\预处理.csv'
            df1.to_csv(output_filename, index=False, encoding='utf-8-sig')
            csv_to_json(output_filename)
            # return df
            return print("查询已完成，请查看文件： " + output_filename)

        except Exception as error:
            print("Error while inserting data:", error)

    def preprocessing_sp_overstock_targeting_group_manual(self, country, cur_time, version=1.0):

        """"""
        try:
            conn = self.conn

            if version == 1.0:
                query = """
                            SELECT
    a.campaignName,
    a.campaignId,
                a.placementClassification,
    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY)  THEN a.clicks ELSE 0 END) AS total_clicks_3d,
    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY)  THEN a.clicks ELSE 0 END) AS total_clicks_7d,
    SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN a.clicks ELSE 0 END) AS total_clicks_yesterday,
    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY)  THEN a.sales14d ELSE 0 END) AS total_sales14d_3d,
    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY)  THEN a.sales14d ELSE 0 END) AS total_sales14d_7d,
    SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN a.sales14d ELSE 0 END) AS total_sales14d_yesterday,
    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY)  THEN a.cost ELSE 0 END) AS total_cost_3d,
    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY)  THEN a.cost ELSE 0 END) AS total_cost_7d,
    SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN a.cost ELSE 0 END) AS total_cost_yesterday,
    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY)  THEN a.cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY)  THEN a.sales14d ELSE 0 END) AS ACOS_3d,
    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY)  THEN a.cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY)  THEN a.sales14d ELSE 0 END) AS ACOS_7d,
    SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN a.cost ELSE 0 END) / SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN a.sales14d ELSE 0 END)  AS ACOS_yesterday ,
    COALESCE(
        CASE
            WHEN a.placementClassification = 'Detail Page on-Amazon' THEN c.dynamicBidding_placementProductPage_percentage
            WHEN a.placementClassification = 'Other on-Amazon' THEN c.dynamicBidding_placementRestOfSearch_percentage
            WHEN a.placementClassification = 'Top of Search on-Amazon' THEN c.dynamicBidding_placementTop_percentage
        END,
    0) AS bid
FROM
    amazon_campaign_placement_reports_sp a
JOIN
    (SELECT
         campaignId,
         targetingType,
         dynamicBidding_placementTop_percentage,
         dynamicBidding_placementProductPage_percentage,
         dynamicBidding_placementRestOfSearch_percentage
     FROM
         amazon_campaigns_list_sp
     ) c ON a.campaignId = c.campaignId
WHERE
    a.market = '{}'
    AND a.campaignId IN (
        SELECT campaignId
        FROM amazon_advertised_product_reports_sp
        WHERE campaignStatus = 'ENABLED' AND date = '{}' - INTERVAL 1 DAY
                                AND campaignName LIKE '%_overstock'
                                GROUP BY campaignId
    )
    AND c.targetingType LIKE '%MAN%'
GROUP BY
    a.campaignId,
    a.placementClassification,
    c.dynamicBidding_placementTop_percentage,
    c.dynamicBidding_placementProductPage_percentage,
    c.dynamicBidding_placementRestOfSearch_percentage
ORDER BY
    a.campaignName,
    a.placementClassification;
                                                        """.format(cur_time, cur_time, cur_time, cur_time, cur_time,
                                                                   cur_time, cur_time, cur_time, cur_time,
                                                                   cur_time, cur_time, cur_time, cur_time, cur_time,
                                                                   cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                                   cur_time, cur_time, cur_time,country, cur_time)
            else:
                query = None
            # print(query)
            df1 = pd.read_sql(query, con=conn)
            output_filename = '.\滞销品优化\手动sp广告\广告位优化\预处理.csv'
            df1.to_csv(output_filename, index=False, encoding='utf-8-sig')
            csv_to_json(output_filename)
            # return df
            return print("查询已完成，请查看文件： " + output_filename)

        except Exception as error:
            print("Error while inserting data:", error)

    def preprocessing_sp_overstock_targeting_group_auto(self, country, cur_time, version=1.0):

        """"""
        try:
            conn = self.conn

            if version == 1.0:
                query = """
SELECT
    a.campaignName,
    a.campaignId,
                a.placementClassification,
    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY)  THEN a.clicks ELSE 0 END) AS total_clicks_3d,
    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY)  THEN a.clicks ELSE 0 END) AS total_clicks_7d,
    SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN a.clicks ELSE 0 END) AS total_clicks_yesterday,
    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY)  THEN a.sales14d ELSE 0 END) AS total_sales14d_3d,
    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY)  THEN a.sales14d ELSE 0 END) AS total_sales14d_7d,
    SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN a.sales14d ELSE 0 END) AS total_sales14d_yesterday,
    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY)  THEN a.cost ELSE 0 END) AS total_cost_3d,
    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY)  THEN a.cost ELSE 0 END) AS total_cost_7d,
    SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN a.cost ELSE 0 END) AS total_cost_yesterday,
    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY)  THEN a.cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY)  THEN a.sales14d ELSE 0 END) AS ACOS_3d,
    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY)  THEN a.cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY)  THEN a.sales14d ELSE 0 END) AS ACOS_7d,
    SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN a.cost ELSE 0 END) / SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN a.sales14d ELSE 0 END)  AS ACOS_yesterday ,
    COALESCE(
        CASE
            WHEN a.placementClassification = 'Detail Page on-Amazon' THEN c.dynamicBidding_placementProductPage_percentage
            WHEN a.placementClassification = 'Other on-Amazon' THEN c.dynamicBidding_placementRestOfSearch_percentage
            WHEN a.placementClassification = 'Top of Search on-Amazon' THEN c.dynamicBidding_placementTop_percentage
        END,
    0) AS bid
FROM
    amazon_campaign_placement_reports_sp a
JOIN
    (SELECT
         campaignId,
         targetingType,
         dynamicBidding_placementTop_percentage,
         dynamicBidding_placementProductPage_percentage,
         dynamicBidding_placementRestOfSearch_percentage
     FROM
         amazon_campaigns_list_sp
     ) c ON a.campaignId = c.campaignId
WHERE
    a.market = '{}'
    AND a.campaignId IN (
        SELECT campaignId
        FROM amazon_advertised_product_reports_sp
        WHERE campaignStatus = 'ENABLED' AND date = '{}' - INTERVAL 1 DAY
                                AND campaignName LIKE '%_overstock'
                                GROUP BY campaignId
    )
    AND c.targetingType LIKE '%AUT%'
GROUP BY
    a.campaignId,
    a.placementClassification,
    c.dynamicBidding_placementTop_percentage,
    c.dynamicBidding_placementProductPage_percentage,
    c.dynamicBidding_placementRestOfSearch_percentage
ORDER BY
    a.campaignName,
    a.placementClassification;
                                                            """.format(cur_time, cur_time, cur_time, cur_time, cur_time,
                                                                   cur_time, cur_time, cur_time, cur_time,
                                                                   cur_time, cur_time, cur_time, cur_time, cur_time,
                                                                   cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                                   cur_time, cur_time, cur_time,country, cur_time)
            else:
                query = None
            # print(query)
            df1 = pd.read_sql(query, con=conn)
            output_filename = '.\滞销品优化\自动sp广告\广告位优化\预处理.csv'
            df1.to_csv(output_filename, index=False, encoding='utf-8-sig')
            csv_to_json(output_filename)
            # return df
            return print("查询已完成，请查看文件： " + output_filename)

        except Exception as error:
            print("Error while inserting data:", error)

    def preprocessing_sp_overstock_sku_manual(self, country, cur_time, version=1.0):

        """"""
        try:
            conn = self.conn

            if version == 1.0:
                query = """
                                               SELECT
                                    adGroupName,
                                    a.adId,
                                    a.campaignId,
                                    campaignName,
                                    advertisedSku,
                                        -- 过去30天的总订单数
                                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS ORDER_1m,
                                        -- 过去7天的总订单数
                                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS ORDER_7d,
                                        -- 过去30天（包含今天）的总点击量
                                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_30d,
                                        -- 过去7天（包含今天）的总点击量
                                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_7d,
                                        -- 昨天的总点击量
                                        SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN clicks ELSE 0 END) AS total_clicks_yesterday,
                                        -- 过去30天的总销售额
                                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_30d,
                                        -- 过去7天的总销售额
                                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_7d,
                                        -- 昨天的总销售额
                                        SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END) AS total_sales14d_yesterday,
                                        -- 过去30天的总成本
                                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_30d,
                                        -- 过去7天的总成本
                                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_7d,
                                        -- 昨天的总成本
                                        SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) AS total_cost_yesterday,
                                        -- 过去30天的平均成本销售比（ACOS）
                                        CASE WHEN SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) > 0
                                             THEN SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) /
                                                  SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END)
                                             ELSE 0
                                        END AS ACOS_30d,
                                        -- 过去7天的平均成本销售比（ACOS）
                                        CASE WHEN SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) > 0
                                             THEN SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) /
                                                  SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END)
                                             ELSE 0
                                        END AS ACOS_7d,
                                        -- 昨天的平均成本销售比（ACOS）
                                        CASE WHEN SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END) > 0
                                             THEN SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) /
                                                  SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END)
                                             ELSE 0
                                        END AS ACOS_yesterday
                                    FROM
                                        amazon_advertised_product_reports_sp a
                                    JOIN
                                        amazon_campaigns_list_sp c ON a.campaignId = c.campaignId
                                    WHERE
                                        a.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND '{}'- INTERVAL 1 DAY
                                        AND a.market = '{}'
                                        AND a.campaignId IN (
                                            SELECT campaignId
                                            FROM amazon_advertised_product_reports_sp
                                            WHERE campaignStatus = 'ENABLED' AND date = '{}' - INTERVAL 1 DAY
                                            AND campaignName LIKE '%_overstock%'
                                        )
                                        AND c.targetingType like '%MAN%'
                                        AND not EXISTS (
                                SELECT 1
                                FROM amazon_sp_productads_list
                                WHERE sku = a.advertisedSku
                                  AND campaignId = a.campaignId
                                  AND adId = a.adId
                                  AND state in ('ARCHIVED','PAUSED')
                            )
                                    GROUP BY
                                        adGroupName,
                                        a.adId,
                                        campaignName,
                                        advertisedSku

                                    ORDER BY
                                        adGroupName,
                                        campaignName,
                                        advertisedSku;
                                                                """.format(cur_time, cur_time, cur_time, cur_time,
                                                                           cur_time, cur_time,
                                                                           cur_time,
                                                                           cur_time, cur_time,
                                                                           cur_time, cur_time, cur_time, cur_time,
                                                                           cur_time, cur_time,
                                                                           cur_time,
                                                                           cur_time,
                                                                           cur_time, cur_time, cur_time, cur_time,
                                                                           cur_time, cur_time,
                                                                           cur_time,
                                                                           cur_time, cur_time, cur_time, cur_time,
                                                                           cur_time,
                                                                           cur_time, cur_time, cur_time, cur_time,
                                                                           cur_time, cur_time,
                                                                           cur_time,
                                                                           country, cur_time)
            else:
                query = None
            # print(query)
            df1 = pd.read_sql(query, con=conn)
            output_filename = '.\滞销品优化\手动sp广告\关闭SKU\预处理.csv'
            df1.to_csv(output_filename, index=False, encoding='utf-8-sig')
            csv_to_json(output_filename)
            # return df
            return print("查询已完成，请查看文件： " + output_filename)

        except Exception as error:
            print("Error while inserting data:", error)

    def preprocessing_sp_overstock_sku_auto(self, country, cur_time, version=1.0):

        """"""
        try:
            conn = self.conn

            if version == 1.0:
                query = """
                                            SELECT
                            adGroupName,
                            a.adId,
                            a.campaignId,
                            campaignName,
                            advertisedSku,
                                -- 过去30天的总订单数
                                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS ORDER_1m,
                                -- 过去7天的总订单数
                                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS ORDER_7d,
                                -- 过去30天（包含今天）的总点击量
                                SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_30d,
                                -- 过去7天（包含今天）的总点击量
                                SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_7d,
                                -- 昨天的总点击量
                                SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN clicks ELSE 0 END) AS total_clicks_yesterday,
                                -- 过去30天的总销售额
                                SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_30d,
                                -- 过去7天的总销售额
                                SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_7d,
                                -- 昨天的总销售额
                                SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END) AS total_sales14d_yesterday,
                                -- 过去30天的总成本
                                SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_30d,
                                -- 过去7天的总成本
                                SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_7d,
                                -- 昨天的总成本
                                SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) AS total_cost_yesterday,
                                -- 过去30天的平均成本销售比（ACOS）
                                CASE WHEN SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) > 0
                                     THEN SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) /
                                          SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END)
                                     ELSE 0
                                END AS ACOS_30d,
                                -- 过去7天的平均成本销售比（ACOS）
                                CASE WHEN SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) > 0
                                     THEN SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) /
                                          SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END)
                                     ELSE 0
                                END AS ACOS_7d,
                                -- 昨天的平均成本销售比（ACOS）
                                CASE WHEN SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END) > 0
                                     THEN SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) /
                                          SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END)
                                     ELSE 0
                                END AS ACOS_yesterday
                            FROM
                                amazon_advertised_product_reports_sp a
                            JOIN
                                amazon_campaigns_list_sp c ON a.campaignId = c.campaignId
                            WHERE
                                a.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND '{}'- INTERVAL 1 DAY
                                AND a.market = '{}'
                                AND a.campaignId IN (
                                    SELECT campaignId
                                    FROM amazon_advertised_product_reports_sp
                                    WHERE campaignStatus = 'ENABLED' AND date = '{}' - INTERVAL 1 DAY
                                    AND campaignName LIKE '%_overstock%'
                                )
                                AND c.targetingType like '%AUT%'
                                AND not EXISTS (
                        SELECT 1
                        FROM amazon_sp_productads_list
                        WHERE sku = a.advertisedSku
                          AND campaignId = a.campaignId
                          AND adId = a.adId
                          AND state in ('ARCHIVED','PAUSED')
                    )
                            GROUP BY
                                adGroupName,
                                a.adId,
                                campaignName,
                                advertisedSku

                            ORDER BY
                                adGroupName,
                                campaignName,
                                advertisedSku;
                                                            """.format(cur_time, cur_time, cur_time, cur_time, cur_time,
                                                                       cur_time,
                                                                       cur_time, cur_time, cur_time, cur_time,
                                                                       cur_time, cur_time, cur_time, cur_time, cur_time,
                                                                       cur_time,
                                                                       cur_time, cur_time, cur_time, cur_time,
                                                                       cur_time, cur_time, cur_time, cur_time, cur_time,
                                                                       cur_time,
                                                                       cur_time, cur_time, cur_time, cur_time,
                                                                       cur_time, cur_time, cur_time, cur_time, cur_time,
                                                                       cur_time,
                                                                       country,
                                                                       cur_time)
            else:
                query = None
            # print(query)
            df1 = pd.read_sql(query, con=conn)
            output_filename = '.\滞销品优化\自动sp广告\关闭SKU\预处理.csv'
            df1.to_csv(output_filename, index=False, encoding='utf-8-sig')
            csv_to_json(output_filename)
            # return df
            return print("查询已完成，请查看文件： " + output_filename)

        except Exception as error:
            print("Error while inserting data:", error)

    def preprocessing_sp_overstock_sku_reopen_manual(self, country, cur_time, version=1.0):

        """"""
        try:
            conn = self.conn

            if version == 1.0:
                query = """
                                       SELECT
                            adGroupName,
                            a.adId,
                            a.campaignId,
                            campaignName,
                            advertisedSku,
                                -- 过去30天的总订单数
                                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS ORDER_1m,
                                -- 过去7天的总订单数
                                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS ORDER_7d,
                                -- 过去30天（包含今天）的总点击量
                                SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_30d,
                                -- 过去7天（包含今天）的总点击量
                                SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_7d,
                                -- 昨天的总点击量
                                SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN clicks ELSE 0 END) AS total_clicks_yesterday,
                                -- 过去30天的总销售额
                                SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_30d,
                                -- 过去7天的总销售额
                                SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_7d,
                                -- 昨天的总销售额
                                SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END) AS total_sales14d_yesterday,
                                -- 过去30天的总成本
                                SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_30d,
                                -- 过去7天的总成本
                                SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_7d,
                                -- 昨天的总成本
                                SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) AS total_cost_yesterday,
                                -- 过去30天的平均成本销售比（ACOS）
                                CASE WHEN SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) > 0
                                     THEN SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) /
                                          SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END)
                                     ELSE 0
                                END AS ACOS_30d,
                                -- 过去7天的平均成本销售比（ACOS）
                                CASE WHEN SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) > 0
                                     THEN SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) /
                                          SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END)
                                     ELSE 0
                                END AS ACOS_7d,
                                -- 昨天的平均成本销售比（ACOS）
                                CASE WHEN SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END) > 0
                                     THEN SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) /
                                          SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END)
                                     ELSE 0
                                END AS ACOS_yesterday
                            FROM
                                amazon_advertised_product_reports_sp a
                            JOIN
                                amazon_campaigns_list_sp c ON a.campaignId = c.campaignId
                            WHERE
                                a.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND '{}'- INTERVAL 1 DAY
                                AND a.market = '{}'
                                AND a.campaignId IN (
                                    SELECT campaignId
                                    FROM amazon_advertised_product_reports_sp
                                    WHERE campaignStatus = 'ENABLED' AND date = '{}' - INTERVAL 1 DAY
                                    AND campaignName LIKE '%_overstock%'
                                )
                                AND c.targetingType like '%MAN%'
                                AND not EXISTS (
                        SELECT 1
                        FROM amazon_sp_productads_list
                        WHERE sku = a.advertisedSku
                          AND campaignId = a.campaignId
                          AND adId = a.adId
                          AND state in ('ARCHIVED','ENABLED')
                    )
                            GROUP BY
                                adGroupName,
                                a.adId,
                                campaignName,
                                advertisedSku

                            ORDER BY
                                adGroupName,
                                campaignName,
                                advertisedSku;
                                                        """.format(cur_time, cur_time, cur_time, cur_time, cur_time,
                                                                   cur_time,
                                                                   cur_time,
                                                                   cur_time, cur_time,
                                                                   cur_time, cur_time, cur_time, cur_time, cur_time,
                                                                   cur_time,
                                                                   cur_time,
                                                                   cur_time,
                                                                   cur_time, cur_time, cur_time, cur_time, cur_time,
                                                                   cur_time,
                                                                   cur_time,
                                                                   cur_time, cur_time, cur_time, cur_time, cur_time,
                                                                   cur_time, cur_time, cur_time, cur_time, cur_time,
                                                                   cur_time,
                                                                   cur_time,
                                                                   country, cur_time)
            else:
                query = None
            # print(query)
            df1 = pd.read_sql(query, con=conn)
            output_filename = '.\滞销品优化\手动sp广告\复开SKU\预处理.csv'
            df1.to_csv(output_filename, index=False, encoding='utf-8-sig')
            csv_to_json(output_filename)
            # return df
            return print("查询已完成，请查看文件： " + output_filename)

        except Exception as error:
            print("Error while inserting data:", error)

    def preprocessing_sp_overstock_sku_reopen_auto(self, country, cur_time, version=1.0):

        """"""
        try:
            conn = self.conn

            if version == 1.0:
                query = """
                                            SELECT
                            adGroupName,
                            a.adId,
                            a.campaignId,
                            campaignName,
                            advertisedSku,
                                -- 过去30天的总订单数
                                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS ORDER_1m,
                                -- 过去7天的总订单数
                                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS ORDER_7d,
                                -- 过去30天（包含今天）的总点击量
                                SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_30d,
                                -- 过去7天（包含今天）的总点击量
                                SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_7d,
                                -- 昨天的总点击量
                                SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN clicks ELSE 0 END) AS total_clicks_yesterday,
                                -- 过去30天的总销售额
                                SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_30d,
                                -- 过去7天的总销售额
                                SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_7d,
                                -- 昨天的总销售额
                                SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END) AS total_sales14d_yesterday,
                                -- 过去30天的总成本
                                SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_30d,
                                -- 过去7天的总成本
                                SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_7d,
                                -- 昨天的总成本
                                SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) AS total_cost_yesterday,
                                -- 过去30天的平均成本销售比（ACOS）
                                CASE WHEN SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) > 0
                                     THEN SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) /
                                          SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END)
                                     ELSE 0
                                END AS ACOS_30d,
                                -- 过去7天的平均成本销售比（ACOS）
                                CASE WHEN SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) > 0
                                     THEN SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) /
                                          SUM(CASE WHEN date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END)
                                     ELSE 0
                                END AS ACOS_7d,
                                -- 昨天的平均成本销售比（ACOS）
                                CASE WHEN SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END) > 0
                                     THEN SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) /
                                          SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END)
                                     ELSE 0
                                END AS ACOS_yesterday
                            FROM
                                amazon_advertised_product_reports_sp a
                            JOIN
                                amazon_campaigns_list_sp c ON a.campaignId = c.campaignId
                            WHERE
                                a.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND '{}'- INTERVAL 1 DAY
                                AND a.market = '{}'
                                AND a.campaignId IN (
                                    SELECT campaignId
                                    FROM amazon_advertised_product_reports_sp
                                    WHERE campaignStatus = 'ENABLED' AND date = '{}' - INTERVAL 1 DAY
                                    AND campaignName LIKE '%_overstock%'
                                )
                                AND c.targetingType like '%AUT%'
                                AND not EXISTS (
                        SELECT 1
                        FROM amazon_sp_productads_list
                        WHERE sku = a.advertisedSku
                          AND campaignId = a.campaignId
                          AND adId = a.adId
                          AND state in ('ARCHIVED','ENABLED')
                    )
                            GROUP BY
                                adGroupName,
                                a.adId,
                                campaignName,
                                advertisedSku

                            ORDER BY
                                adGroupName,
                                campaignName,
                                advertisedSku;
                                                            """.format(cur_time, cur_time, cur_time, cur_time, cur_time,
                                                                       cur_time,
                                                                       cur_time, cur_time, cur_time, cur_time,
                                                                       cur_time, cur_time, cur_time, cur_time, cur_time,
                                                                       cur_time,
                                                                       cur_time, cur_time, cur_time, cur_time,
                                                                       cur_time, cur_time, cur_time, cur_time, cur_time,
                                                                       cur_time,
                                                                       cur_time, cur_time, cur_time, cur_time,
                                                                       cur_time, cur_time, cur_time, cur_time, cur_time,
                                                                       cur_time,
                                                                       country,
                                                                       cur_time)
            else:
                query = None
            # print(query)
            df1 = pd.read_sql(query, con=conn)
            output_filename = '.\滞销品优化\自动sp广告\复开SKU\预处理.csv'
            df1.to_csv(output_filename, index=False, encoding='utf-8-sig')
            csv_to_json(output_filename)
            # return df
            return print("查询已完成，请查看文件： " + output_filename)

        except Exception as error:
            print("Error while inserting data:", error)

    def preprocessing_sp_overstock_keyword_manual(self, country, cur_time, version=1.0):

        """"""
        try:
            conn = self.conn

            if version == 1.0:
                query = """
                                            WITH a AS (
                                SELECT
                                    keywordId,
                                    keyword,
                                    targeting,
                                    matchType,
                                    adGroupName,
                                    campaignName,
                                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS ORDER_1m,
                                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_30d,
                                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_7d,
                                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_3d,
                                    SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN clicks ELSE 0 END) AS total_clicks_yesterday,
                                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_30d,
                                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_7d,
                                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_3d,
                                    SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END) AS total_sales14d_yesterday,
                                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_30d,
                                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_7d,
                                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_3d,
                                    SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) AS total_cost_yesterday,
                                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_30d,
                                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_7d,
                                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_3d,
                                    SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) / SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END)  AS ACOS_yesterday
                                FROM
                                    amazon_targeting_reports_sp b
                                JOIN
                                amazon_campaigns_list_sp c ON b.campaignId = c.campaignId -- 联接广告活动表，获取广告活动类型
                                WHERE
                                b.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND '{}'- INTERVAL 1 DAY
                                AND b.market = '{}'
                                AND b.keywordId IN (
                                    SELECT keywordId
                                    FROM amazon_targeting_reports_sp
                                    WHERE campaignStatus = 'ENABLED' AND date = '{}' - INTERVAL 1 DAY
                                    AND campaignName LIKE '%_overstock%'
                                )
                                AND c.targetingType like '%MAN%'
                                AND  b.keywordId not in (SELECT DISTINCT entityId
                                    FROM amazon_advertising_change_history
                                    WHERE timestamp >= (UNIX_TIMESTAMP(NOW(3)) - 4 * 24 * 60 * 60) * 1000
                                    AND entityType = 'KEYWORD'
                                    AND market = '{}')
                                AND matchType not in ('TARGETING_EXPRESSION')
                                GROUP BY
                                    b.adGroupName,
                                    b.campaignName,
                                    b.keyword,
                                    b.matchType,
                                    b.targeting,
                                    b.keywordId
                                ORDER BY
                                    b.adGroupName,
                                    b.campaignName,
                                    b.keyword,
                                    b.matchType,
                                    b.keywordId
                                    )
                            SELECT
                                a.*,
                                d.keywordBid
                            FROM
                                a
                            LEFT JOIN
                                amazon_targeting_reports_sp d ON a.keywordId = d.keywordId
                            WHERE
                                d.date = DATE_SUB('{}', INTERVAL 1 DAY)
                                            """.format(cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                       cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                       cur_time,cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                       cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                       cur_time, cur_time, cur_time,
                                                       cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                       cur_time,
                                                       cur_time, cur_time, cur_time, cur_time, country, cur_time,
                                                       country, cur_time)
            else:
                query = None
            # print(query)
            df1 = pd.read_sql(query, con=conn)
            output_filename = '.\滞销品优化\手动sp广告\关键词优化\预处理.csv'
            df1.to_csv(output_filename, index=False, encoding='utf-8-sig')
            csv_to_json(output_filename)
            # return df
            return print("查询已完成，请查看文件： " + output_filename)

        except Exception as error:
            print("Error while inserting data:", error)

    def preprocessing_sp_overstock_automatic_targeting_auto(self, country, cur_time, version=1.0):

        """"""
        try:
            conn = self.conn

            if version == 1.0:
                query = """
                                           WITH a AS (
                    SELECT
                        keywordId,
                        keyword,
                        adGroupName,
                        campaignName,
                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS ORDER_1m,
                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_30d,
                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_7d,
                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_3d,
                        SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN clicks ELSE 0 END) AS total_clicks_yesterday,
                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_30d,
                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_7d,
                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_3d,
                        SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END) AS total_sales14d_yesterday,
                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_30d,
                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_7d,
                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_3d,
                        SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) AS total_cost_yesterday,
                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_30d,
                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_7d,
                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_3d,
                        SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) / SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END)  AS ACOS_yesterday
                    FROM
                        amazon_targeting_reports_sp b
                   JOIN
                        amazon_campaigns_list_sp c ON b.campaignId = c.campaignId
                WHERE
                    b.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND '{}'- INTERVAL 1 DAY
                    AND b.market = '{}'
                    AND b.keywordId IN (
                        SELECT keywordId
                        FROM amazon_targeting_reports_sp
                        WHERE campaignStatus = 'ENABLED' AND date = '{}' - INTERVAL 1 DAY
                        AND campaignName LIKE '%_overstock%'
                    )
                    AND c.targetingType like '%AUT%'
                    AND  b.keywordId not in ( SELECT DISTINCT entityId
                        FROM amazon_advertising_change_history
                        WHERE timestamp >= (UNIX_TIMESTAMP(NOW(3)) - 4 * 24 * 60 * 60) * 1000
                        AND market = '{}'
                        AND predefinedTarget <> '')
                    GROUP BY
                        b.adGroupName,
                        b.campaignName,
                        b.keyword,
                        b.targeting,
                        b.keywordId
                    ORDER BY
                        b.adGroupName,
                        b.campaignName,
                        b.keyword,
                        b.keywordId
                        )
                SELECT
                    a.*,
                    d.keywordBid
                FROM
                    a
                LEFT JOIN
                    amazon_targeting_reports_sp d ON a.keywordId = d.keywordId
                WHERE
                    d.date = DATE_SUB('{}', INTERVAL 1 DAY)
                                                            """.format(cur_time, cur_time, cur_time, cur_time, cur_time,
                                                                       cur_time,cur_time, cur_time, cur_time, cur_time, cur_time,
                                                                       cur_time, cur_time, cur_time, cur_time, cur_time,
                                                                       cur_time,cur_time, cur_time, cur_time,
                                                                       cur_time, cur_time, cur_time, cur_time, cur_time,
                                                                       cur_time,
                                                                       cur_time, cur_time, cur_time, cur_time, cur_time,
                                                                       cur_time,
                                                                       cur_time, cur_time, cur_time, cur_time, cur_time,
                                                                       cur_time,
                                                                       cur_time, country, cur_time, country, cur_time)
            else:
                query = None
            # print(query)
            df1 = pd.read_sql(query, con=conn)
            output_filename = '.\滞销品优化\自动sp广告\自动定位组优化\预处理.csv'
            df1.to_csv(output_filename, index=False, encoding='utf-8-sig')
            csv_to_json(output_filename)
            # return df
            return print("查询已完成，请查看文件： " + output_filename)

        except Exception as error:
            print("Error while inserting data:", error)

    def preprocessing_sp_overstock_search_term_auto(self, country, cur_time, version=1.0):

        """"""
        try:
            conn = self.conn

            if version == 1.0:
                query = """
                                            SELECT
                                            a.keyword,
                                            a.searchTerm,
                                            a.adGroupName,
                                            a.adGroupId,
                                            a.matchType,
                                            a.campaignName,
                                            a.campaignId,
                                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS ORDER_1m,
                                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS ORDER_7d,
                                            SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN purchases7d ELSE 0 END) AS ORDER_yesterday,
                                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.clicks ELSE 0 END) AS total_clicks_30d,
                                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.clicks ELSE 0 END) AS total_clicks_7d,
                                            SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN a.clicks ELSE 0 END) AS total_clicks_yesterday,
                                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.sales14d ELSE 0 END) AS total_sales14d_30d,
                                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.sales14d ELSE 0 END) AS total_sales14d_7d,
                                            SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN a.sales14d ELSE 0 END) AS total_sales14d_yesterday,
                                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) AS total_cost_30d,
                                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) AS total_cost_7d,
                                            SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN a.cost ELSE 0 END) AS total_cost_yesterday,
                                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.sales14d ELSE 0 END) AS ACOS_30d,
                                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.sales14d ELSE 0 END) AS ACOS_7d,
                                            SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) / SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN a.sales14d ELSE 0 END)  AS ACOS_yesterday

                            FROM
                            amazon_search_term_reports_sp a
                            JOIN
                                amazon_campaigns_list_sp c ON a.campaignId = c.campaignId -- 联接广告活动表，获取广告活动类型
                            WHERE
                                a.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND '{}'- INTERVAL 1 DAY
                                AND a.market = '{}'
                                and a.keywordId in (select keywordId from amazon_search_term_reports_sp where campaignStatus='ENABLED' and date='{}'-INTERVAL 1 DAY)
                                AND c.targetingType like '%AUT%' -- 筛选出手动广告
                                AND campaignName LIKE '%_overstock%'
                                AND NOT EXISTS (
                            SELECT 1
                            FROM amazon_targeting_reports_sp
                            WHERE keyword = a.searchTerm
                              AND campaignId = a.campaignId
                              AND adGroupId = a.adGroupId
                        )
                            GROUP BY
                              a.adGroupName,
                              a.campaignId,
                              a.keyword,
                              a.searchTerm,
                              a.matchType
                            ORDER BY
                              a.adGroupName,
                              a.campaignId,
                              a.keyword,
                              a.searchTerm;
                                                            """.format(cur_time, cur_time, cur_time, cur_time, cur_time,
                                                                       cur_time,
                                                                       cur_time, cur_time,
                                                                       cur_time, cur_time, cur_time, cur_time, cur_time,
                                                                       cur_time,
                                                                       cur_time, cur_time, cur_time,
                                                                       cur_time, cur_time, cur_time, cur_time, cur_time,
                                                                       cur_time,
                                                                       cur_time, cur_time, cur_time,
                                                                       cur_time, cur_time, cur_time, cur_time, cur_time,
                                                                       cur_time,
                                                                       country,
                                                                       cur_time)
            else:
                query = None
            # print(query)
            df1 = pd.read_sql(query, con=conn)
            output_filename = '.\滞销品优化\自动sp广告\搜索词优化\预处理.csv'
            df1.to_csv(output_filename, index=False, encoding='utf-8-sig')
            csv_to_json(output_filename)
            # return df
            return print("查询已完成，请查看文件： " + output_filename)

        except Exception as error:
            print("Error while inserting data:", error)

    def preprocessing_sp_anomaly_detection_macroscopic(self, country, cur_time, version=1.0):

        """"""
        try:
            conn = self.conn
            select = 0.17
            if version == 1.0:
                query = f"""
WITH DailyData AS (
    SELECT
        sp.market,
        sp.date,
        COALESCE(sp.sum_cost, 0) AS sp_cost,
        COALESCE(sd.sum_cost, 0) AS sd_cost,
        COALESCE(sb.sum_cost, 0) AS sb_cost,
        COALESCE(sp.sum_sales, 0) AS sp_sales,
        COALESCE(sd.sum_sales, 0) AS sd_sales,
        COALESCE(sb.sum_sales, 0) AS sb_sales,
        sp.DeepBI0502_sales,
        sd.DeepBI0507_sales,
        sd.DeepBI0509_sales,
        sp.DeepBI0514_sales,
        sp.DeepBI0502_cost,
        sd.DeepBI0507_cost,
        sd.DeepBI0509_cost,
        sp.DeepBI0514_cost
    FROM
        (
        SELECT
            market,
            date,
            SUM(cost) AS sum_cost,
            SUM(sales7d) AS sum_sales,
            SUM(IF(campaignName LIKE 'DeepBI_0502%', sales7d, 0)) AS DeepBI0502_sales,
            SUM(IF(campaignName LIKE 'DeepBI_0514%', sales7d, 0)) AS DeepBI0514_sales,
            SUM(IF(campaignName LIKE 'DeepBI_0502%', cost, 0)) AS DeepBI0502_cost,
            SUM(IF(campaignName LIKE 'DeepBI_0514%', cost, 0)) AS DeepBI0514_cost
        FROM
            amazon_campaign_reports_sp
        WHERE
            date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 3 DAY) AND '{cur_time}' - INTERVAL 1 DAY
        GROUP BY
            market,
            date
        ) AS sp
        LEFT JOIN (
        SELECT
            market,
            date,
            SUM(cost) AS sum_cost,
            SUM(sales) AS sum_sales,
            SUM(IF(campaignName LIKE 'DeepBI_0507%', sales, 0)) AS DeepBI0507_sales,
            SUM(IF(campaignName LIKE 'DeepBI_0509%', sales, 0)) AS DeepBI0509_sales,
            SUM(IF(campaignName LIKE 'DeepBI_0507%', cost, 0)) AS DeepBI0507_cost,
            SUM(IF(campaignName LIKE 'DeepBI_0509%', cost, 0)) AS DeepBI0509_cost
        FROM
            amazon_campaign_reports_sd
        WHERE
            date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 3 DAY) AND '{cur_time}' - INTERVAL 1 DAY
        GROUP BY
            market,
            date
        ) AS sd
        ON sd.market = sp.market AND sd.date = sp.date
        LEFT JOIN (
        SELECT
            market,
            date,
            SUM(cost) AS sum_cost,
            SUM(sales) AS sum_sales
        FROM
            amazon_campaign_reports_sb
        WHERE
            date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 3 DAY) AND '{cur_time}' - INTERVAL 1 DAY
        GROUP BY
            market,
            date
        ) AS sb
        ON sb.market = sp.market AND sb.date = sp.date
    WHERE
        sp.market IN ('{country}')
),
ThreeDayAvg AS (
  SELECT
    market,
    ROUND(AVG(sp_sales + sd_sales + sb_sales), 2) AS avg_sales,
    ROUND(AVG(sp_cost + sd_cost + sb_cost), 2) AS avg_cost
  FROM DailyData
  WHERE date BETWEEN DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 3 DAY) AND DATE_SUB('{cur_time}' - INTERVAL 1 DAY, INTERVAL 1 DAY)
  GROUP BY market
),
CalculatedData AS (
    SELECT
        dd.market,
        dd.date,
        IF(
            dd.date = (SELECT MAX(date) FROM DailyData),
            dd.sp_sales - LAG(dd.sp_sales, 1, 0) OVER (PARTITION BY dd.market ORDER BY dd.date),
            NULL
        ) AS sp计划销售额变化,
        IF(
            dd.date = (SELECT MAX(date) FROM DailyData),
            dd.sd_sales - LAG(dd.sd_sales, 1, 0) OVER (PARTITION BY dd.market ORDER BY dd.date),
            NULL
        ) AS sd计划销售额变化,
        IF(
            dd.date = (SELECT MAX(date) FROM DailyData),
            dd.DeepBI0502_sales - LAG(dd.DeepBI0502_sales, 1, 0) OVER (PARTITION BY dd.market ORDER BY dd.date),
            NULL
        ) AS DeepBI0502_sales变化,
        IF(
            dd.date = (SELECT MAX(date) FROM DailyData),
            dd.DeepBI0507_sales - LAG(dd.DeepBI0507_sales, 1, 0) OVER (PARTITION BY dd.market ORDER BY dd.date),
            NULL
        ) AS DeepBI0507_sales变化,
        IF(
            dd.date = (SELECT MAX(date) FROM DailyData),
            dd.DeepBI0509_sales - LAG(dd.DeepBI0509_sales, 1, 0) OVER (PARTITION BY dd.market ORDER BY dd.date),
            NULL
        ) AS DeepBI0509_sales变化,
        IF(
            dd.date = (SELECT MAX(date) FROM DailyData),
            dd.DeepBI0514_sales - LAG(dd.DeepBI0514_sales, 1, 0) OVER (PARTITION BY dd.market ORDER BY dd.date),
            NULL
        ) AS DeepBI0514_sales变化,
        IF(
            dd.date = (SELECT MAX(date) FROM DailyData),
            dd.sp_cost - LAG(dd.sp_cost, 1, 0) OVER (PARTITION BY dd.market ORDER BY dd.date),
            NULL
        ) AS sp计划花费变化,
        IF(
            dd.date = (SELECT MAX(date) FROM DailyData),
            dd.sd_cost - LAG(dd.sd_cost, 1, 0) OVER (PARTITION BY dd.market ORDER BY dd.date),
            NULL
        ) AS sd计划花费变化,
        IF(
            dd.date = (SELECT MAX(date) FROM DailyData),
            dd.DeepBI0502_cost - LAG(dd.DeepBI0502_cost, 1, 0) OVER (PARTITION BY dd.market ORDER BY dd.date),
            NULL
        ) AS DeepBI0502_cost变化,
        IF(
            dd.date = (SELECT MAX(date) FROM DailyData),
            dd.DeepBI0507_cost - LAG(dd.DeepBI0507_cost, 1, 0) OVER (PARTITION BY dd.market ORDER BY dd.date),
            NULL
        ) AS DeepBI0507_cost变化,
        IF(
            dd.date = (SELECT MAX(date) FROM DailyData),
            dd.DeepBI0509_cost - LAG(dd.DeepBI0509_cost, 1, 0) OVER (PARTITION BY dd.market ORDER BY dd.date),
            NULL
        ) AS DeepBI0509_cost变化,
        IF(
            dd.date = (SELECT MAX(date) FROM DailyData),
            dd.DeepBI0514_cost - LAG(dd.DeepBI0514_cost, 1, 0) OVER (PARTITION BY dd.market ORDER BY dd.date),
            NULL
        ) AS DeepBI0514_cost变化
    FROM DailyData dd
    WHERE
        dd.date IN ('{cur_time}' - INTERVAL 1 DAY, '{cur_time}' - INTERVAL 2 DAY)
)
SELECT
    dd.market,
    dd.date,
    (dd.sp_cost + dd.sd_cost + dd.sb_cost) AS '广告总花费',
    (dd.sp_sales + dd.sd_sales + dd.sb_sales) AS '广告总销量',
    ROUND((dd.sp_cost + dd.sd_cost + dd.sb_cost) / (dd.sp_sales + dd.sd_sales + dd.sb_sales), 4) AS '总ACOS',
    IF(dd.date = '{cur_time}' - INTERVAL 1 DAY, ta.avg_sales, NULL) AS '前三天平均销售额值',
    IF(
        dd.date = '{cur_time}' - INTERVAL 1 DAY,
        CASE
            WHEN (dd.sp_sales + dd.sd_sales + dd.sb_sales) - LAG(dd.sp_sales + dd.sd_sales + dd.sb_sales, 1, 0) OVER (PARTITION BY dd.market ORDER BY dd.date) > ({select} * ta.avg_sales) THEN '广告总销售额异常好'
            WHEN LAG(dd.sp_sales + dd.sd_sales + dd.sb_sales, 1, 0) OVER (PARTITION BY dd.market ORDER BY dd.date) - (dd.sp_sales + dd.sd_sales + dd.sb_sales) > ({select} * ta.avg_sales) THEN '广告总销售额异常差'
            ELSE NULL
        END,
        NULL
    ) AS '销售额异常现象',
    IF(dd.date = '{cur_time}' - INTERVAL 1 DAY, ta.avg_cost, NULL) AS '前三天平均广告花费',
    IF(
        dd.date = '{cur_time}' - INTERVAL 1 DAY,
        CASE
            WHEN (dd.sp_cost + dd.sd_cost + dd.sb_cost) - LAG(dd.sp_cost + dd.sd_cost + dd.sb_cost, 1, 0) OVER (PARTITION BY dd.market ORDER BY dd.date) > ({select} * ta.avg_cost) THEN '广告总花费异常多'
            WHEN LAG(dd.sp_cost + dd.sd_cost + dd.sb_cost, 1, 0) OVER (PARTITION BY dd.market ORDER BY dd.date) - (dd.sp_cost + dd.sd_cost + dd.sb_cost) > ({select} * ta.avg_cost) THEN '广告总花费异常少'
            ELSE NULL
        END,
        NULL
    ) AS '广告总花费异常现象',
    cd.sp计划销售额变化,
    cd.sp计划花费变化,
    COALESCE(ROUND((dd.sp_cost / dd.sp_sales), 4), 0) AS sp_acos,
    cd.sd计划销售额变化,
    cd.sd计划花费变化,
    COALESCE(ROUND((dd.sd_cost / dd.sd_sales), 4), 0) AS sd_acos,
    cd.DeepBI0502_sales变化,
    cd.DeepBI0502_cost变化,
    COALESCE(ROUND((dd.DeepBI0502_cost / dd.DeepBI0502_sales), 4), 0) AS DeepBI0502_acos,
    cd.DeepBI0507_sales变化,
    cd.DeepBI0507_cost变化,
    COALESCE(ROUND((dd.DeepBI0507_cost / dd.DeepBI0507_sales), 4), 0) AS DeepBI0507_acos,
    cd.DeepBI0509_sales变化,
    cd.DeepBI0509_cost变化,
    COALESCE(ROUND((dd.DeepBI0509_cost / dd.DeepBI0509_sales), 4), 0) AS DeepBI0509_acos,
    cd.DeepBI0514_sales变化,
    cd.DeepBI0514_cost变化,
    COALESCE(ROUND((dd.DeepBI0514_cost / dd.DeepBI0514_sales), 4), 0) AS DeepBI0514_acos

FROM DailyData dd
LEFT JOIN ThreeDayAvg ta ON dd.market = ta.market
LEFT JOIN CalculatedData cd ON dd.market = cd.market and dd.date = cd.date
WHERE
    dd.date IN ('{cur_time}' - INTERVAL 1 DAY, '{cur_time}' - INTERVAL 2 DAY)
ORDER BY
    dd.market,
    dd.date;
                """
            else:
                query = None
            # print(query)
            df1 = pd.read_sql(query, con=conn)
            output_path = self.brand + '_' + country + '_' + cur_time
            name = "异常检测_宏观销售额异常现象" + '_' + self.brand + '_' + country + '_' + cur_time + '.csv'
            output_filename = f'C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\输出结果\\{output_path}\\{name}.csv'
            df1.to_csv(output_filename, index=False, encoding='utf-8-sig')
            # return df
            return print("查询已完成，请查看文件： " + output_filename)

        except Exception as error:
            print("Error while inserting data:", error)

    def preprocessing_sp_anomaly_detection_reason(self, country, cur_time, version=1.0):

        """"""
        try:
            conn = self.conn
            select = 0.17
            if version == 1.0:
                query = f"""
SELECT
    date,
    market,
    campaignName,
    campaignId,
    sales,
    cost,
    sales_change,
    cost_change,
    ACOS,
    acos_change
FROM (
  SELECT
    date,
    market,
    campaignName,
    campaignId,
    sales,
    cost,
    CASE WHEN date = DATE('{cur_time}' - INTERVAL 1 DAY) THEN COALESCE(sales - LAG(sales, 1, 0) OVER (PARTITION BY campaignId ORDER BY date), 0) ELSE NULL END AS sales_change,
    CASE WHEN date = DATE('{cur_time}' - INTERVAL 1 DAY) THEN COALESCE(cost - LAG(cost, 1, 0) OVER (PARTITION BY campaignId ORDER BY date), 0) ELSE NULL END AS cost_change,
    ACOS,
    CASE WHEN date = DATE('{cur_time}' - INTERVAL 1 DAY) THEN COALESCE(ACOS - LAG(ACOS, 1, 0) OVER (PARTITION BY campaignId ORDER BY date), 0) ELSE NULL END AS acos_change
  FROM (
    SELECT
        date,
        market,
        campaignName,
        campaignId,
        sales7d AS sales,
        cost,
        ROUND((cost / sales7d), 4) AS ACOS,
        COUNT(*) OVER (PARTITION BY campaignName) AS campaign_count
    FROM
        amazon_campaign_reports_sp
    WHERE
        date >= DATE('{cur_time}' - INTERVAL 1 DAY) - INTERVAL 1 DAY
        AND market = '{country}'
        AND (campaignName LIKE 'DeepBI_0502%' OR campaignName LIKE 'DeepBI_0514%')
    UNION ALL
    SELECT
        t1.date,
        t1.market,
        t1.campaignName,
        t1.campaignId,
        t1.sales AS sales,
        t1.cost,
        ROUND((t1.cost / t1.sales), 4) AS ACOS,
        COUNT(*) OVER (PARTITION BY t1.campaignName) AS campaign_count
    FROM
        amazon_campaign_reports_sd t1
    WHERE
        t1.date >= DATE('{cur_time}' - INTERVAL 1 DAY) - INTERVAL 1 DAY
        AND t1.market = '{country}'
        AND (t1.campaignName LIKE 'DeepBI_0509%' OR t1.campaignName LIKE 'DeepBI_0507%')
  ) AS tmp
  WHERE campaign_count >= 2
) as tmp2
WHERE date = DATE('{cur_time}' - INTERVAL 1 DAY)
ORDER BY
    CASE
        WHEN campaignName LIKE '%0502%' THEN 1
        WHEN campaignName LIKE '%0507%' THEN 2
        WHEN campaignName LIKE '%0509%' THEN 3
        WHEN campaignName LIKE '%0514%' THEN 4
        ELSE 5
    END,
    sales DESC;
                """
            else:
                query = None
            # print(query)
            df1 = pd.read_sql(query, con=conn)
            output_path = self.brand + '_' + country + '_' + cur_time
            name = "异常检测_宏观销售额异常原因" + '_' + self.brand + '_' + country + '_' + cur_time + '.csv'
            output_filename = f'C:\\Users\\admin\\PycharmProjects\\DeepBI\\ai\\backend\\util\\db\\auto_yzj\\日常优化\\输出结果\\{output_path}\\{name}.csv'
            df1.to_csv(output_filename, index=False, encoding='utf-8-sig')
            # return df
            return print("查询已完成，请查看文件： " + output_filename)

        except Exception as error:
            print("Error while inserting data:", error)


    def preprocessing_sd_budget(self, country, cur_time, version=1.0):

        """"""
        try:
            conn = self.conn
            api = BudgetQuerySD()
            if version == 1.0 or version == '初阶':
                query = api.get_query_v1_0(cur_time, country)
            else:
                query = None
            # print(query)
            df1 = pd.read_sql(query, con=conn)
            output_filename = '.\日常优化\sd广告\预算优化\预处理.csv'
            df1.to_csv(output_filename, index=False, encoding='utf-8-sig')
            csv_to_json(output_filename)
            # return df
            return print("查询已完成，请查看文件： " + output_filename)

        except Exception as error:
            print("Error while inserting data:", error)

    def preprocessing_sd_sku(self, country, cur_time, version=1.1):

        """"""
        try:
            conn = self.conn
            api = SkuQuerySD()
            if version == 1.0:
                query = api.get_query_v1_0(cur_time, country)
            elif version == 1.1 or version == '初阶':
                query = api.get_query_v1_1(cur_time, country)
            else:
                query = None
            # print(query)
            df1 = pd.read_sql(query, con=conn)
            output_filename = '.\日常优化\sd广告\关闭SKU\预处理.csv'
            df1.to_csv(output_filename, index=False, encoding='utf-8-sig')
            csv_to_json(output_filename)
            # return df
            return print("查询已完成，请查看文件： " + output_filename)

        except Exception as error:
            print("Error while inserting data:", error)

    def preprocessing_sd_sku_reopen(self, country, cur_time, version=1.0):

        """"""
        try:
            conn = self.conn
            api = reopenSkuQuerySD()
            if version == 1.0 or version == '初阶':
                query = api.get_query_v1_0(cur_time, country)
            else:
                query = None
            # print(query)
            df1 = pd.read_sql(query, con=conn)
            output_filename = '.\日常优化\sd广告\复开SKU\预处理.csv'
            df1.to_csv(output_filename, index=False, encoding='utf-8-sig')
            csv_to_json(output_filename)
            # return df
            return print("查询已完成，请查看文件： " + output_filename)

        except Exception as error:
            print("Error while inserting data:", error)

    def preprocessing_sd_product_targets(self, country, cur_time, version=1.0):

        """"""
        try:
            conn = self.conn

            api = ProductTargetsQuerySD()
            if version == 1.0:
                query = api.get_query_v1_0(cur_time, country)
            elif version == 1.1 or version == '初阶':
                query = api.get_query_v1_1(cur_time, country)
            else:
                query = None
            # print(query)
            df1 = pd.read_sql(query, con=conn)
            output_filename = '.\日常优化\sd广告\商品投放优化\预处理.csv'
            df1.to_csv(output_filename, index=False, encoding='utf-8-sig')
            csv_to_json(output_filename)
            # return df
            return print("查询已完成，请查看文件： " + output_filename)

        except Exception as error:
            print("Error while inserting data:", error)

    def preprocessing_sd_sp_product_targets(self, country, cur_time, version=1.0):

        """"""
        try:
            conn = self.conn

            if version == 1.0:
                query = f"""
                WITH a AS (
                    SELECT
                        b.campaignName,
                        b.campaignId,
                        b.adGroupName,
                        b.adGroupId,
                        CASE
                            WHEN b.targetingText LIKE '%asinExpandedFrom%' THEN CONCAT('asin-expanded="', SUBSTRING_INDEX(SUBSTRING_INDEX(b.targetingText, 'value\\': \\'', -1), '\\'}}', 1), '"')
                            WHEN b.targetingText LIKE '%asinSameAs%' THEN CONCAT('asin="', SUBSTRING_INDEX(SUBSTRING_INDEX(b.targetingText, 'value\\': \\'', -1), '\\'}}', 1), '"')
                            WHEN b.targetingText LIKE '%asinCategorySameAs%' AND b.targetingText LIKE '%asinBrandSameAs%' THEN CONCAT('category="', SUBSTRING_INDEX(SUBSTRING_INDEX(b.targetingText, 'asinCategorySameAs\\', \\'value\\': \\'', -1), '\\'}},', 1), '" ', 'brand="', SUBSTRING_INDEX(SUBSTRING_INDEX(b.targetingText, 'asinBrandSameAs\\', \\'value\\': \\'', -1), '\\'}}', 1), '"')
                            WHEN b.targetingText LIKE '%asinCategorySameAs%' THEN CONCAT('category="', SUBSTRING_INDEX(SUBSTRING_INDEX(b.targetingText, 'value\\': \\'', -1), '\\'}}', 1), '"')
                            ELSE b.targetingText
                        END AS targetingText,
                        b.targetingId,
                        b.sales,
                        b.cost,
                        bid,
                        b.date
                    FROM
                        amazon_targeting_reports_sd b
                        JOIN amazon_targets_list_sd c ON b.targetingId = c.targetId
                    WHERE
                        b.market = '{country}'
                        AND b.date BETWEEN DATE_SUB('{cur_time}', INTERVAL 15 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY)
                    UNION
                    SELECT
                        NULL AS campaignName,
                        akl.campaignId,
                        NULL AS adGroupName,
                        akl.adGroupId,
                        CASE
                            WHEN akl.resolvedExpression LIKE '%asinExpandedFrom%' THEN CONCAT('asin-expanded="', SUBSTRING_INDEX(SUBSTRING_INDEX(akl.resolvedExpression, 'value\\': \\'', -1), '\\'}}', 1), '"')
                            WHEN akl.resolvedExpression LIKE '%asinSameAs%' THEN CONCAT('asin="', SUBSTRING_INDEX(SUBSTRING_INDEX(akl.resolvedExpression, 'value\\': \\'', -1), '\\'}}', 1), '"')
                            WHEN akl.resolvedExpression LIKE '%asinCategorySameAs%' AND akl.resolvedExpression LIKE '%asinBrandSameAs%' THEN CONCAT('category="', SUBSTRING_INDEX(SUBSTRING_INDEX(akl.resolvedExpression, 'asinCategorySameAs\\', \\'value\\': \\'', -1), '\\'}},', 1), '" ', 'brand="', SUBSTRING_INDEX(SUBSTRING_INDEX(akl.resolvedExpression, 'asinBrandSameAs\\', \\'value\\': \\'', -1), '\\'}}', 1), '"')
                            WHEN akl.resolvedExpression LIKE '%asinCategorySameAs%' THEN CONCAT('category="', SUBSTRING_INDEX(SUBSTRING_INDEX(akl.resolvedExpression, 'value\\': \\'', -1), '\\'}}', 1), '"')
                            ELSE akl.resolvedExpression
                        END AS targetingText,
                        akl.targetId AS targetingId,
                        0 AS sales,
                        0 AS cost,
                        akl.bid AS bid,
                        DATE_SUB('{cur_time}', INTERVAL 1 DAY) AS date
                    FROM
                        amazon_targets_list_sd akl
                    WHERE
                        akl.market = '{country}'
                        AND akl.state = 'enabled'
                        AND akl.servingStatus NOT IN ('CAMPAIGN_PAUSED', 'AD_GROUP_PAUSED', 'TARGETING_CLAUSE_PAUSED')
                        AND akl.targetId NOT IN (
                            SELECT
                                b.targetingId
                            FROM
                                amazon_targeting_reports_sd b
                            WHERE
                                b.market = '{country}'
                                AND b.date BETWEEN DATE_SUB('{cur_time}', INTERVAL 15 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY)
                        )
                ),
                b AS (
                    SELECT
                        campaignName,
                        campaignId,
                        adGroupName,
                        adGroupId,
                        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}', INTERVAL 7 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN sales ELSE 0 END) AS total_sales_7d,
                        SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}', INTERVAL 7 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_7d
                    FROM
                        a
                    GROUP BY
                        campaignId,
                        adGroupId
                )
                SELECT
                    a.campaignName,
                    a.campaignId,
                    a.adGroupName,
                    a.adGroupId,
                    a.targetingText,
                    a.targetingId AS keywordId,
                    b.total_sales_7d,
                    b.total_cost_7d,
                    a.bid
                FROM a
                JOIN b ON a.campaignId = b.campaignId AND a.adGroupId = b.adGroupId
                GROUP BY
                    a.campaignName,
                    a.campaignId,
                    a.adGroupName,
                    a.adGroupId,
                    a.targetingText,
                    a.targetingId,
                    b.total_sales_7d,
                    b.total_cost_7d,
                    a.bid
                ORDER BY
                    a.campaignId,
                    a.adGroupId;
                """
            #                 query = f"""
# WITH a AS (
#     SELECT
#         b.campaignName,
#         b.campaignId,
#         b.adGroupName,
#         b.adGroupId,
#         CASE
#             WHEN b.targetingText LIKE '%asinExpandedFrom%' THEN CONCAT('asin-expanded="', SUBSTRING_INDEX(SUBSTRING_INDEX(b.targetingText, 'value\': \'', -1), '\'}}', 1), '"')
#             WHEN b.targetingText LIKE '%asinSameAs%' THEN CONCAT('asin="', SUBSTRING_INDEX(SUBSTRING_INDEX(b.targetingText, 'value\': \'', -1), '\'}}', 1), '"')
#             WHEN b.targetingText LIKE '%asinCategorySameAs%' AND b.targetingText LIKE '%asinBrandSameAs%' THEN CONCAT('category="', SUBSTRING_INDEX(SUBSTRING_INDEX(b.targetingText, 'asinCategorySameAs\', \'value\': \'', -1), '\'}},', 1), '" ', 'brand="', SUBSTRING_INDEX(SUBSTRING_INDEX(b.targetingText, 'asinBrandSameAs\', \'value\': \'', -1), '\'}}', 1), '"')
#             WHEN b.targetingText LIKE '%asinCategorySameAs%' THEN CONCAT('category="', SUBSTRING_INDEX(SUBSTRING_INDEX(b.targetingText, 'value\': \'', -1), '\'}}', 1), '"')
#             ELSE b.targetingText
#         END AS targetingText,
#         targetingId,
#         sales,
#         cost,
#         bid,
#         date
#     FROM
#         amazon_targeting_reports_sd b
#         JOIN amazon_targets_list_sd c ON b.targetingId = c.targetId
#     WHERE
#         b.market = '{country}'
#         AND b.date BETWEEN DATE_SUB('{cur_time}', INTERVAL 15 DAY)
#         AND DATE_SUB('{cur_time}', INTERVAL 1 DAY)
#     UNION
#     SELECT
#         NULL AS campaignName,
#         akl.campaignId,
#         NULL AS adGroupName,
#         akl.adGroupId,
#         CASE
#             WHEN akl.resolvedExpression LIKE '%asinExpandedFrom%' THEN CONCAT('asin-expanded="', SUBSTRING_INDEX(SUBSTRING_INDEX(akl.resolvedExpression, 'value\': \'', -1), '\'}}', 1), '"')
#             WHEN akl.resolvedExpression LIKE '%asinSameAs%' THEN CONCAT('asin="', SUBSTRING_INDEX(SUBSTRING_INDEX(akl.resolvedExpression, 'value\': \'', -1), '\'}}', 1), '"')
#             WHEN akl.resolvedExpression LIKE '%asinCategorySameAs%' AND akl.resolvedExpression LIKE '%asinBrandSameAs%' THEN CONCAT('category="', SUBSTRING_INDEX(SUBSTRING_INDEX(akl.resolvedExpression, 'asinCategorySameAs\', \'value\': \'', -1), '\'}},', 1), '" ', 'brand="', SUBSTRING_INDEX(SUBSTRING_INDEX(akl.resolvedExpression, 'asinBrandSameAs\', \'value\': \'', -1), '\'}}', 1), '"')
#             WHEN akl.resolvedExpression LIKE '%asinCategorySameAs%' THEN CONCAT('category="', SUBSTRING_INDEX(SUBSTRING_INDEX(akl.resolvedExpression, 'value\': \'', -1), '\'}}', 1), '"')
#             ELSE akl.resolvedExpression
#         END AS targetingText,
#         akl.targetId AS targetingId,
#         0 AS sales,
#         0 AS cost,
#         bid,
#         DATE_SUB('{cur_time}', INTERVAL 1 DAY) AS date
#     FROM
#         amazon_targets_list_sd akl
#     WHERE
#         akl.market = '{country}'
#         AND akl.state = 'enabled'
#         AND akl.servingStatus NOT IN ('CAMPAIGN_PAUSED', 'AD_GROUP_PAUSED', 'TARGETING_CLAUSE_PAUSED')
#         AND akl.targetId NOT IN (
#             SELECT
#                 b.targetingId
#             FROM
#                 amazon_targeting_reports_sd b
#             WHERE
#                 b.market = '{country}'
#                 AND b.date BETWEEN DATE_SUB('{cur_time}', INTERVAL 15 DAY)
#                 AND DATE_SUB('{cur_time}', INTERVAL 1 DAY)
#         )
#     ORDER BY
#         campaignId,
#         adGroupId
# ),
# b AS (
#     SELECT
#         campaignName,
#         campaignId,
#         adGroupName,
#         adGroupId,
#         SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}', INTERVAL 7 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN sales ELSE 0 END) AS total_sales_7d,
#         SUM(CASE WHEN date BETWEEN DATE_SUB('{cur_time}', INTERVAL 7 DAY) AND DATE_SUB('{cur_time}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_7d
#     FROM
#         a
#     GROUP BY
#         campaignId,
#         adGroupId
# )
# SELECT
#     a.campaignName,
#     a.campaignId,
#     a.adGroupName,
#     a.adGroupId,
#     a.targetingText,
#     a.targetingId,
#     b.total_sales_7d,
#     b.total_cost_7d,
#     a.bid
# FROM a
# JOIN b ON a.campaignId = b.campaignId AND a.adGroupId = b.adGroupId
# GROUP BY
#     a.targetingId
# ORDER BY
#     a.campaignId,
#     a.adGroupId;
#                                                             """
            else:
                query = None
            # print(query)
            df1 = pd.read_sql(query, con=conn)
            output_filename = '.\日常优化\sd广告\特殊商品投放\预处理.csv'
            df1.to_csv(output_filename, index=False, encoding='utf-8-sig')
            csv_to_json(output_filename)
            # return df
            return print("查询已完成，请查看文件： " + output_filename)

        except Exception as error:
            print("Error while inserting data:", error)


# amr = AmazonMysqlRagUitl('LAPASA')
# #amr.preprocessing_sp_anomaly_detection_macroscopic('US','2024-07-09')
# amr.preprocessing_sp_overstock_targeting_group_manual('US','2024-07-17')
