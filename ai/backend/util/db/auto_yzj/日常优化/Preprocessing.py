import json

import pymysql
import pandas as pd
from datetime import datetime
import warnings
import os
from ai.backend.util.db.auto_yzj.utils.trans_to import csv_to_json
from ai.backend.util.db.auto_yzj.日常优化.手动sp广告.SKU优化.query import skuquery_manual

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
        self.brand = brand
        self.db_info = self.load_db_info(brand)
        self.conn = self.connect(self.db_info)

    def load_db_info(self, brand):
        # 从 JSON 文件加载数据库信息
        with open('C:/Users/admin/PycharmProjects/DeepBI/ai/backend/util/db/db_info.json', 'r') as f:
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

    def preprocessing_sku(self, country, cur_time, version=1.3):

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
                    AND campaignName NOT LIKE '%_overstock%'
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
                                        """.format(cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                   cur_time,
                                                   cur_time, cur_time,
                                                   cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                   cur_time,
                                                   cur_time,
                                                   cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                   cur_time,
                                                   cur_time, cur_time, cur_time, cur_time, cur_time,
                                                   cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                   cur_time,
                                                   country, cur_time)
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

    def preprocessing_keyword(self, country, cur_time, version=1.2):

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
        -- ... 其他字段和聚合计算
                                  SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN purchases7d ELSE 0 END) AS ORDER_1m,
    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_30d,
    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_7d,
    SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN clicks ELSE 0 END) AS total_clicks_yesterday,
                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_30d,
                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_7d,
                SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END) AS total_sales14d_yesterday,
                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_30d,
                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_7d,
                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 3 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_4d,
                SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) AS total_cost_yesterday,
                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_30d,
               SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_7d,
     SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) / SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END)  AS ACOS_yesterday
    FROM
        amazon_targeting_reports_sp
    WHERE
        date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) and DATE_SUB('{}', INTERVAL 1 DAY)
        -- ... 其他 WHERE 条件
                                AND market = '{}'
    -- 确保keywordId是来自特定日期的启用状态的campaign
    AND keywordId IN (SELECT keywordId FROM amazon_targeting_reports_sp WHERE campaignStatus = 'ENABLED' AND date = '{}'- INTERVAL 1 DAY)
    -- 确保campaignName包含特定文本
    AND (campaignName LIKE '%MAN%' OR campaignName LIKE '%手动%' OR campaignName LIKE '%Man%' OR campaignName LIKE '%man%')
    -- 排除最近4天内有变更的keywordId
    AND keywordId NOT IN (
        SELECT DISTINCT entityId
        FROM amazon_advertising_change_history
        WHERE timestamp >= (UNIX_TIMESTAMP(NOW(3)) - 4 * 24 * 60 * 60) * 1000
        AND entityType = 'KEYWORD'
        AND market = '{}')
    GROUP BY
        adGroupName,
        campaignName,
        keyword,
        matchType,
        keywordId,
        targeting
    ORDER BY
        adGroupName,
        campaignName,
        keyword,
        matchType,
        keywordId,
        targeting)
SELECT
    a.*,
    b.keywordBid
FROM
    a
LEFT JOIN
    amazon_targeting_reports_sp b ON a.keywordId = b.keywordId
WHERE
    b.date = DATE_SUB('{}', INTERVAL 1 DAY)
                """.format(cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,country,cur_time,country,cur_time)
            elif version == 1.1:
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
        SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN clicks ELSE 0 END) AS total_clicks_yesterday,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_30d,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_7d,
        SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END) AS total_sales14d_yesterday,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_30d,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_7d,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 3 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_4d,
        SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) AS total_cost_yesterday,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_30d,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_7d,
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
    )
    AND c.targetingType like '%MAN%'
    AND  b.keywordId not in (SELECT DISTINCT entityId
        FROM amazon_advertising_change_history
        WHERE timestamp >= (UNIX_TIMESTAMP(NOW(3)) - 4 * 24 * 60 * 60) * 1000
        AND entityType = 'KEYWORD'
        AND market = '{}')
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
                """.format(cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,
                           cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,
                           cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,
                           cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,
                           cur_time,cur_time,cur_time,cur_time,country,cur_time,country,cur_time)
            elif version == 1.2:
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
                    AND campaignName NOT LIKE '%_overstock%'
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
                                       cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                       cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                       cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                       cur_time, cur_time, cur_time, cur_time, country, cur_time, country, cur_time)
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

    def preprocessing_targeting_group(self, country, cur_time, version=1.2):

        """"""
        try:
            conn = self.conn

            if version == 1.0:
                query = """
        SELECT placementClassification,
            campaignName,
            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_3d,
            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_7d,
            SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN clicks ELSE 0 END) AS total_clicks_yesterday,
            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_3d,
            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_7d,
            SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END) AS total_sales14d_yesterday,
            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_3d,
            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_7d,
            SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) AS total_cost_yesterday,
            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_3d,
            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_7d,
            SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) / SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END)  AS ACOS_yesterday
        FROM
        amazon_campaign_placement_reports_sp
        WHERE
            DATE BETWEEN DATE_SUB('{}', INTERVAL 30 DAY)
            AND ('{}'-INTERVAL 1 DAY)
            AND market = '{}'
            AND  campaignId in (select campaignId from amazon_targeting_reports_sp where campaignStatus='ENABLED' and date=DATE_SUB('{}', INTERVAL 1 DAY))
            AND( campaignName not LIKE '%AUTO%' and  campaignName not  LIKE '%auto%' and campaignName not LIKE '%Auto%' and  campaignName not LIKE '%自动%' )

        GROUP BY
          campaignName,
          placementClassification
        ORDER BY
          campaignName,
          placementClassification;
                """.format(cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,country,cur_time)
            elif version == 1.1:
                query = """
                        SELECT placementClassification,
                            campaignName,
                            campaignId,
                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_3d,
                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_7d,
                            SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN clicks ELSE 0 END) AS total_clicks_yesterday,
                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_3d,
                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_7d,
                            SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END) AS total_sales14d_yesterday,
                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_3d,
                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_7d,
                            SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) AS total_cost_yesterday,
                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_3d,
                            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_7d,
                            SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) / SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END)  AS ACOS_yesterday

                        FROM
                        amazon_campaign_placement_reports_sp
                        WHERE
                            DATE BETWEEN DATE_SUB('{}', INTERVAL 30 DAY)
                            AND ('{}'-INTERVAL 1 DAY)
                            AND market = '{}'
                            AND  campaignId in (select campaignId from amazon_targeting_reports_sp where campaignStatus='ENABLED' and date=DATE_SUB('{}', INTERVAL 1 DAY))
                            AND( campaignName not LIKE '%AUTO%' and  campaignName not  LIKE '%auto%' and campaignName not LIKE '%Auto%' and  campaignName not LIKE '%自动%' )

                        GROUP BY
                          campaignName,
                          placementClassification
                        ORDER BY
                          campaignName,
                          placementClassification;
                                """.format(cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                           cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                           cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                           cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, country,
                                           cur_time)
            elif version == 1.2:
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
        FROM amazon_campaigns_list_sp
        WHERE campaignStatus = 'ENABLED' AND date = '{}' - INTERVAL 1 DAY
        AND campaignName NOT LIKE '%_overstock%'
    )
    AND c.targetingType LIKE '%MAN%' -- 筛选出手动广告
GROUP BY
    a.campaignName,
    a.campaignId,
    a.placementClassification,
    c.dynamicBidding_placementTop_percentage,
    c.dynamicBidding_placementProductPage_percentage,
    c.dynamicBidding_placementRestOfSearch_percentage
ORDER BY
    a.campaignName,
    a.placementClassification;
                                """.format(cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                           cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                           cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                           cur_time, cur_time, cur_time, cur_time, country, cur_time)
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

    def preprocessing_search_term(self, country, cur_time, version=1.4):

        """"""
        try:
            conn = self.conn

            if version == 1.0:
                query = """
SELECT keyword,searchTerm,adGroupName,matchType,
    campaignName,
    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_30d,
    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_7d,
    SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN clicks ELSE 0 END) AS total_clicks_yesterday,
    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_30d,
    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_7d,
    SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END) AS total_sales14d_yesterday,
    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_30d,
    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_7d,
    SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) AS total_cost_yesterday,
    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_30d,
    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_7d,
    SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) / SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END)  AS ACOS_yesterday

FROM
amazon_search_term_reports_sp
WHERE
(date between DATE_SUB('{}', INTERVAL 30 DAY) and ('{}'-INTERVAL 1 DAY))
and market='{}'
and keywordId in (select keywordId from amazon_targeting_reports_sp where campaignStatus='ENABLED' and date='{}'-INTERVAL 1 DAY)
and  ( campaignName not LIKE '%AUTO%' and  campaignName not  LIKE '%auto%' and campaignName not LIKE '%Auto%' and  campaignName not LIKE '%自动%' )
and campaignId not in (SELECT
DISTINCT entityId
FROM
amazon_advertising_change_history
WHERE
timestamp >= (UNIX_TIMESTAMP(NOW(3)) - 4 * 24 * 60 * 60) * 1000
and market = '{}'
and predefinedTarget <> '')
GROUP BY
  adGroupName,
  campaignName,
  keyword,
  searchTerm,
        matchType
ORDER BY
  adGroupName,
  campaignName,
  keyword,
  searchTerm;
                """.format(cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,country,cur_time,country)
            elif version == 1.1:
                query = """
                SELECT keyword,searchTerm,adGroupName,adGroupId,matchType,
                    campaignName,
                    campaignId,
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_30d,
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_7d,
                    SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN clicks ELSE 0 END) AS total_clicks_yesterday,
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_30d,
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_7d,
                    SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END) AS total_sales14d_yesterday,
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_30d,
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_7d,
                    SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) AS total_cost_yesterday,
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_30d,
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_7d,
                    SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) / SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END)  AS ACOS_yesterday
                FROM
                amazon_search_term_reports_sp
                WHERE
                (date between DATE_SUB('{}', INTERVAL 30 DAY) and ('{}'-INTERVAL 1 DAY))
                and market='{}'
                and keywordId in (select keywordId from amazon_targeting_reports_sp where campaignStatus='ENABLED' and date='{}')
                and  ( campaignName not LIKE '%AUTO%' and  campaignName not  LIKE '%auto%' and campaignName not LIKE '%Auto%' and  campaignName not LIKE '%自动%' )
                and campaignId not in (SELECT
                DISTINCT entityId
                FROM
                amazon_advertising_change_history
                WHERE
                timestamp >= (UNIX_TIMESTAMP(NOW(3)) - 4 * 24 * 60 * 60) * 1000
                and market = '{}'
                and predefinedTarget <> '')
                GROUP BY
                  adGroupName,
                  campaignName,
                  keyword,
                  searchTerm,
                        matchType
                ORDER BY
                  adGroupName,
                  campaignName,
                  keyword,
                  searchTerm;
                                """.format(cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                           cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                           cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                           cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, country,
                                           cur_time, country)
            elif version == 1.2:
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
                and a.keywordId in (select keywordId from amazon_targeting_reports_sp where campaignStatus='ENABLED' and date='{}'-INTERVAL 1 DAY)
    AND c.targetingType like '%MAN%' -- 筛选出手动广告
GROUP BY
  a.adGroupName,
  a.campaignName,
  a.keyword,
  a.searchTerm,
        a.matchType
ORDER BY
  a.adGroupName,
  a.campaignName,
  a.keyword,
  a.searchTerm;
                                """.format(cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                           cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                           cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                           cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, country,
                                           cur_time)
            elif version == 1.3:
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
                and a.keywordId in (select keywordId from amazon_targeting_reports_sp where campaignStatus='ENABLED' and date='{}'-INTERVAL 1 DAY)
                AND c.targetingType like '%MAN%' -- 筛选出手动广告
                AND (a.campaignName not LIKE '%%ASIN%%' and a.campaignName not LIKE '%%asin%%' and a.campaignName not LIKE '%%商品投放%%' and a.campaignName not LIKE '%%品类投放%%' and a.campaignName not LIKE '%%CATEGORY%%' and a.campaignName not LIKE '%%PRODUCT%%')
            GROUP BY
              a.adGroupName,
              a.campaignName,
              a.keyword,
              a.searchTerm,
              a.matchType
            ORDER BY
              a.adGroupName,
              a.campaignName,
              a.keyword,
              a.searchTerm;
                                            """.format(cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                       cur_time, cur_time,
                                                       cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                       cur_time, cur_time, cur_time,
                                                       cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                       cur_time, cur_time, cur_time,
                                                       cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                       country,
                                                       cur_time)
            elif version == 1.4:
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
                and a.keywordId in (select keywordId from amazon_search_term_reports_sp where campaignStatus='ENABLED' and date='{}'-INTERVAL 1 DAY AND campaignName NOT LIKE '%_overstock%')
                AND c.targetingType like '%MAN%' -- 筛选出手动广告
                AND (a.campaignName not LIKE '%%ASIN%%' and a.campaignName not LIKE '%%asin%%' and a.campaignName not LIKE '%%商品投放%%' and a.campaignName not LIKE '%%品类投放%%' and a.campaignName not LIKE '%%CATEGORY%%' and a.campaignName not LIKE '%%PRODUCT%%')
                AND a.campaignId NOT IN (
		SELECT DISTINCT campaignId
		FROM amazon_targeting_reports_sp
		WHERE matchType = 'TARGETING_EXPRESSION'
		)
                AND NOT EXISTS (
            SELECT 1
            FROM amazon_targeting_reports_sp
            WHERE keyword = a.searchTerm
              AND campaignId = a.campaignId
              AND adGroupId = a.adGroupId
        )
            GROUP BY
              a.adGroupName,
              a.campaignName,
              a.keyword,
              a.searchTerm,
              a.matchType
            ORDER BY
              a.adGroupName,
              a.campaignName,
              a.keyword,
              a.searchTerm;
                                            """.format(cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                       cur_time, cur_time,
                                                       cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                       cur_time, cur_time, cur_time,
                                                       cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                       cur_time, cur_time, cur_time,
                                                       cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                       country,
                                                       cur_time)
            else:
                query = None
            df1 = pd.read_sql(query, con=conn)
            output_filename = '.\日常优化\手动sp广告\搜索词优化\预处理.csv'
            df1.to_csv(output_filename, index=False, encoding='utf-8-sig')
            csv_to_json(output_filename)
            # return df
            return print("查询已完成，请查看文件： " + output_filename)

        except Exception as error:
            print("Error while inserting data:", error)

    def preprocessing_spkeyword(self, country, cur_time, version=1.2):

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

    def preprocessing_budget(self, country, cur_time, version=1.2):

        """"""
        try:
            conn = self.conn

            if version == 1.0:
                query = """
WITH Campaign_Stats AS (
    SELECT
        campaignName,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS cost_7d,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS sales14d_7d,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS cost_1m,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS sales14d_1m,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales7d ELSE 0 END) AS sales_1m,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS clicks_7d,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS clicks_1m
    FROM
        amazon_campaign_reports_sp
    WHERE
        date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND ('{}'-INTERVAL 1 DAY)
        AND campaignStatus = 'ENABLED'
        AND ( campaignName not LIKE '%AUTO%' and  campaignName not  LIKE '%auto%' and campaignName not LIKE '%Auto%' and  campaignName not LIKE '%自动%' )
        AND market = '{}'
    GROUP BY
        campaignName
)
SELECT
    a.campaignName,
    a.campaignId,
    a.date,
    a.campaignBudgetAmount AS Budget,
    a.clicks,
    a.cost,
    a.sales7d as sales,
    (a.cost / NULLIF(a.sales14d, 0)) AS ACOS,
    cs.sales_1m,
    cs.cost_1m,
    (cs.cost_7d / NULLIF(cs.sales14d_7d, 0)) AS avg_ACOS_7d,
    cs.cost_1m / NULLIF(cs.sales14d_1m, 0) AS avg_ACOS_1m,
    cs.clicks_1m,
    cs.clicks_7d,
    (SELECT SUM(cost) / SUM(sales14d) FROM amazon_campaign_reports_sp WHERE market = '{}' AND date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND ('{}'-INTERVAL 1 DAY) AND ( campaignName not LIKE '%AUTO%' and  campaignName not  LIKE '%auto%' and campaignName not LIKE '%Auto%' and  campaignName not LIKE '%自动%' )) AS country_avg_ACOS_1m
FROM
    amazon_campaign_reports_sp a
LEFT JOIN Campaign_Stats cs ON a.campaignName = cs.campaignName
WHERE
    a.date = ('{}'-INTERVAL 1 DAY)
    AND a.campaignStatus = 'ENABLED'
    AND a.campaignName NOT LIKE '%_overstock%'
    AND ( a.campaignName not LIKE '%AUTO%' and  a.campaignName not  LIKE '%auto%' and a.campaignName not LIKE '%Auto%' and  a.campaignName not LIKE '%自动%' )
    AND a.market = '{}'
ORDER BY
    a.date;

                """.format(cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,country,country,cur_time,cur_time,cur_time,country)
            elif version == 1.1:
                query = """
                WITH Campaign_Stats AS (
                    SELECT
                        campaignId,
                        campaignName,
                        campaignBudgetAmount AS Budget,
                        market,
                        sum(CASE WHEN date = DATE_SUB('{}', INTERVAL 2 DAY) THEN cost ELSE 0 END) as cost_yesterday,
                        sum(CASE WHEN date = DATE_SUB('{}', INTERVAL 2 DAY) THEN clicks ELSE 0 END) as clicks_yesterday,
                        sum(CASE WHEN date = DATE_SUB('{}', INTERVAL 2 DAY) THEN sales14d ELSE 0 END) as sales_yesterday,
                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_7d,
                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_7d,
                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_30d,
                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_30d,
                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_30d,
                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_7d,
                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY)  THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY)  THEN sales14d ELSE 0 END) AS ACOS_30d,
                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY)  THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY)  THEN sales14d ELSE 0 END) AS ACOS_7d,
                        SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) / SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END)  AS ACOS_yesterday
                    FROM
                        amazon_campaign_reports_sp
                    WHERE
                        date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND ('{}'-INTERVAL 1 DAY)
                        AND campaignId IN (
        SELECT campaignId
        FROM amazon_campaign_reports_sp
        WHERE campaignStatus = 'ENABLED' AND date = '{}' - INTERVAL 1 DAY )
                        AND ( campaignName not LIKE '%AUTO%' and  campaignName not  LIKE '%auto%' and campaignName not LIKE '%Auto%' and  campaignName not LIKE '%自动%' )
                        AND market = '{}'
                    GROUP BY
                        campaignName
                ),
      b as (select sum(cost)/sum(sales14d) as country_avg_ACOS_1m,market
from amazon_campaign_reports_sp
    WHERE
        date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND ('{}'-INTERVAL 1 DAY)
        AND campaignId IN (
        SELECT campaignId
        FROM amazon_campaign_reports_sp
        WHERE campaignStatus = 'ENABLED' AND date = '{}' - INTERVAL 1 DAY )
        AND ( campaignName not LIKE '%AUTO%' and  campaignName not  LIKE '%auto%' and campaignName not LIKE '%Auto%' and  campaignName not LIKE '%自动%' )
        AND market = '{}'
                                )
  SELECT Campaign_Stats.*,b. country_avg_ACOS_1m
        from Campaign_Stats join b
        on Campaign_Stats.market =b.market
                                """.format(cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                           cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                           cur_time, cur_time, cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time, country, cur_time, cur_time, cur_time, country)
            elif version == 1.2:
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
                        AND campaignName NOT LIKE '%_overstock%')
                    AND acl.targetingType LIKE '%MAN%'  -- 这里筛选手动广告
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
                 WHERE campaignStatus = 'ENABLED' AND date = '{}' - INTERVAL 1 DAY)

                AND campaigns.targetingType LIKE '%MAN%'  -- 筛选手动广告
                AND reports.market = '{}'
            GROUP BY
                reports.market)


              SELECT Campaign_Stats.*,b. country_avg_ACOS_1m
                    from Campaign_Stats join b
                    on Campaign_Stats.market =b.market
                                        """.format(cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                   cur_time, cur_time,
                                                   cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                   cur_time,
                                                   cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                   cur_time,
                                                   cur_time, cur_time, cur_time, country, cur_time, cur_time, cur_time,
                                                   country)
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

    def preprocessing_product_targets(self, country, cur_time, version=1.0):

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
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_3d,
        -- 昨天的总销售额
        SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END) AS total_sales14d_yesterday,
        -- 过去30天的总成本
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_30d,
        -- 过去7天的总成本
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_7d,
        -- 过去4天的总成本
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_3d,
        -- 昨天的总成本
        SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) AS total_cost_yesterday,
        -- 过去30天的平均成本销售比（ACOS）
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) /
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_30d,
        -- 过去7天的平均成本销售比（ACOS）
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) /
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_7d,
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
            AND campaignName NOT LIKE '%_overstock%'
        )
        AND c.targetingType LIKE '%MAN%' -- 筛选出手动广告
        -- 排除最近4天内有变更的keywordId
        AND b.keywordId NOT IN (SELECT DISTINCT entityId
            FROM amazon_advertising_change_history
            WHERE timestamp >= (UNIX_TIMESTAMP(NOW(3)) - 4 * 24 * 60 * 60) * 1000
            AND entityType = 'KEYWORD'
            AND market = '{}')
        AND b.campaignId NOT IN (SELECT DISTINCT campaignId FROM amazon_targeting_reports_sd) -- 排除在amazon_targeting_reports_sd中的campaignId
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
    AND (a.keyword LIKE '%asin%' OR a.targeting LIKE '%asin%' OR a.keyword LIKE '%category%' OR a.targeting LIKE '%category%' OR a.campaignName LIKE '%ASIN%');
                """.format(cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                           cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                           cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                           cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, country, cur_time, country, cur_time)
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

    def preprocessing_sp_product_targets(self, country, cur_time, version=1.0):

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

    def preprocessing_product_targets_search_term(self, country, cur_time, version=1.1):

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
        AND campaignName NOT LIKE '%_overstock%'
    )
    AND c.targetingType LIKE '%MAN%' -- 筛选出手动广告
    AND (
        a.keyword LIKE '%asin%'
        OR a.targeting LIKE '%asin%'
        OR a.campaignName LIKE '%ASIN%'
        OR a.keyword LIKE '%category%'
        OR a.targeting LIKE '%category%'
    ) -- 筛选出keyword、targeting中包含asin或category，或campaignName中包含ASIN的数据行
    AND a.campaignId NOT IN (
        SELECT DISTINCT campaignId
        FROM amazon_targeting_reports_sd
    ) -- 排除在 amazon_targeting_reports_sd 中的 campaignId
GROUP BY
    a.adGroupName,
    a.campaignName,
    a.keyword,
    a.searchTerm,
    a.matchType
ORDER BY
    a.adGroupName,
    a.campaignName,
    a.keyword,
    a.searchTerm;
                """.format(cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                           cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                           cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                           cur_time, country, cur_time)
            if version == 1.1:
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
            a.campaignName,
            a.keyword,
            a.searchTerm,
            a.matchType
        ORDER BY
            a.adGroupName,
            a.campaignName,
            a.keyword,
            a.searchTerm;
                        """.format(cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                   cur_time,
                                   cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                   cur_time, cur_time, cur_time,
                                   cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                   cur_time, cur_time,
                                   cur_time, country, cur_time)
            else:
                query = None
            # print(query)
            df1 = pd.read_sql(query, con=conn)
            output_filename = '.\日常优化\手动sp广告\商品投放搜索词优化\预处理.csv'
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

            if version == 1.0:
                query = """
SELECT
    adGroupName,
    campaignName,
    advertisedSku,
    -- 过去30天（包含今天）的总点击量
    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_30d,
    -- 过去7天（包含今天）的总点击量
    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_7d,
    -- 昨天的总点击量
    SUM(CASE WHEN date = DATE_SUB('{}', INTERVAL 1 DAY) - INTERVAL 1 DAY THEN clicks ELSE 0 END) AS total_clicks_yesterday,
    -- 过去30天的总销售额
    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_30d,
    -- 过去7天的总销售额
    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_7d,
    -- 昨天的总销售额
    SUM(CASE WHEN date = DATE_SUB('{}', INTERVAL 1 DAY) - INTERVAL 1 DAY THEN sales14d ELSE 0 END) AS total_sales14d_yesterday,
    -- 过去30天的总成本
    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_30d,
    -- 过去7天的总成本
    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_7d,
    -- 昨天的总成本
    SUM(CASE WHEN date = DATE_SUB('{}', INTERVAL 1 DAY) - INTERVAL 1 DAY THEN cost ELSE 0 END) AS total_cost_yesterday,
    -- 过去30天的平均成本销售比（ACOS）
    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / NULLIF(SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END), 0) AS ACOS_30d,
    -- 过去7天的平均成本销售比（ACOS）
    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / NULLIF(SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END), 0) AS ACOS_7d,
    -- 昨天的平均成本销售比（ACOS）
    SUM(CASE WHEN date = DATE_SUB('{}', INTERVAL 1 DAY) - INTERVAL 1 DAY THEN cost ELSE 0 END) / NULLIF(SUM(CASE WHEN date = DATE_SUB('{}', INTERVAL 1 DAY) - INTERVAL 1 DAY THEN sales14d ELSE 0 END), 0) AS ACOS_yesterday
FROM
    amazon_advertised_product_reports_sp
WHERE
    date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND ('{}'-INTERVAL 1 DAY)
    AND market = '{}'
    AND adId IN (
        SELECT adId
        FROM amazon_advertised_product_reports_sp
        WHERE campaignStatus = 'ENABLED' AND date = DATE_SUB('{}', INTERVAL 1 DAY
        AND campaignName NOT LIKE '%_overstock%')
    )
    AND (campaignName LIKE '%AUTO%' OR campaignName LIKE '%auto%' OR campaignName LIKE '%Auto%' OR campaignName LIKE '%自动%')
GROUP BY
    adGroupName,
    campaignName,
    advertisedSku
ORDER BY
    adGroupName,
    campaignName,
    advertisedSku;
                """.format(cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,country,cur_time)
            elif version == 1.1:
                query = """
                SELECT
                    adGroupName,
                    adId,
                    campaignName,
                    advertisedSku,
                    -- 过去30天（包含今天）的总点击量
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_30d,
                    -- 过去7天（包含今天）的总点击量
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_7d,
                    -- 昨天的总点击量
                    SUM(CASE WHEN date = DATE_SUB('{}', INTERVAL 1 DAY) - INTERVAL 1 DAY THEN clicks ELSE 0 END) AS total_clicks_yesterday,
                    -- 过去30天的总销售额
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_30d,
                    -- 过去7天的总销售额
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_7d,
                    -- 昨天的总销售额
                    SUM(CASE WHEN date = DATE_SUB('{}', INTERVAL 1 DAY) - INTERVAL 1 DAY THEN sales14d ELSE 0 END) AS total_sales14d_yesterday,
                    -- 过去30天的总成本
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_30d,
                    -- 过去7天的总成本
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_7d,
                    -- 昨天的总成本
                    SUM(CASE WHEN date = DATE_SUB('{}', INTERVAL 1 DAY) - INTERVAL 1 DAY THEN cost ELSE 0 END) AS total_cost_yesterday,
                    -- 过去30天的平均成本销售比（ACOS）
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / NULLIF(SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END), 0) AS ACOS_30d,
                    -- 过去7天的平均成本销售比（ACOS）
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / NULLIF(SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END), 0) AS ACOS_7d,
                    -- 昨天的平均成本销售比（ACOS）
                    SUM(CASE WHEN date = DATE_SUB('{}', INTERVAL 1 DAY) - INTERVAL 1 DAY THEN cost ELSE 0 END) / NULLIF(SUM(CASE WHEN date = DATE_SUB('{}', INTERVAL 1 DAY) - INTERVAL 1 DAY THEN sales14d ELSE 0 END), 0) AS ACOS_yesterday
                FROM
                    amazon_advertised_product_reports_sp
                WHERE
                    date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND ('{}'-INTERVAL 1 DAY)
                    AND market = '{}'
                    AND adId IN (
                        SELECT adId
                        FROM amazon_advertised_product_reports_sp
                        WHERE campaignStatus = 'ENABLED' AND date = DATE_SUB('{}', INTERVAL 1 DAY)
                    )
                    AND (campaignName LIKE '%AUTO%' OR campaignName LIKE '%auto%' OR campaignName LIKE '%Auto%' OR campaignName LIKE '%自动%')
                GROUP BY
                    adGroupName,
                    campaignName,
                    advertisedSku
                ORDER BY
                    adGroupName,
                    campaignName,
                    advertisedSku;
                                """.format(cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                           cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                           cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                           cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, country,
                                           cur_time)
            elif version == 1.2:
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
    )
    AND c.targetingType like '%AUT%'
GROUP BY
    adGroupName,
    a.adId,
    campaignName,
    advertisedSku

ORDER BY
    adGroupName,
    campaignName,
    advertisedSku;
                                """.format(cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                           cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                           cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                           cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, country,
                                           cur_time)
            elif version == 1.3:
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
                    AND campaignName NOT LIKE '%_overstock%'
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
                                            """.format(cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                       cur_time, cur_time, cur_time, cur_time,
                                                       cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                       cur_time, cur_time, cur_time, cur_time,
                                                       cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                       cur_time, cur_time, cur_time, cur_time,
                                                       cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                       country,
                                                       cur_time)
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
                    AND campaignName NOT LIKE '%_overstock%'
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
                                            """.format(cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                       cur_time, cur_time, cur_time, cur_time,
                                                       cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                       cur_time, cur_time, cur_time, cur_time,
                                                       cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                       cur_time, cur_time, cur_time, cur_time,
                                                       cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                       country,
                                                       cur_time)
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

    def preprocessing_targeting_group_auto(self, country, cur_time, version=1.2):

        """"""
        try:
            conn = self.conn

            if version == 1.0:
                query = """
        SELECT placementClassification,
    campaignName,
    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_3d,
    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_7d,
    SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN clicks ELSE 0 END) AS total_clicks_yesterday,
                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_3d,
                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_7d,
                SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END) AS total_sales14d_yesterday,
                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_3d,
                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_7d,
                SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) AS total_cost_yesterday,
                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_3d,
               SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_7d,
     SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) / SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END)  AS ACOS_yesterday

FROM
amazon_campaign_placement_reports_sp
  WHERE
    DATE BETWEEN DATE_SUB('{}', INTERVAL 30 DAY)
    AND ('{}'-INTERVAL 1 DAY)
    AND market = '{}'
    AND  campaignId in (select campaignId from amazon_targeting_reports_sp where campaignStatus='ENABLED' and date=DATE_SUB('{}', INTERVAL 1 DAY))
    AND ( campaignName LIKE '%AUTO%' OR campaignName LIKE '%auto%' OR campaignName LIKE '%Auto%' OR campaignName LIKE '%自动%' )

GROUP BY
  campaignName,
  placementClassification
ORDER BY
  campaignName,
  placementClassification;
                """.format(cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,country,cur_time)
            elif version == 1.1:
                query = """
                SELECT
                    placementClassification,
                    campaignName,
                    campaignId,
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_3d,
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_7d,
                    SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN clicks ELSE 0 END) AS total_clicks_yesterday,
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_3d,
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_7d,
                    SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END) AS total_sales14d_yesterday,
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_3d,
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_7d,
                    SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) AS total_cost_yesterday,
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 2 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_3d,
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_7d,
                    SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) / SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END)  AS ACOS_yesterday

                FROM
                amazon_campaign_placement_reports_sp
                WHERE
                    DATE BETWEEN DATE_SUB('{}', INTERVAL 30 DAY)
                    AND ('{}'-INTERVAL 1 DAY)
                    AND market = '{}'
                    AND  campaignId in (select campaignId from amazon_targeting_reports_sp where campaignStatus='ENABLED' and date=DATE_SUB('{}', INTERVAL 1 DAY))
                    AND ( campaignName LIKE '%AUTO%' OR campaignName LIKE '%auto%' OR campaignName LIKE '%Auto%' OR campaignName LIKE '%自动%' )

                GROUP BY
                  campaignName,
                  placementClassification
                ORDER BY
                  campaignName,
                  placementClassification;
                                """.format(cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                           cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                           cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                           cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, country,
                                           cur_time)
            elif version == 1.2:
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
        AND campaignName NOT LIKE '%_overstock%'
    )
    AND c.targetingType LIKE '%AUT%'
GROUP BY
    a.campaignName,
    a.campaignId,
    a.placementClassification,
    c.dynamicBidding_placementTop_percentage,
    c.dynamicBidding_placementProductPage_percentage,
    c.dynamicBidding_placementRestOfSearch_percentage
ORDER BY
    a.campaignName,
    a.placementClassification;
                                """.format(cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                           cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                           cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                           cur_time, cur_time, cur_time, cur_time, country,
                                           cur_time)
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

    def preprocessing_search_term_auto(self, country, cur_time, version=1.2):

        """"""
        try:
            conn = self.conn

            if version == 1.0:
                query = """
SELECT keyword,searchTerm,adGroupName,
    campaignName,
    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_30d,
    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_7d,
    SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN clicks ELSE 0 END) AS total_clicks_yesterday,
                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_30d,
                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_7d,
                SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END) AS total_sales14d_yesterday,
                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_30d,
                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_7d,
                SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) AS total_cost_yesterday,
                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_30d,
               SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_7d,
     SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) / SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END)  AS ACOS_yesterday

FROM
amazon_search_term_reports_sp
WHERE
(date between DATE_SUB('{}', INTERVAL 30 DAY) and ('{}'-INTERVAL 1 DAY))
and market='{}'
and keywordId in (select keywordId from amazon_targeting_reports_sp where campaignStatus='ENABLED' and date='{}')
and ( campaignName LIKE '%AUTO%' OR campaignName LIKE '%auto%' OR campaignName LIKE '%Auto%' OR campaignName LIKE '%自动%' )
and campaignId not in (SELECT
DISTINCT entityId
FROM
amazon_advertising_change_history
WHERE
timestamp >= (UNIX_TIMESTAMP(NOW(3)) - 4 * 24 * 60 * 60) * 1000
and market = '{}'
and predefinedTarget <> '')
GROUP BY
  adGroupName,
  campaignName,
  keyword,
  searchTerm
ORDER BY
  adGroupName,
  campaignName,
  keyword,
  searchTerm;
                """.format(cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,country,cur_time,country)
            elif version == 1.1:
                query = """
                SELECT
                    keyword,
                    searchTerm,
                    adGroupName,
                    adGroupId,
                    campaignName,
                    campaignId,
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_30d,
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_7d,
                    SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN clicks ELSE 0 END) AS total_clicks_yesterday,
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_30d,
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_7d,
                    SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END) AS total_sales14d_yesterday,
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_30d,
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_7d,
                    SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) AS total_cost_yesterday,
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_30d,
                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_7d,
                    SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) / SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END)  AS ACOS_yesterday
                FROM
                amazon_search_term_reports_sp
                WHERE
                (date between DATE_SUB('{}', INTERVAL 30 DAY) and ('{}'-INTERVAL 1 DAY))
                and market='{}'
                and keywordId in (select keywordId from amazon_targeting_reports_sp where campaignStatus='ENABLED' and date='{}')
                and ( campaignName LIKE '%AUTO%' OR campaignName LIKE '%auto%' OR campaignName LIKE '%Auto%' OR campaignName LIKE '%自动%' )
                and campaignId not in (SELECT
                DISTINCT entityId
                FROM
                amazon_advertising_change_history
                WHERE
                timestamp >= (UNIX_TIMESTAMP(NOW(3)) - 4 * 24 * 60 * 60) * 1000
                and market = '{}'
                and predefinedTarget <> '')
                GROUP BY
                  adGroupName,
                  campaignName,
                  keyword,
                  searchTerm
                ORDER BY
                  adGroupName,
                  campaignName,
                  keyword,
                  searchTerm;
                                """.format(cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                           cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                           cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                           cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, country,
                                           cur_time, country)
            elif version == 1.2:
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
    amazon_campaigns_list_sp c ON a.campaignId = c.campaignId
WHERE
    a.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND '{}'- INTERVAL 1 DAY
    AND a.market = '{}'
    and a.keywordId in (select keywordId from amazon_targeting_reports_sp where campaignStatus='ENABLED' and date='{}'-INTERVAL 1 DAY AND campaignName NOT LIKE '%_overstock%')
    AND c.targetingType like '%AUT%'
    AND NOT (a.searchTerm LIKE 'b0%' AND LENGTH(a.searchTerm) = 10)
GROUP BY
  a.adGroupName,
  a.campaignName,
  a.keyword,
  a.searchTerm,
  a.matchType
ORDER BY
  a.adGroupName,
  a.campaignName,
  a.keyword,
  a.searchTerm;
                                """.format(cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                           cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                           cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                           cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, country,
                                           cur_time)
            else:
                query = None
            df1 = pd.read_sql(query, con=conn)
            output_filename = '.\日常优化\自动sp广告\搜索词优化\预处理.csv'
            df1.to_csv(output_filename, index=False, encoding='utf-8-sig')
            csv_to_json(output_filename)
            # return df
            return print("查询已完成，请查看文件： " + output_filename)

        except Exception as error:
            print("Error while inserting data:", error)

    def preprocessing_automatic_targeting_auto(self, country, cur_time, version=1.2):

        """"""
        try:
            conn = self.conn

            if version == 1.0:
                query = """
SELECT keyword,keywordBid,adGroupName,
    campaignName,
    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_30d,
    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_7d,
    SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN clicks ELSE 0 END) AS total_clicks_yesterday,
                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_30d,
                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_7d,
                SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END) AS total_sales14d_yesterday,
                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_30d,
                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_7d,
                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 3 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_4d,
                SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) AS total_cost_yesterday,
                SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_30d,
               SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_7d,
     SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) / SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END)  AS ACOS_yesterday

FROM
 amazon_targeting_reports_sp
  WHERE
    DATE BETWEEN DATE_SUB('{}', INTERVAL 30 DAY)
    AND ('{}'-INTERVAL 1 DAY)
    AND market = '{}'
    AND  keywordId in (select keywordId from amazon_targeting_reports_sp where campaignStatus='ENABLED' and date='{}')
    AND ( campaignName LIKE '%AUTO%' OR campaignName LIKE '%auto%' OR campaignName LIKE '%Auto%' OR campaignName LIKE '%自动%' )
    and campaignId not in ( SELECT DISTINCT entityId
        FROM amazon_advertising_change_history
        WHERE timestamp >= (UNIX_TIMESTAMP(NOW(3)) - 4 * 24 * 60 * 60) * 1000
        AND market = '{}'
        AND predefinedTarget <> '')

GROUP BY
  adGroupName,
  campaignName,
  keyword
ORDER BY
  adGroupName,
  campaignName,
  keyword;
                """.format(cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,country,cur_time,country)
            elif version == 1.1:
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
        SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN clicks ELSE 0 END) AS total_clicks_yesterday,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_30d,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_7d,
        SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END) AS total_sales14d_yesterday,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_30d,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_7d,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 3 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_4d,
        SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) AS total_cost_yesterday,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_30d,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_7d,
        SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) / SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END)  AS ACOS_yesterday
    FROM
        amazon_targeting_reports_sp
    WHERE
        date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) and DATE_SUB('{}', INTERVAL 1 DAY)
        -- ... 其他 WHERE 条件
        AND market = '{}'
    -- 确保keywordId是来自特定日期的启用状态的campaign
    AND keywordId IN (SELECT keywordId FROM amazon_targeting_reports_sp WHERE campaignStatus = 'ENABLED' AND date = '{}'- INTERVAL 1 DAY)
    -- 确保campaignName包含特定文本
    AND ( campaignName LIKE '%AUTO%' OR campaignName LIKE '%auto%' OR campaignName LIKE '%Auto%' OR campaignName LIKE '%自动%' )
    -- 排除最近4天内有变更的keywordId
    AND  keywordId not in ( SELECT DISTINCT entityId
        FROM amazon_advertising_change_history
        WHERE timestamp >= (UNIX_TIMESTAMP(NOW(3)) - 4 * 24 * 60 * 60) * 1000
        AND market = '{}'
        AND predefinedTarget <> '')
    GROUP BY
        adGroupName,
        campaignName,
        keyword,
        keywordId
    ORDER BY
        adGroupName,
        campaignName,
        keyword,
        keywordId
        )
SELECT
    a.*,
    b.keywordBid
FROM
    a
LEFT JOIN
    amazon_targeting_reports_sp b ON a.keywordId = b.keywordId
WHERE
    b.date = DATE_SUB('{}', INTERVAL 1 DAY)
                                """.format(cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, country, cur_time, country, cur_time)
            elif version == 1.2:
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
        FROM amazon_advertised_product_reports_sp
        WHERE campaignStatus = 'ENABLED' AND date = '{}' - INTERVAL 1 DAY
        AND campaignName NOT LIKE '%_overstock%'
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
                                            """.format(cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                       cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                       cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                       cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                       cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                       cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                       cur_time, country, cur_time, country, cur_time)
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

    def preprocessing_sp_automatic_targeting_auto(self, country, cur_time, version=1.1):

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

    def preprocessing_budget_auto(self, country, cur_time, version=1.2):

        """"""
        try:
            conn = self.conn

            if version == 1.0:
                query = """
WITH Campaign_Stats AS (
    SELECT
        campaignName,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS cost_7d,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS sales14d_7d,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS cost_1m,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS sales14d_1m,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales7d ELSE 0 END) AS sales_1m,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS clicks_7d,
        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS clicks_1m
    FROM
        amazon_campaign_reports_sp
    WHERE
        date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND ('{}'-INTERVAL 1 DAY)
        AND campaignStatus = 'ENABLED'
        AND ( campaignName  LIKE '%AUTO%' or  campaignName   LIKE '%auto%' or campaignName  LIKE '%Auto%' or  campaignName  LIKE '%自动%' )
        AND market = '{}'
    GROUP BY
        campaignName
)
SELECT
    a.campaignName,
    a.campaignId,
    a.date,
    a.campaignBudgetAmount AS Budget,
    a.clicks,
    a.cost,
    a.sales7d as sales,
    (a.cost / NULLIF(a.sales14d, 0)) AS ACOS,
    cs.sales_1m,
    cs.cost_1m,
    (cs.cost_7d / NULLIF(cs.sales14d_7d, 0)) AS avg_ACOS_7d,
    cs.cost_1m / NULLIF(cs.sales14d_1m, 0) AS avg_ACOS_1m,
    cs.clicks_1m,
    cs.clicks_7d,
    (SELECT SUM(cost) / SUM(sales14d) FROM amazon_campaign_reports_sp WHERE market = '{}' AND date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND ('{}'-INTERVAL 1 DAY) AND ( campaignName  LIKE '%AUTO%' or  campaignName   LIKE '%auto%' or campaignName  LIKE '%Auto%' or  campaignName  LIKE '%自动%' )) AS country_avg_ACOS_1m
FROM
    amazon_campaign_reports_sp a
LEFT JOIN Campaign_Stats cs ON a.campaignName = cs.campaignName
WHERE
    a.date = ('{}'-INTERVAL 1 DAY)
    AND a.campaignStatus = 'ENABLED'
    AND ( a.campaignName  LIKE '%AUTO%' or  a.campaignName   LIKE '%auto%' or a.campaignName  LIKE '%Auto%' or  a.campaignName  LIKE '%自动%' )
    AND a.market = '{}'
ORDER BY
    a.date;

                """.format(cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,country,country,cur_time,cur_time,cur_time,country)
            elif version == 1.1:
                query = """
               WITH Campaign_Stats AS (
        SELECT
            campaignId,
            campaignName,
            campaignBudgetAmount AS Budget,
            market,
            sum(CASE WHEN date = DATE_SUB('{}', INTERVAL 2 DAY) THEN cost ELSE 0 END) as cost_yesterday,
            sum(CASE WHEN date = DATE_SUB('{}', INTERVAL 2 DAY) THEN clicks ELSE 0 END) as clicks_yesterday,
            sum(CASE WHEN date = DATE_SUB('{}', INTERVAL 2 DAY) THEN sales14d ELSE 0 END) as sales_yesterday,
            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_7d,
            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_7d,
            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_30d,
            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_30d,
            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_30d,
            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN clicks ELSE 0 END) AS total_clicks_7d,
            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY)  THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY)  THEN sales14d ELSE 0 END) AS ACOS_30d,
            SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY)  THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY)  THEN sales14d ELSE 0 END) AS ACOS_7d,
            SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) / SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END)  AS ACOS_yesterday
        FROM
        amazon_campaign_reports_sp
        WHERE
        date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND ('{}'-INTERVAL 1 DAY)
        AND campaignId IN (
        SELECT campaignId
        FROM amazon_campaign_reports_sp
        WHERE campaignStatus = 'ENABLED' AND date = '{}' - INTERVAL 1 DAY )
        AND  ( campaignName LIKE '%AUTO%' or  campaignName  LIKE '%auto%' or campaignName LIKE '%Auto%' or  campaignName LIKE '%自动%' )
        AND market = '{}'
        GROUP BY
        campaignName
),
b as (
        select sum(cost)/sum(sales14d) as country_avg_ACOS_1m,market
        from amazon_campaign_reports_sp
        WHERE
        date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND ('{}'-INTERVAL 1 DAY)
        AND campaignId IN (
        SELECT campaignId
        FROM amazon_campaign_reports_sp
        WHERE campaignStatus = 'ENABLED' AND date = '{}' - INTERVAL 1 DAY )
        AND  ( campaignName LIKE '%AUTO%' or  campaignName  LIKE '%auto%' or campaignName LIKE '%Auto%' or  campaignName LIKE '%自动%' )
        AND market = '{}'
                                )
    SELECT Campaign_Stats.*,b. country_avg_ACOS_1m
    from Campaign_Stats join b
        on Campaign_Stats.market =b.market

                                """.format(cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                           cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                           cur_time, cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time, country, cur_time, cur_time, cur_time, country)
            elif version == 1.2:
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
            AND campaignName NOT LIKE '%_overstock%')
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
                                            """.format(cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                       cur_time, cur_time, cur_time, cur_time,
                                                       cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                       cur_time,
                                                       cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                       cur_time, cur_time, cur_time, cur_time, cur_time, country,
                                                       cur_time, cur_time, cur_time, country)
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

    def preprocessing_campaign_anomaly_detection(self, country, cur_time):

        """"""
        try:
            conn = self.conn

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
            df1 = pd.read_sql(query, con=conn)

            output_filename = '.\日常优化\异常定位检测\广告活动\预处理.csv'
            df1.to_csv(output_filename, index=False, encoding='utf-8-sig')
            csv_to_json(output_filename)
            # return df
            return print("查询已完成，请查看文件： " + output_filename)

        except Exception as error:
            print("Error while inserting data:", error)

    def preprocessing_targeting_group_anomaly_detection(self, country, cur_time):

        """"""
        try:
            conn = self.conn


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
            df1 = pd.read_sql(query, con=conn)

            output_filename = '.\日常优化\异常定位检测\广告位\预处理.csv'
            df1.to_csv(output_filename, index=False, encoding='utf-8-sig')
            csv_to_json(output_filename)
            # return df
            return print("查询已完成，请查看文件： " + output_filename)

        except Exception as error:
            print("Error while inserting data:", error)

    def preprocessing_sku_anomaly_detection(self, country, cur_time,campaign_ids):

        """"""
        try:
            conn = self.conn


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
            df1 = pd.read_sql(query, con=conn)

            output_filename = '.\日常优化\异常定位检测\商品\预处理1.csv'
            df1.to_csv(output_filename, index=False, encoding='utf-8-sig')
            csv_to_json(output_filename)
            # return df
            return print("查询已完成，请查看文件： " + output_filename)

        except Exception as error:
            print("Error while inserting data:", error)

    def preprocessing_targeting_anomaly_detection(self, country, cur_time,campaign_ids):

        """"""
        try:
            conn = self.conn


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
            df1 = pd.read_sql(query, con=conn)

            output_filename = '.\日常优化\异常定位检测\投放词\预处理1.csv'
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
            output_filename = '.\滞销品优化\手动sp广告\搜索词优化\预处理.csv'
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
                                    SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN clicks ELSE 0 END) AS total_clicks_yesterday,
                                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_30d,
                                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_7d,
                                    SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END) AS total_sales14d_yesterday,
                                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_30d,
                                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_7d,
                                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 3 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_4d,
                                    SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) AS total_cost_yesterday,
                                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_30d,
                                    SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_7d,
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
                                                       cur_time,
                                                       cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                       cur_time,
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
                        SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN clicks ELSE 0 END) AS total_clicks_yesterday,
                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_30d,
                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS total_sales14d_7d,
                        SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN sales14d ELSE 0 END) AS total_sales14d_yesterday,
                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_30d,
                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_7d,
                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 3 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) AS total_cost_4d,
                        SUM(CASE WHEN date = '{}' - INTERVAL 2 DAY THEN cost ELSE 0 END) AS total_cost_yesterday,
                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_30d,
                        SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_7d,
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
                                                                       cur_time,
                                                                       cur_time, cur_time, cur_time, cur_time, cur_time,
                                                                       cur_time,
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
        sp.market IN ('IT', 'SE', 'FR', 'UK', 'NL', 'ES', 'DE')
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

            if version == 1.0:
                query = """
WITH CampaignStats AS (
    SELECT
        a.campaignId,                          -- 广告活动ID
        a.campaignName,                       -- 广告活动名称
        c.budget AS campaignBudget,           -- 预算
        a.market,                           -- 市场

        -- 计算昨天的花费
        SUM(CASE WHEN a.date = DATE_SUB('{}', INTERVAL 2 DAY) THEN a.cost ELSE 0 END) AS costYesterday,

        -- 计算昨天的点击量
        SUM(CASE WHEN a.date = DATE_SUB('{}', INTERVAL 2 DAY) THEN a.clicks ELSE 0 END) AS clicksYesterday,

        -- 计算昨天的销售额，使用 sales 字段，因为它代表 "Total value of sales occurring within 14 days of an ad click or view."
        SUM(CASE WHEN a.date = DATE_SUB('{}', INTERVAL 2 DAY) THEN a.sales ELSE 0 END) AS salesYesterday,

        -- 计算过去7天的总花费
        SUM(CASE WHEN a.date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) AS totalCost7d,

        -- 计算过去7天的总销售额，使用 sales 字段
        SUM(CASE WHEN a.date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.sales ELSE 0 END) AS totalSales7d,

        -- 计算过去30天的总花费
        SUM(CASE WHEN a.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) AS totalCost30d,

        -- 计算过去30天的总销售额，使用 sales 字段
        SUM(CASE WHEN a.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.sales ELSE 0 END) AS totalSales30d,

        -- 计算过去30天的总点击量
        SUM(CASE WHEN a.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.clicks ELSE 0 END) AS totalClicks30d,

        -- 计算过去7天的总点击量
        SUM(CASE WHEN a.date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.clicks ELSE 0 END) AS totalClicks7d,

        -- 计算过去30天的ACOS (Advertising Cost of Sales)，使用 sales 字段
        SUM(CASE WHEN a.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) / NULLIF(SUM(CASE WHEN a.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.sales ELSE 0 END), 0) AS ACOS30d,

        -- 计算过去7天的ACOS (Advertising Cost of Sales)，使用 sales 字段
        SUM(CASE WHEN a.date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) / NULLIF(SUM(CASE WHEN a.date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.sales ELSE 0 END), 0) AS ACOS7d,

        -- 计算昨天的ACOS (Advertising Cost of Sales)，使用 sales 字段
        SUM(CASE WHEN a.date = DATE_SUB('{}', INTERVAL 2 DAY) THEN a.cost ELSE 0 END) / NULLIF(SUM(CASE WHEN a.date = DATE_SUB('{}', INTERVAL 2 DAY) THEN a.sales ELSE 0 END), 0)  AS ACOSYesterday

    FROM
        amazon_advertised_product_reports_sd a
    JOIN
        amazon_campaigns_list_sd c ON a.campaignId = c.campaignId
    WHERE
        -- 筛选过去30天内的数据
        a.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND ('{}' - INTERVAL 1 DAY)

        -- 筛选在查询日期前一天仍然处于启用状态的广告活动
        AND a.campaignId IN (
            SELECT campaignId
            FROM amazon_advertised_product_reports_sd
            WHERE date = '{}' - INTERVAL 1 DAY
        )

        -- 筛选法国市场的数据
        AND a.market = '{}'

    -- 根据广告活动ID、名称、预算和市场进行分组
    GROUP BY
        a.campaignId,
        a.campaignName,
        c.budget,
        a.market
),

-- 计算每个市场的平均 ACOS
CountryAvgACOS AS (
    SELECT
        SUM(reports.cost) / SUM(reports.sales) AS countryAvgACOS1m,
        reports.market
    FROM
        amazon_advertised_product_reports_sd AS reports
    INNER JOIN
        amazon_campaigns_list_sd AS campaigns ON reports.campaignId = campaigns.campaignId
    WHERE
        reports.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND ('{}' - INTERVAL 1 DAY)
        AND campaigns.campaignId IN (
            SELECT campaignId
            FROM amazon_advertised_product_reports_sd
            WHERE date = '{}' - INTERVAL 1 DAY
        )
        AND reports.market = '{}'
    GROUP BY
        reports.market
)

-- 连接两个 CTE，获取最终结果并筛选campaignName包含0507和0509的数据
SELECT
    cs.*,
    ca.countryAvgACOS1m
FROM
    CampaignStats cs
JOIN
    CountryAvgACOS ca ON cs.market = ca.market
WHERE
    cs.campaignName LIKE '%0507%' OR cs.campaignName LIKE '%0509%';

                                                            """.format(cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                                       cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                                       cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                                       cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                                                       cur_time, cur_time, cur_time, cur_time, country, cur_time, cur_time,
                                                                       cur_time, country)
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

    def preprocessing_sd_sku(self, country, cur_time, version=1.0):

        """"""
        try:
            conn = self.conn

            if version == 1.0:
                query = """
SELECT
    a.adGroupName,                             -- 广告组名称
    a.adId,                                -- 广告ID
    a.campaignId,                          -- 广告活动ID
    a.campaignName,                       -- 广告活动名称
    a.promotedSku AS advertisedSku,        -- 使用 promotedSku 代替 advertisedSku
    -- 计算过去30天内的总订单数，使用 purchases 字段，因为它代表 "Number of attributed conversion events occurring within 14 days of an ad click or view."
    SUM(CASE WHEN a.date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.purchases ELSE 0 END) AS ORDER_1m,
    -- 计算过去7天内的总订单数，使用 purchases 字段
    SUM(CASE WHEN a.date BETWEEN DATE_SUB('{}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.purchases ELSE 0 END) AS ORDER_7d,
    -- 计算过去30天内（包含今天）的总点击量
    SUM(CASE WHEN a.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.clicks ELSE 0 END) AS total_clicks_30d,
    -- 计算过去7天内（包含今天）的总点击量
    SUM(CASE WHEN a.date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.clicks ELSE 0 END) AS total_clicks_7d,
    -- 计算昨天的总点击量
    SUM(CASE WHEN a.date = '{}' - INTERVAL 2 DAY THEN a.clicks ELSE 0 END) AS total_clicks_yesterday,
    -- 计算过去30天内的总销售额，使用 sales 字段，因为它代表 "Total value of sales occurring within 14 days of an ad click or view."
    SUM(CASE WHEN a.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.sales ELSE 0 END) AS total_sales_30d,
    -- 计算过去7天内的总销售额，使用 sales 字段
    SUM(CASE WHEN a.date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.sales ELSE 0 END) AS total_sales_7d,
    -- 计算昨天的总销售额，使用 sales 字段
    SUM(CASE WHEN a.date = '{}' - INTERVAL 2 DAY THEN a.sales ELSE 0 END) AS total_sales_yesterday,
    -- 计算过去30天内的总成本
    SUM(CASE WHEN a.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) AS total_cost_30d,
    -- 计算过去7天内的总成本
    SUM(CASE WHEN a.date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) AS total_cost_7d,
    -- 计算昨天的总成本
    SUM(CASE WHEN a.date = '{}' - INTERVAL 2 DAY THEN a.cost ELSE 0 END) AS total_cost_yesterday,
    -- 计算过去30天内的平均广告花费回报率 (ACOS)，使用 sales 字段
    CASE WHEN SUM(CASE WHEN a.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.sales ELSE 0 END) > 0
         THEN SUM(CASE WHEN a.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) /
              SUM(CASE WHEN a.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.sales ELSE 0 END)
         ELSE 0
    END AS ACOS_30d,
    -- 计算过去7天内的平均广告花费回报率 (ACOS)，使用 sales 字段
    CASE WHEN SUM(CASE WHEN a.date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.sales ELSE 0 END) > 0
         THEN SUM(CASE WHEN a.date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.cost ELSE 0 END) /
              SUM(CASE WHEN a.date BETWEEN DATE_SUB('{}', INTERVAL 7 DAY) AND DATE_SUB('{}', INTERVAL 1 DAY) THEN a.sales ELSE 0 END)
         ELSE 0
    END AS ACOS_7d,
    -- 计算昨天的平均广告花费回报率 (ACOS)，使用 sales 字段
    CASE WHEN SUM(CASE WHEN a.date = '{}' - INTERVAL 2 DAY THEN a.sales ELSE 0 END) > 0
         THEN SUM(CASE WHEN a.date = '{}' - INTERVAL 2 DAY THEN a.cost ELSE 0 END) /
              SUM(CASE WHEN a.date = '{}' - INTERVAL 2 DAY THEN a.sales ELSE 0 END)
         ELSE 0
    END AS ACOS_yesterday
-- 从 amazon_advertised_product_reports_sd 表中读取数据，并将其连接到 amazon_campaigns_list_sd 表
FROM
    amazon_advertised_product_reports_sd a
JOIN
    amazon_campaigns_list_sd c ON a.campaignId = c.campaignId
-- 设置查询条件
WHERE
    a.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY) AND '{}' - INTERVAL 1 DAY  -- 筛选过去30天内的数据
    AND a.market = '{}'
    AND a.campaignId IN (
        SELECT campaignId
        FROM amazon_advertised_product_reports_sd
        WHERE date = '{}' - INTERVAL 1 DAY
    )
    AND (a.campaignName LIKE '%0507%' OR a.campaignName LIKE '%0509%') -- 筛选广告活动名称包含'0507'或'0509' --修改: 添加筛选条件
-- 根据广告组名称、广告ID、广告活动名称和广告商品SKU对结果进行分组
GROUP BY
    adGroupName,
    a.adId,
    a.campaignName,
    advertisedSku
-- 按照广告组名称、广告活动名称和广告商品SKU对结果进行排序
ORDER BY
    adGroupName,
    a.campaignName,
    advertisedSku;
                                                            """.format(cur_time, cur_time, cur_time, cur_time, cur_time,
                                                                       cur_time,cur_time, cur_time, cur_time, cur_time,
                                                                       cur_time, cur_time, cur_time, cur_time, cur_time,
                                                                       cur_time,cur_time, cur_time, cur_time,
                                                                       cur_time, cur_time, cur_time, cur_time, cur_time,
                                                                       cur_time, cur_time, cur_time, cur_time, cur_time,
                                                                       cur_time,
                                                                       cur_time, cur_time, cur_time, cur_time,
                                                                       cur_time, cur_time, country,
                                                                       cur_time)
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


# amr = AmazonMysqlRagUitl('LAPASA')
# #amr.preprocessing_sp_anomaly_detection_macroscopic('FR','2024-07-09')
# amr.preprocessing_product_targets_search_term('US','2024-07-15')
