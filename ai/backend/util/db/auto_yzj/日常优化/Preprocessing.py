import pymysql
import pandas as pd
from datetime import datetime
import warnings
import os
from ai.backend.util.db.auto_yzj.utils.trans_to import csv_to_json

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

    def __init__(self):
        db_info = {'host': '192.168.5.114', 'user': 'test_deepdata', 'passwd': 'test123!@#', 'port': 3308,
                   'db': 'amazon_ads',
                   'charset': 'utf8mb4', 'use_unicode': True, }
        self.conn = self.connect(db_info)

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

    def preprocessing_sku(self, country, cur_time, version=1.2):

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
        WHERE campaignStatus = 'ENABLED' AND date = ('{}'-INTERVAL 1 DAY)
    )
    AND ( campaignName not LIKE '%AUTO%' and  campaignName not  LIKE '%auto%' and campaignName not LIKE '%Auto%' and  campaignName not LIKE '%自动%' )
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
        WHERE campaignStatus = 'ENABLED' AND date = ('{}'-INTERVAL 1 DAY)
    )
    AND ( campaignName not LIKE '%AUTO%' and  campaignName not  LIKE '%auto%' and campaignName not LIKE '%Auto%' and  campaignName not LIKE '%自动%' )
GROUP BY
    adGroupName,
    campaignName,
    advertisedSku
ORDER BY
    adGroupName,
    campaignName,
    advertisedSku;
                """.format(cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,country,cur_time)
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
    AND c.targetingType like '%MAN%'
GROUP BY
    adGroupName,
    a.adId,
    campaignName,
    advertisedSku

ORDER BY
    adGroupName,
    campaignName,
    advertisedSku;
                            """.format(cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,cur_time,
                                       cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                       cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time,
                                       cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, country, cur_time)
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
        FROM amazon_advertised_product_reports_sp
        WHERE campaignStatus = 'ENABLED' AND date = '{}' - INTERVAL 1 DAY
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

    def preprocessing_search_term(self, country, cur_time, version=1.3):

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
              b.keywordId IN (SELECT keywordId FROM amazon_targeting_reports_sp WHERE campaignStatus = 'ENABLED' AND date = '{}' - INTERVAL 1 DAY)
                    and b.date= '{}' - INTERVAL 1 DAY
                    AND b.matchType not in ('TARGETING_EXPRESSION')
              AND b.keywordId NOT IN (
                SELECT DISTINCT entityId
                FROM amazon_advertising_change_history
                WHERE timestamp >= (UNIX_TIMESTAMP(NOW(3)) - 4 * 24 * 60 * 60) * 1000
                AND entityType = 'KEYWORD'
              )
                    group by b.keywordId;
                            """.format(cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, country, cur_time,
                                       cur_time)
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
                        WHERE campaignStatus = 'ENABLED' AND date = '{}' - INTERVAL 1 DAY )
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
                 WHERE campaignStatus = 'ENABLED' AND date = '{}' - INTERVAL 1 DAY )

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

    def preprocessing_sku_auto(self, country, cur_time, version=1.2):

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
                and a.keywordId in (select keywordId from amazon_targeting_reports_sp where campaignStatus='ENABLED' and date='{}'-INTERVAL 1 DAY)
    AND c.targetingType like '%AUT%'
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
        FROM amazon_advertised_product_reports_sp
        WHERE campaignStatus = 'ENABLED' AND date = '{}' - INTERVAL 1 DAY
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
        b.keywordId IN ( SELECT keywordId FROM amazon_targeting_reports_sp WHERE campaignStatus = 'ENABLED' AND date = '{}' - INTERVAL 1 DAY )
        AND b.date = '{}' - INTERVAL 1 DAY
        AND b.keywordId NOT IN (
        SELECT DISTINCT
                entityId
        FROM
                amazon_advertising_change_history
        WHERE
                TIMESTAMP >= ( UNIX_TIMESTAMP( NOW( 3 )) - 4 * 24 * 60 * 60 ) * 1000
                AND entityType = 'KEYWORD'
                AND market = '{}'
        )
GROUP BY
        b.keywordId;
                            """.format(cur_time, cur_time, cur_time, cur_time, cur_time, cur_time, country,
                                       cur_time, cur_time, country)
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
            WHERE campaignStatus = 'ENABLED' AND date = '{}' - INTERVAL 1 DAY )
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

    AND campaigns.targetingType LIKE '%MAN%'  -- 筛选手动广告
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
    b.date = DATE_SUB('{}', INTERVAL 1 DAY)
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
    b.date = DATE_SUB('{}', INTERVAL 1 DAY)
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
    b.date = DATE_SUB('{}', INTERVAL 1 DAY)
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
    b.date = DATE_SUB('{}', INTERVAL 1 DAY)
                """.format(cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,cur_time,campaign_ids,cur_time,cur_time,country,cur_time)
            df1 = pd.read_sql(query, con=conn)

            output_filename = '.\日常优化\异常定位检测\投放词\预处理1.csv'
            df1.to_csv(output_filename, index=False, encoding='utf-8-sig')
            csv_to_json(output_filename)
            # return df
            return print("查询已完成，请查看文件： " + output_filename)

        except Exception as error:
            print("Error while inserting data:", error)

# amr = AmazonMysqlRagUitl()
# amr.preprocessing('NL','2024-5-27')
