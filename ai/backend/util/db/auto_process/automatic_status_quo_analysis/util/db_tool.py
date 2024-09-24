import json
import os
from datetime import datetime

import pandas as pd
import pymysql
from ai.backend.util.db.configuration.path import get_config_path
from ai.backend.util.db.auto_process.automatic_status_quo_analysis.export.export_path import get_export_path



def get_timestamp():
    # 获取当前时间
    current_time = datetime.now()
    timestamp = int(current_time.timestamp())
    date_string = current_time.strftime("%Y-%m-%d")
    # 组合日期和时间戳
    date_timestamp_string = f"{date_string}_{timestamp}"
    return date_timestamp_string

class DbToolsCsv:
    def __init__(self, brand,market):
        self.brand = brand
        self.market = market
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

    def load_config_info(self):
        # 从 JSON 文件加载数据库信息
        time_zone_info_path = os.path.join(get_config_path(), 'time_zone_information.json')
        with open(time_zone_info_path, 'r') as f:
            time_zone_info_json = json.load(f)
        return time_zone_info_json.get(self.market, "国家代码不存在")

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

    def get_advertising_data(self, market,start_date,end_date):
        # 低于 平均ACOS值 30% 以上的  campaign 广告活动
        # 建议执行的操作：预算提升30%
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            query = f"""
WITH a AS (
    SELECT
        '含SB广告' AS 广告数据,
        (COALESCE(sp.sum_sales, 0) + COALESCE(sd.sum_sales, 0) + COALESCE(sb.sum_sales, 0)) AS 广告销售额,
        (COALESCE(sp.sum_cost, 0) + COALESCE(sd.sum_cost, 0) + COALESCE(sb.sum_cost, 0)) AS 广告花费,
        CONCAT(ROUND(
            ((COALESCE(sp.sum_cost, 0) + COALESCE(sd.sum_cost, 0) + COALESCE(sb.sum_cost, 0)) /
            (COALESCE(sp.sum_sales, 0) + COALESCE(sd.sum_sales, 0) + COALESCE(sb.sum_sales, 0)))* 100,
            2
        ),'%') AS ACOS
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
                DATE >= '{end_date}'
                AND DATE <= '{start_date}'
            GROUP BY
                market
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
                DATE >= '{end_date}'
                AND DATE <= '{start_date}'
            GROUP BY
                market
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
                DATE >= '{end_date}'
                AND DATE <= '{start_date}'
            GROUP BY
                market
        ) AS sb ON sb.market = sp.market AND sb.DATE = sp.DATE
    WHERE
        sp.market = '{market}'
), b AS (
    SELECT
        '不含SB广告' AS 广告数据,
        (COALESCE(sp.sum_sales, 0) + COALESCE(sd.sum_sales, 0)) AS 广告销售额,
        (COALESCE(sp.sum_cost, 0) + COALESCE(sd.sum_cost, 0)) AS 广告花费,
        CONCAT(ROUND(
            ((COALESCE(sp.sum_cost, 0) + COALESCE(sd.sum_cost, 0)) /
            (COALESCE(sp.sum_sales, 0) + COALESCE(sd.sum_sales, 0)))* 100,
            2
        ),'%') AS ACOS
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
                DATE >= '{end_date}'
                AND DATE <= '{start_date}'
            GROUP BY
                market
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
                DATE >= '{end_date}'
                AND DATE <= '{start_date}'
            GROUP BY
                market
        ) AS sd ON sd.market = sp.market AND sd.DATE = sp.DATE
    WHERE
        sp.market = '{market}'
)
SELECT
    a.*,
    ROUND(a.广告销售额 / 30, 2) AS 日均销售额,
    ROUND(a.广告花费 / 30, 2) AS 日均花费
FROM a
UNION
SELECT
    b.*,
    ROUND(b.广告销售额 / 30, 2) AS 日均销售额,
    ROUND(b.广告花费 / 30, 2) AS 日均花费
FROM b;


             """
            df = pd.read_sql(query, con=conn)
            output_filename = f'{self.brand}_{market}_advertising_data.csv'
            csv_path = os.path.join(get_export_path(), output_filename)
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            print("get_advertising_data successfully!")
            return csv_path
        except Exception as error:
            print("get_advertising_data Error while query data:", error)


    def get_store_data(self, market,start_date,end_date):
        # 低于 平均ACOS值 30% 以上的  campaign 广告活动
        # 建议执行的操作：预算提升30%
        try:
            conn = self.conn
            # 暂时忽略了market转化 US
            query = f"""
SELECT
        '' as 全店数据,
        all_order.总销售额,
        ad_order.广告总销售额 AS 广告销售额,
        ROUND((all_order.总销售额 - ad_order.广告总销售额),2) AS 自然销售额,
        CONCAT(ROUND(((1 - ad_order.广告总销售额 / all_order.总销售额) * 100), 2),'%') AS 自然销售额比例,
                                ad_order.广告总花费 AS 广告花费,
                                CONCAT(ROUND(COALESCE(((ad_order.广告总花费 / all_order.总销售额) * 100), 0), 2), '%') AS 广告花费占比
    FROM
    (
        -- 计算总销售额
        SELECT
            sales_channel AS 国家,
            '{market}' AS market,
            DATE(CONVERT_TZ(purchase_date ,'+08:00', '{self.load_config_info()['timezone_offset']}')) AS event_date,
            ROUND(SUM(item_price), 2) AS 总销售额
        FROM
            amazon_get_flat_file_all_orders_data_by_last_update_general
        WHERE
            CONVERT_TZ(purchase_date ,'+08:00', '{self.load_config_info()['timezone_offset']}') >= '{end_date}'
            AND CONVERT_TZ(purchase_date ,'+08:00', '{self.load_config_info()['timezone_offset']}') < '{start_date}' + INTERVAL 1 DAY
            AND sales_channel = '{self.load_config_info()['sales_channel']}'
        GROUP BY
            sales_channel
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
                    DATE >= '{end_date}'
                    AND DATE <= '{start_date}'
                GROUP BY
                    market
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
                    DATE >= '{end_date}'
                    AND DATE <= '{start_date}'
                GROUP BY
                    market
            ) AS sd ON sd.market = sp.market
        LEFT JOIN (
                SELECT
                    market,
                    DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales) AS sum_sales
                FROM
                    amazon_campaign_reports_sb
                WHERE
                    DATE >= '{end_date}'
                    AND DATE <= '{start_date}'
                GROUP BY
                    market
            ) AS sb ON sb.market = sp.market
        WHERE
            sp.market = '{market}'
        ORDER BY
            sp.DATE
    ) AS ad_order ON all_order.market = ad_order.国家

             """
            df = pd.read_sql(query, con=conn)
            output_filename = f'{self.brand}_{market}_store_data.csv'
            csv_path = os.path.join(get_export_path(), output_filename)
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            print("get_store_data successfully!")
            return csv_path
        except Exception as error:
            print("get_store_data Error while query data:", error)

    def get_store_data_supplier(self, market, start_date, end_date):
        # 低于 平均ACOS值 30% 以上的  campaign 广告活动
        # 建议执行的操作：预算提升30%
        try:
            conn = self.conn
            # 暂时忽略了market转化 US
            query = f"""
SELECT
        '' as 全店数据,
        all_order.总销售额,
        ad_order.广告总销售额 AS 广告销售额,
        ROUND((all_order.总销售额 - ad_order.广告总销售额),2) AS 自然销售额,
        CONCAT(ROUND(((1 - ad_order.广告总销售额 / all_order.总销售额) * 100), 2),'%') AS 自然销售额比例,
                                ad_order.广告总花费 AS 广告花费,
                                CONCAT(ROUND(COALESCE(((ad_order.广告总花费 / all_order.总销售额) * 100), 0), 2), '%') AS 广告花费占比
    FROM
    (
        SELECT
            market,
            DATE(CONVERT_TZ(endTime ,'+08:00', '{self.load_config_info()['timezone_offset']}')) AS event_date,
            ROUND(SUM(orderedRevenue), 2) AS 总销售额
        FROM
            amazon_vendor_get_real_time_sales_report
        WHERE
            CONVERT_TZ(endTime ,'+08:00', '{self.load_config_info()['timezone_offset']}') >= '{end_date}'
            AND CONVERT_TZ(endTime ,'+08:00', '{self.load_config_info()['timezone_offset']}') < '{start_date}' + INTERVAL 1 DAY
            AND market = '{market}'
        GROUP BY
            market
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
                    DATE >= '{end_date}'
                    AND DATE <= '{start_date}'
                GROUP BY
                    market
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
                    DATE >= '{end_date}'
                    AND DATE <= '{start_date}'
                GROUP BY
                    market
            ) AS sd ON sd.market = sp.market
        LEFT JOIN (
                SELECT
                    market,
                    DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales) AS sum_sales
                FROM
                    amazon_campaign_reports_sb
                WHERE
                    DATE >= '{end_date}'
                    AND DATE <= '{start_date}'
                GROUP BY
                    market
            ) AS sb ON sb.market = sp.market
        WHERE
            sp.market = '{market}'
        ORDER BY
            sp.DATE
    ) AS ad_order ON all_order.market = ad_order.国家

             """
            df = pd.read_sql(query, con=conn)
            output_filename = f'{self.brand}_{market}_store_data.csv'
            csv_path = os.path.join(get_export_path(), output_filename)
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            print("get_store_data successfully!")
            return csv_path
        except Exception as error:
            print("get_store_data Error while query data:", error)

    def get_ad_type(self, market,start_date,end_date):
        # 低于 平均ACOS值 30% 以上的  campaign 广告活动
        # 建议执行的操作：预算提升30%
        try:
            conn = self.conn
            # 暂时忽略了market转化 US
            query = f"""
WITH a AS (
    SELECT COUNT(*) AS SP_MANUAL FROM amazon_campaigns_list_sp
    WHERE market = '{market}'
    AND state = 'ENABLED'
    AND targetingType = 'MANUAL'
),
b AS (
    SELECT COUNT(*) AS SP_AUTO FROM amazon_campaigns_list_sp
    WHERE market = '{market}'
    AND state = 'ENABLED'
    AND targetingType = 'AUTO'
),
c AS (
    SELECT COUNT(*) AS SD_ADS FROM amazon_campaigns_list_sd
    WHERE market = '{market}'
    AND state = 'enabled'
),
d AS (
    SELECT COUNT(*) AS SB_ADS FROM amazon_campaign_reports_sb
    WHERE date = '{end_date}'
    AND campaignStatus = 'ENABLED'
    AND market = '{market}'
)
SELECT
        '广告数量' AS 广告类型,
        b.SP_AUTO AS SP自动,
        a.SP_MANUAL AS SP手动,
        c.SD_ADS AS SD广告,
        d.SB_ADS AS SB广告,
        ( a.SP_MANUAL + b.SP_AUTO + c.SD_ADS + d.SB_ADS ) AS 总计
FROM
        a
        CROSS JOIN b
        CROSS JOIN c
        CROSS JOIN d;

             """
            df = pd.read_sql(query, con=conn)
            output_filename = f'{self.brand}_{market}_ad_type.csv'
            csv_path = os.path.join(get_export_path(), output_filename)
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            print("get_ad_type successfully!")
            return csv_path
        except Exception as error:
            print("get_ad_type Error while query data:", error)

    def get_ad_type_data(self, market,start_date,end_date):
        # 低于 平均ACOS值 30% 以上的  campaign 广告活动
        # 建议执行的操作：预算提升30%
        try:
            conn = self.conn
            # 暂时忽略了market转化 US
            query = f"""
WITH a AS (
    SELECT
        '广告整体' AS 广告类型,
        (COALESCE(sp.sum_sales, 0) + COALESCE(sd.sum_sales, 0) + COALESCE(sb.sum_sales, 0)) AS 广告销售额,
        (COALESCE(sp.sum_cost, 0) + COALESCE(sd.sum_cost, 0) + COALESCE(sb.sum_cost, 0)) AS 广告花费,
        CONCAT(
            CASE
                WHEN ROUND(
                    ((COALESCE(sp.sum_cost, 0) + COALESCE(sd.sum_cost, 0) + COALESCE(sb.sum_cost, 0)) /
                    (COALESCE(sp.sum_sales, 0) + COALESCE(sd.sum_sales, 0) + COALESCE(sb.sum_sales, 0))* 100), 2
                ) = ROUND(
                    ((COALESCE(sp.sum_cost, 0) + COALESCE(sd.sum_cost, 0) + COALESCE(sb.sum_cost, 0)) /
                    (COALESCE(sp.sum_sales, 0) + COALESCE(sd.sum_sales, 0) + COALESCE(sb.sum_sales, 0))* 100), 0
                ) THEN ROUND(
                    ((COALESCE(sp.sum_cost, 0) + COALESCE(sd.sum_cost, 0) + COALESCE(sb.sum_cost, 0)) /
                    (COALESCE(sp.sum_sales, 0) + COALESCE(sd.sum_sales, 0) + COALESCE(sb.sum_sales, 0))* 100), 0
                )
                ELSE ROUND(
                    ((COALESCE(sp.sum_cost, 0) + COALESCE(sd.sum_cost, 0) + COALESCE(sb.sum_cost, 0)) /
                    (COALESCE(sp.sum_sales, 0) + COALESCE(sd.sum_sales, 0) + COALESCE(sb.sum_sales, 0))* 100), 2
                )
            END, '%'
        ) AS ACOS
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
                DATE >= '{end_date}'
                AND DATE <= '{start_date}'
            GROUP BY
                market
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
                DATE >= '{end_date}'
                AND DATE <= '{start_date}'
            GROUP BY
                market
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
                DATE >= '{end_date}'
                AND DATE <= '{start_date}'
            GROUP BY
                market
        ) AS sb ON sb.market = sp.market AND sb.DATE = sp.DATE
    WHERE
        sp.market = '{market}'
),
b AS (
    SELECT
        'SP整体' AS 广告类型,
        (COALESCE(sp.sum_sales, 0)) AS 广告销售额,
        (COALESCE(sp.sum_cost, 0)) AS 广告花费,
        CONCAT(
            CASE
                WHEN ROUND(
                    ((COALESCE(sp.sum_cost, 0)) /
                    (COALESCE(sp.sum_sales, 0))* 100), 2
                ) = ROUND(
                    ((COALESCE(sp.sum_cost, 0)) /
                    (COALESCE(sp.sum_sales, 0))* 100), 0
                ) THEN ROUND(
                    ((COALESCE(sp.sum_cost, 0)) /
                    (COALESCE(sp.sum_sales, 0))* 100), 0
                )
                ELSE ROUND(
                    ((COALESCE(sp.sum_cost, 0)) /
                    (COALESCE(sp.sum_sales, 0))* 100), 2
                )
            END, '%'
        ) AS ACOS
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
                DATE >= '{end_date}'
                AND DATE <= '{start_date}'
            GROUP BY
                market
        ) AS sp
    WHERE
        sp.market = '{market}'
),
c AS (
    SELECT
        'SD广告' AS 广告类型,
        (COALESCE(sd.sum_sales, 0)) AS 广告销售额,
        (COALESCE(sd.sum_cost, 0)) AS 广告花费,
        CONCAT(
            CASE
                WHEN ROUND(
                    ((COALESCE(sd.sum_cost, 0)) /
                    (COALESCE(sd.sum_sales, 0))* 100), 2
                ) = ROUND(
                    ((COALESCE(sd.sum_cost, 0)) /
                    (COALESCE(sd.sum_sales, 0))* 100), 0
                ) THEN ROUND(
                    ((COALESCE(sd.sum_cost, 0)) /
                    (COALESCE(sd.sum_sales, 0))* 100), 0
                )
                ELSE ROUND(
                    ((COALESCE(sd.sum_cost, 0)) /
                    (COALESCE(sd.sum_sales, 0))* 100), 2
                )
            END, '%'
        ) AS ACOS
    FROM
        (
            SELECT
                market,
                DATE,
                SUM(cost) AS sum_cost,
                SUM(sales) AS sum_sales
            FROM
                amazon_campaign_reports_sd
            WHERE
                DATE >= '{end_date}'
                AND DATE <= '{start_date}'
            GROUP BY
                market
        ) AS sd
    WHERE
        sd.market = '{market}'
),
d AS (
    SELECT
        'SB广告' AS 广告类型,
        COALESCE(SUM(sb.sum_sales), 0) AS 广告销售额,
        COALESCE(SUM(sb.sum_cost), 0) AS 广告花费,
        CONCAT(
            CASE
                WHEN ROUND(
                    ((COALESCE(sb.sum_cost, 0)) /
                    (COALESCE(sb.sum_sales, 0))* 100), 2
                ) = ROUND(
                    ((COALESCE(sb.sum_cost, 0)) /
                    (COALESCE(sb.sum_sales, 0))* 100), 0
                ) THEN ROUND(
                    ((COALESCE(sb.sum_cost, 0)) /
                    (COALESCE(sb.sum_sales, 0))* 100), 0
                )
                ELSE ROUND(
                    ((COALESCE(sb.sum_cost, 0)) /
                    (COALESCE(sb.sum_sales, 0))* 100), 2
                )
            END, '%'
        ) AS ACOS
    FROM
        (
            SELECT
                market,
                DATE,
                SUM(cost) AS sum_cost,
                SUM(sales) AS sum_sales
            FROM
                amazon_campaign_reports_sb
            WHERE
                DATE >= '{end_date}'
                AND DATE <= '{start_date}'
            GROUP BY
                market
        ) AS sb
    WHERE
        sb.market = '{market}'
)
SELECT
    b.*,
    CONCAT(
        CASE
            WHEN ROUND((b.广告销售额 / a.广告销售额)*100, 2) = ROUND((b.广告销售额 / a.广告销售额)*100, 0)
            THEN ROUND((b.广告销售额 / a.广告销售额)*100, 0)
            ELSE ROUND((b.广告销售额 / a.广告销售额)*100, 2)
        END, '%'
    ) AS 销售额占比,
    CONCAT(
        CASE
            WHEN ROUND((b.广告花费 / a.广告花费)*100, 2) = ROUND((b.广告花费 / a.广告花费)*100, 0)
            THEN ROUND((b.广告花费 / a.广告花费)*100, 0)
            ELSE ROUND((b.广告花费 / a.广告花费)*100, 2)
        END, '%'
    ) AS 广告花费占比
FROM b CROSS JOIN a
UNION
SELECT
    c.*,
    CONCAT(
        CASE
            WHEN ROUND((c.广告销售额 / a.广告销售额)*100, 2) = ROUND((c.广告销售额 / a.广告销售额)*100, 0)
            THEN ROUND((c.广告销售额 / a.广告销售额)*100, 0)
            ELSE ROUND((c.广告销售额 / a.广告销售额)*100, 2)
        END, '%'
    ) AS 销售额占比,
    CONCAT(
        CASE
            WHEN ROUND((c.广告花费 / a.广告花费)*100, 2) = ROUND((c.广告花费 / a.广告花费)*100, 0)
            THEN ROUND((c.广告花费 / a.广告花费)*100, 0)
            ELSE ROUND((c.广告花费 / a.广告花费)*100, 2)
        END, '%'
    ) AS 广告花费占比
FROM c CROSS JOIN a
UNION
SELECT
    d.*,
    CONCAT(
        CASE
            WHEN ROUND((d.广告销售额 / a.广告销售额)*100, 2) = ROUND((d.广告销售额 / a.广告销售额)*100, 0)
            THEN ROUND((d.广告销售额 / a.广告销售额)*100, 0)
            ELSE ROUND((d.广告销售额 / a.广告销售额)*100, 2)
        END, '%'
    ) AS 销售额占比,
    CONCAT(
        CASE
            WHEN ROUND((d.广告花费 / a.广告花费)*100, 2) = ROUND((d.广告花费 / a.广告花费)*100, 0)
            THEN ROUND((d.广告花费 / a.广告花费)*100, 0)
            ELSE ROUND((d.广告花费 / a.广告花费)*100, 2)
        END, '%'
    ) AS 广告花费占比
FROM d CROSS JOIN a
UNION
SELECT
    a.*,
    CONCAT(
        CASE
            WHEN ROUND((a.广告销售额 / a.广告销售额)*100, 2) = ROUND((a.广告销售额 / a.广告销售额)*100, 0)
            THEN ROUND((a.广告销售额 / a.广告销售额)*100, 0)
            ELSE ROUND((a.广告销售额 / a.广告销售额)*100, 2)
        END, '%'
    ) AS 销售额占比,
    CONCAT(
        CASE
            WHEN ROUND((a.广告花费 / a.广告花费)*100, 2) = ROUND((a.广告花费 / a.广告花费)*100, 0)
            THEN ROUND((a.广告花费 / a.广告花费)*100, 0)
            ELSE ROUND((a.广告花费 / a.广告花费)*100, 2)
        END, '%'
    ) AS 广告花费占比
FROM a
             """
            df = pd.read_sql(query, con=conn)
            output_filename = f'{self.brand}_{market}_ad_type_data.csv'
            csv_path = os.path.join(get_export_path(), output_filename)
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            print("get_ad_type_data successfully!")
            return csv_path
        except Exception as error:
            print("get_ad_type_data Error while query data:", error)


    def get_sp_type_data(self, market,start_date,end_date):
        # 低于 平均ACOS值 30% 以上的  campaign 广告活动
        # 建议执行的操作：预算提升30%
        try:
            conn = self.conn
            # 暂时忽略了market转化 US
            query = f"""
WITH b AS (
    SELECT
        'SP整体' AS 广告类型,
        COALESCE(sp.sum_sales, 0) AS 广告销售额,
        COALESCE(sp.sum_cost, 0) AS 广告花费,
        CONCAT(
            CASE
                WHEN ROUND(
                    ((COALESCE(sp.sum_cost, 0)) /
                    (COALESCE(sp.sum_sales, 0)))* 100, 2
                ) = ROUND(
                    ((COALESCE(sp.sum_cost, 0)) /
                    (COALESCE(sp.sum_sales, 0)))* 100, 0
                ) THEN ROUND(
                    ((COALESCE(sp.sum_cost, 0)) /
                    (COALESCE(sp.sum_sales, 0)))* 100, 0
                )
                ELSE ROUND(
                    ((COALESCE(sp.sum_cost, 0)) /
                    (COALESCE(sp.sum_sales, 0)))* 100, 2
                )
            END, '%'
        ) AS ACOS
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
                DATE >= '{end_date}'
                AND DATE <= '{start_date}'
            GROUP BY
                market
        ) AS sp
    WHERE
        sp.market = '{market}'
),
c AS (
    SELECT
        'SP手动' AS 广告类型,
        COALESCE(sd.sum_sales, 0) AS 广告销售额,
        COALESCE(sd.sum_cost, 0) AS 广告花费,
        CONCAT(
            CASE
                WHEN ROUND(
                    ((COALESCE(sd.sum_cost, 0)) /
                    (COALESCE(sd.sum_sales, 0)))* 100, 2
                ) = ROUND(
                    ((COALESCE(sd.sum_cost, 0)) /
                    (COALESCE(sd.sum_sales, 0)))* 100, 0
                ) THEN ROUND(
                    ((COALESCE(sd.sum_cost, 0)) /
                    (COALESCE(sd.sum_sales, 0)))* 100, 0
                )
                ELSE ROUND(
                    ((COALESCE(sd.sum_cost, 0)) /
                    (COALESCE(sd.sum_sales, 0)))* 100, 2
                )
            END, '%'
        ) AS ACOS
    FROM
        (
            SELECT
                acr.market,
                acr.DATE,
                SUM(acr.cost) AS sum_cost,
                SUM(acr.sales7d) AS sum_sales
            FROM
                amazon_campaign_reports_sp acr
            LEFT JOIN
                amazon_campaigns_list_sp acl ON acr.campaignId = acl.campaignId
            WHERE
                acr.DATE >= '{end_date}'
                AND acr.DATE <= '{start_date}'
                AND acl.targetingType = 'MANUAL'
            GROUP BY
                acr.market
        ) AS sd
    WHERE
        sd.market = '{market}'
),
d AS (
    SELECT
        'SP自动' AS 广告类型,
        COALESCE(SUM(sb.sum_sales), 0) AS 广告销售额,
        COALESCE(SUM(sb.sum_cost), 0) AS 广告花费,
        CONCAT(
            CASE
                WHEN ROUND(
                    ((COALESCE(sb.sum_cost, 0)) /
                    (COALESCE(sb.sum_sales, 0)))* 100, 2
                ) = ROUND(
                    ((COALESCE(sb.sum_cost, 0)) /
                    (COALESCE(sb.sum_sales, 0)))* 100, 0
                ) THEN ROUND(
                    ((COALESCE(sb.sum_cost, 0)) /
                    (COALESCE(sb.sum_sales, 0)))* 100, 0
                )
                ELSE ROUND(
                    ((COALESCE(sb.sum_cost, 0)) /
                    (COALESCE(sb.sum_sales, 0)))* 100, 2
                )
            END, '%'
        ) AS ACOS
    FROM
        (
            SELECT
                acr.market,
                acr.DATE,
                SUM(cost) AS sum_cost,
                SUM(sales7d) AS sum_sales
            FROM
                amazon_campaign_reports_sp acr
            LEFT JOIN
                amazon_campaigns_list_sp acl ON acr.campaignId = acl.campaignId
            WHERE
                acr.DATE >= '{end_date}'
                AND acr.DATE <= '{start_date}'
                AND acl.targetingType = 'AUTO'
            GROUP BY
                acr.market
        ) AS sb
    WHERE
        sb.market = '{market}'
)
SELECT
    c.*,
    CONCAT(
        CASE
            WHEN ROUND((c.广告销售额 / b.广告销售额)*100, 2) = ROUND((c.广告销售额 / b.广告销售额)*100, 0)
            THEN ROUND((c.广告销售额 / b.广告销售额)*100, 0)
            ELSE ROUND((c.广告销售额 / b.广告销售额)*100, 2)
        END, '%'
    ) AS 销售额占比,
    CONCAT(
        CASE
            WHEN ROUND((c.广告花费 / b.广告花费)*100, 2) = ROUND((c.广告花费 / b.广告花费)*100, 0)
            THEN ROUND((c.广告花费 / b.广告花费)*100, 0)
            ELSE ROUND((c.广告花费 / b.广告花费)*100, 2)
        END, '%'
    ) AS 广告花费占比
FROM c CROSS JOIN b
UNION
SELECT
    d.*,
    CONCAT(
        CASE
            WHEN ROUND((d.广告销售额 / b.广告销售额)*100, 2) = ROUND((d.广告销售额 / b.广告销售额)*100, 0)
            THEN ROUND((d.广告销售额 / b.广告销售额)*100, 0)
            ELSE ROUND((d.广告销售额 / b.广告销售额)*100, 2)
        END, '%'
    ) AS 销售额占比,
    CONCAT(
        CASE
            WHEN ROUND((d.广告花费 / b.广告花费)*100, 2) = ROUND((d.广告花费 / b.广告花费)*100, 0)
            THEN ROUND((d.广告花费 / b.广告花费)*100, 0)
            ELSE ROUND((d.广告花费 / b.广告花费)*100, 2)
        END, '%'
    ) AS 广告花费占比
FROM d CROSS JOIN b
UNION
SELECT
    b.*,
    CONCAT(
        CASE
            WHEN ROUND((b.广告销售额 / b.广告销售额)*100, 2) = ROUND((b.广告销售额 / b.广告销售额)*100, 0)
            THEN ROUND((b.广告销售额 / b.广告销售额)*100, 0)
            ELSE ROUND((b.广告销售额 / b.广告销售额)*100, 2)
        END, '%'
    ) AS 销售额占比,
    CONCAT(
        CASE
            WHEN ROUND((b.广告花费 / b.广告花费)*100, 2) = ROUND((b.广告花费 / b.广告花费)*100, 0)
            THEN ROUND((b.广告花费 / b.广告花费)*100, 0)
            ELSE ROUND((b.广告花费 / b.广告花费)*100, 2)
        END, '%'
    ) AS 广告花费占比
FROM b

             """
            df = pd.read_sql(query, con=conn)
            output_filename = f'{self.brand}_{market}_sp_type_data.csv'
            csv_path = os.path.join(get_export_path(), output_filename)
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            print("get_sp_type_data successfully!")
            return csv_path
        except Exception as error:
            print("get_sp_type_data Error while query data:", error)


    def get_listing_summary_data(self, market,start_date,end_date):

        try:
            conn = self.conn
            # 暂时忽略了market转化 US
            query = f"""
SELECT
    all_order.parent_asins_or_asin AS list,
    COALESCE(ad_order.广告总销售额, 0) AS 广告销售额,
    COALESCE(ad_order.广告总花费, 0) AS 广告花费,
    CONCAT(
        CASE
            WHEN ROUND((COALESCE(ad_order.广告总花费, 0) / all_order.总销售额) * 100, 2) = ROUND((COALESCE(ad_order.广告总花费, 0) / all_order.总销售额) * 100, 0)
            THEN ROUND((COALESCE(ad_order.广告总花费, 0) / all_order.总销售额) * 100, 0)
            ELSE ROUND((COALESCE(ad_order.广告总花费, 0) / all_order.总销售额) * 100, 2)
        END, '%'
    ) AS 广告花费占比,
    CONCAT(
        CASE
            WHEN ROUND(ad_order.广告总ACOS * 100, 2) = ROUND(ad_order.广告总ACOS * 100, 0)
            THEN ROUND(ad_order.广告总ACOS * 100, 0)
            ELSE ROUND(ad_order.广告总ACOS * 100, 2)
        END, '%'
    ) AS 广告总ACOS,
    CONCAT(
        CASE
            WHEN ROUND(((COALESCE(ad_order.广告总销售额, 0) / all_order.总销售额) * 100), 2) = ROUND(((COALESCE(ad_order.广告总销售额, 0) / all_order.总销售额) * 100), 0)
            THEN ROUND(((COALESCE(ad_order.广告总销售额, 0) / all_order.总销售额) * 100), 0)
            ELSE ROUND(((COALESCE(ad_order.广告总销售额, 0) / all_order.总销售额) * 100), 2)
        END, '%'
    ) AS 广告销售额比例,
    all_order.总销售额,
    ROUND(all_order.总销售额 / 30, 2) AS 日均销售额
FROM
(
    SELECT
        sales_channel AS 国家,
        CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END AS parent_asins_or_asin,
        DATE(CONVERT_TZ(purchase_date,'+08:00', '{self.load_config_info()['timezone_offset']}')) AS event_date,
        ROUND(SUM(item_price), 2) AS 总销售额
    FROM
        amazon_get_flat_file_all_orders_data_by_last_update_general agffa
    LEFT JOIN
        amazon_product_info_extended apie ON agffa.asin = apie.asin
    WHERE
        CONVERT_TZ(purchase_date,'+08:00', '{self.load_config_info()['timezone_offset']}') >= '{end_date}'
        AND CONVERT_TZ(purchase_date,'+08:00', '{self.load_config_info()['timezone_offset']}') < '{start_date}' + INTERVAL 1 DAY
        AND sales_channel = '{self.load_config_info()['sales_channel']}'
        AND apie.market = '{market}'
    GROUP BY
        sales_channel,
        CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END
) AS all_order
LEFT JOIN
(
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
                CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END AS parent_asins_or_asin,
                adpr.market,
                adpr.DATE,
                SUM(cost) AS sum_cost,
                SUM(sales7d) AS sum_sales
            FROM
                amazon_advertised_product_reports_sp adpr
            LEFT JOIN
                amazon_product_info_extended apie ON adpr.advertisedAsin = apie.asin AND adpr.market = apie.market
            WHERE
                adpr.DATE >= '{end_date}'
                AND adpr.DATE <= '{start_date}'
            GROUP BY
                adpr.market,
                CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END
        ) AS sp
    LEFT JOIN (
        SELECT
            CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END AS parent_asins_or_asin,
            adpr.market,
            adpr.DATE,
            SUM(cost) AS sum_cost,
            SUM(sales) AS sum_sales
        FROM
            amazon_advertised_product_reports_sd adpr
        LEFT JOIN
            amazon_product_info_extended apie ON adpr.promotedAsin = apie.asin AND adpr.market = apie.market
        WHERE
            adpr.DATE >= '{end_date}'
            AND adpr.DATE <= '{start_date}'
        GROUP BY
            adpr.market,
            CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END
    ) AS sd ON sd.parent_asins_or_asin = sp.parent_asins_or_asin AND sd.market = sp.market
    WHERE
        sp.market = '{market}'
    ORDER BY
        sp.DATE
) AS ad_order ON all_order.parent_asins_or_asin = ad_order.parent_asins_or_asin
ORDER BY
    all_order.总销售额 DESC

             """
            df = pd.read_sql(query, con=conn)
            # 计算汇总数据
            total_ad_sales = df['广告销售额'].sum()
            total_ad_cost = df['广告花费'].sum()
            total_sales = df['总销售额'].sum()

            # 计算汇总行的各项指标
            if total_sales > 0:
                ad_cost_ratio = (total_ad_cost / total_sales) * 100
                overall_acos = (total_ad_cost / total_ad_sales) * 100 if total_ad_sales > 0 else 0
                ad_sales_ratio = (total_ad_sales / total_sales) * 100
                avg_daily_sales = total_sales / 30
            else:
                ad_cost_ratio = 0
                overall_acos = 0
                ad_sales_ratio = 0
                avg_daily_sales = 0

            # 创建汇总数据行
            summary_data = {
                'list': '总计',
                '广告销售额': total_ad_sales,
                '广告花费': total_ad_cost,
                '广告花费占比': f'{ad_cost_ratio:.2f}%',
                '广告总ACOS': f'{overall_acos:.2f}%',
                '广告销售额比例': f'{ad_sales_ratio:.2f}%',
                '总销售额': round(total_sales,2),
                '日均销售额': round(avg_daily_sales,2)
            }

            # 将汇总数据行添加到 DataFrame
            summary_df = pd.DataFrame([summary_data])
            df = pd.concat([df, summary_df], ignore_index=True)

            # 保存到CSV文件
            output_filename = f'{self.brand}_{market}_listing_summary_data.csv'
            csv_path = os.path.join(get_export_path(), output_filename)
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            print("get_listing_summary_data successfully!")
            return csv_path
        except Exception as error:
            print("get_listing_summary_data Error while query data:", error)

    def get_listing_summary_data_supplier(self, market, start_date, end_date):

        try:
            conn = self.conn
            # 暂时忽略了market转化 US
            query = f"""
SELECT
    all_order.parent_asins_or_asin AS list,
    COALESCE(ad_order.广告总销售额, 0) AS 广告销售额,
    COALESCE(ad_order.广告总花费, 0) AS 广告花费,
    CONCAT(
        CASE
            WHEN ROUND((COALESCE(ad_order.广告总花费, 0) / all_order.总销售额) * 100, 2) = ROUND((COALESCE(ad_order.广告总花费, 0) / all_order.总销售额) * 100, 0)
            THEN ROUND((COALESCE(ad_order.广告总花费, 0) / all_order.总销售额) * 100, 0)
            ELSE ROUND((COALESCE(ad_order.广告总花费, 0) / all_order.总销售额) * 100, 2)
        END, '%'
    ) AS 广告花费占比,
    CONCAT(
        CASE
            WHEN ROUND(ad_order.广告总ACOS * 100, 2) = ROUND(ad_order.广告总ACOS * 100, 0)
            THEN ROUND(ad_order.广告总ACOS * 100, 0)
            ELSE ROUND(ad_order.广告总ACOS * 100, 2)
        END, '%'
    ) AS 广告总ACOS,
    CONCAT(
        CASE
            WHEN ROUND(((COALESCE(ad_order.广告总销售额, 0) / all_order.总销售额) * 100), 2) = ROUND(((COALESCE(ad_order.广告总销售额, 0) / all_order.总销售额) * 100), 0)
            THEN ROUND(((COALESCE(ad_order.广告总销售额, 0) / all_order.总销售额) * 100), 0)
            ELSE ROUND(((COALESCE(ad_order.广告总销售额, 0) / all_order.总销售额) * 100), 2)
        END, '%'
    ) AS 广告销售额比例,
    all_order.总销售额,
    ROUND(all_order.总销售额 / 30, 2) AS 日均销售额
FROM
(
		SELECT
        CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END AS parent_asins_or_asin,
        DATE(CONVERT_TZ(endTime,'+08:00', '{self.load_config_info()['timezone_offset']}')) AS event_date,
        ROUND(SUM(orderedRevenue), 2) AS 总销售额
    FROM
        amazon_vendor_get_real_time_sales_report agffa
    LEFT JOIN
        amazon_product_info_extended apie ON agffa.asin = apie.asin
    WHERE
        CONVERT_TZ(endTime,'+08:00', '{self.load_config_info()['timezone_offset']}') >= '{end_date}'
        AND CONVERT_TZ(endTime,'+08:00', '{self.load_config_info()['timezone_offset']}') < '{start_date}' + INTERVAL 1 DAY
        AND agffa.market = '{market}'
        AND apie.market = '{market}'
    GROUP BY
        agffa.market,
        CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END
) AS all_order
LEFT JOIN
(
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
                CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END AS parent_asins_or_asin,
                adpr.market,
                adpr.DATE,
                SUM(cost) AS sum_cost,
                SUM(sales7d) AS sum_sales
            FROM
                amazon_advertised_product_reports_sp adpr
            LEFT JOIN
                amazon_product_info_extended apie ON adpr.advertisedAsin = apie.asin AND adpr.market = apie.market
            WHERE
                adpr.DATE >= '{end_date}'
                AND adpr.DATE <= '{start_date}'
            GROUP BY
                adpr.market,
                CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END
        ) AS sp
    LEFT JOIN (
        SELECT
            CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END AS parent_asins_or_asin,
            adpr.market,
            adpr.DATE,
            SUM(cost) AS sum_cost,
            SUM(sales) AS sum_sales
        FROM
            amazon_advertised_product_reports_sd adpr
        LEFT JOIN
            amazon_product_info_extended apie ON adpr.promotedAsin = apie.asin AND adpr.market = apie.market
        WHERE
            adpr.DATE >= '{end_date}'
            AND adpr.DATE <= '{start_date}'
        GROUP BY
            adpr.market,
            CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END
    ) AS sd ON sd.parent_asins_or_asin = sp.parent_asins_or_asin AND sd.market = sp.market
    WHERE
        sp.market = '{market}'
    ORDER BY
        sp.DATE
) AS ad_order ON all_order.parent_asins_or_asin = ad_order.parent_asins_or_asin
ORDER BY
    all_order.总销售额 DESC

             """
            df = pd.read_sql(query, con=conn)
            # 计算汇总数据
            total_ad_sales = df['广告销售额'].sum()
            total_ad_cost = df['广告花费'].sum()
            total_sales = df['总销售额'].sum()

            # 计算汇总行的各项指标
            if total_sales > 0:
                ad_cost_ratio = (total_ad_cost / total_sales) * 100
                overall_acos = (total_ad_cost / total_ad_sales) * 100 if total_ad_sales > 0 else 0
                ad_sales_ratio = (total_ad_sales / total_sales) * 100
                avg_daily_sales = total_sales / 30
            else:
                ad_cost_ratio = 0
                overall_acos = 0
                ad_sales_ratio = 0
                avg_daily_sales = 0

            # 创建汇总数据行
            summary_data = {
                'list': '总计',
                '广告销售额': total_ad_sales,
                '广告花费': total_ad_cost,
                '广告花费占比': f'{ad_cost_ratio:.2f}%',
                '广告总ACOS': f'{overall_acos:.2f}%',
                '广告销售额比例': f'{ad_sales_ratio:.2f}%',
                '总销售额': round(total_sales, 2),
                '日均销售额': round(avg_daily_sales, 2)
            }

            # 将汇总数据行添加到 DataFrame
            summary_df = pd.DataFrame([summary_data])
            df = pd.concat([df, summary_df], ignore_index=True)

            # 保存到CSV文件
            output_filename = f'{self.brand}_{market}_listing_summary_data.csv'
            csv_path = os.path.join(get_export_path(), output_filename)
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            print("get_listing_summary_data successfully!")
            return csv_path
        except Exception as error:
            print("get_listing_summary_data Error while query data:", error)

    def get_listing_sp_summary_data(self, market,start_date,end_date):
        # 低于 平均ACOS值 30% 以上的  campaign 广告活动
        # 建议执行的操作：预算提升30%
        try:
            conn = self.conn
            # 暂时忽略了market转化 US
            query = f"""
WITH a AS (
    SELECT
        sales_channel AS 国家,
        CASE
            WHEN apie.parent_asins = '' THEN apie.asin
            ELSE apie.parent_asins
        END AS parent_asins_or_asin,
        DATE(CONVERT_TZ(purchase_date, '+08:00', '{self.load_config_info()['timezone_offset']}')) AS event_date,
        ROUND(SUM(item_price), 2) AS 总销售额
    FROM
        amazon_get_flat_file_all_orders_data_by_last_update_general agffa
    LEFT JOIN
        amazon_product_info_extended apie ON agffa.asin = apie.asin
    WHERE
        CONVERT_TZ(purchase_date, '+08:00', '{self.load_config_info()['timezone_offset']}') >= '{end_date}'
        AND CONVERT_TZ(purchase_date, '+08:00', '{self.load_config_info()['timezone_offset']}') < '{start_date}' + INTERVAL 1 DAY
        AND sales_channel = '{self.load_config_info()['sales_channel']}'
        AND apie.market = '{market}'
    GROUP BY
        sales_channel,
        CASE
            WHEN apie.parent_asins = '' THEN apie.asin
            ELSE apie.parent_asins
        END
),
b AS (
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
                CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END AS parent_asins_or_asin,
                adpr.market,
                adpr.DATE,
                SUM(cost) AS sum_cost,
                SUM(sales7d) AS sum_sales
            FROM
                amazon_advertised_product_reports_sp adpr
            LEFT JOIN
                amazon_product_info_extended apie ON adpr.advertisedAsin = apie.asin AND adpr.market = apie.market
            WHERE
                adpr.DATE >= '{end_date}'
                AND adpr.DATE <= '{start_date}'
            GROUP BY
                adpr.market,
                CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END
        ) AS sp
    LEFT JOIN (
        SELECT
            CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END AS parent_asins_or_asin,
            adpr.market,
            adpr.DATE,
            SUM(cost) AS sum_cost,
            SUM(sales) AS sum_sales
        FROM
            amazon_advertised_product_reports_sd adpr
        LEFT JOIN
            amazon_product_info_extended apie ON adpr.promotedAsin = apie.asin AND adpr.market = apie.market
        WHERE
            adpr.DATE >= '{end_date}'
            AND adpr.DATE <= '{start_date}'
        GROUP BY
            adpr.market,
            CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END
    ) AS sd ON sd.parent_asins_or_asin = sp.parent_asins_or_asin AND sd.market = sp.market
    WHERE
        sp.market = '{market}'
    ORDER BY
        sp.DATE
),
c AS (
    -- 计算广告数据
    SELECT
        sp.market AS 国家,
        sp.parent_asins_or_asin,
        sp.DATE,
        (COALESCE(sp.sum_cost, 0)) AS 广告总花费,
        (COALESCE(sp.sum_sales, 0)) AS 广告总销售额,
        ROUND(
            (COALESCE(sp.sum_cost, 0)) /
            (COALESCE(sp.sum_sales, 0)),
            4
        ) AS 广告总ACOS
    FROM
        (
            SELECT
                CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END AS parent_asins_or_asin,
                adpr.market,
                adpr.DATE,
                SUM(cost) AS sum_cost,
                SUM(sales7d) AS sum_sales
            FROM
                amazon_advertised_product_reports_sp adpr
            LEFT JOIN
                amazon_product_info_extended apie ON adpr.advertisedAsin = apie.asin AND adpr.market = apie.market
            WHERE
                adpr.DATE >= '{end_date}'
                AND adpr.DATE <= '{start_date}'
            GROUP BY
                adpr.market,
                CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END
        ) AS sp
    WHERE
        sp.market = '{market}'
    ORDER BY
        sp.DATE
)

SELECT
    a.parent_asins_or_asin AS list,
    COALESCE(b.广告总销售额, 0) AS 广告销售额,
    COALESCE(b.广告总花费, 0) AS 广告花费,
    CONCAT(
        CASE
            WHEN ROUND(b.广告总ACOS * 100, 2) = ROUND(b.广告总ACOS * 100, 0)
            THEN ROUND(b.广告总ACOS * 100, 0)
            ELSE ROUND(b.广告总ACOS * 100, 2)
        END, '%'
    ) AS ACOS,
    COALESCE(c.广告总销售额, 0) AS SP广告销售额,
    COALESCE(c.广告总花费, 0) AS SP广告花费,
    CONCAT(
        CASE
            WHEN ROUND(c.广告总ACOS * 100, 2) = ROUND(c.广告总ACOS * 100, 0)
            THEN ROUND(c.广告总ACOS * 100, 0)
            ELSE ROUND(c.广告总ACOS * 100, 2)
        END, '%'
    ) AS SP_ACOS,
    CONCAT(
        CASE
            WHEN ROUND(((COALESCE(c.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 2) = ROUND(((COALESCE(c.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 0)
            THEN ROUND(((COALESCE(c.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 0)
            ELSE ROUND(((COALESCE(c.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 2)
        END, '%'
    ) AS SP广告销售额占比
FROM a
LEFT JOIN b ON a.parent_asins_or_asin = b.parent_asins_or_asin
LEFT JOIN c ON a.parent_asins_or_asin = c.parent_asins_or_asin
ORDER BY
    COALESCE(b.广告总销售额, 0) DESC

             """
            df = pd.read_sql(query, con=conn)
            # 计算汇总数据
            total_ad_sales = df['广告销售额'].sum()
            total_ad_cost = df['广告花费'].sum()
            total_sp_sales = df['SP广告销售额'].sum()
            total_sp_cost = df['SP广告花费'].sum()

            # 计算汇总行的各项指标
            if total_sp_sales > 0:
                sp_acos = (total_sp_cost / total_sp_sales) * 100
                overall_acos = (total_ad_cost / total_ad_sales) * 100 if total_ad_sales > 0 else 0
                sp_sales_ratio = (total_sp_sales / total_ad_sales) * 100 if total_ad_sales > 0 else 0
            else:
                sp_acos = 0
                overall_acos = 0
                sp_sales_ratio = 0

            # 创建汇总数据行
            summary_data = {
                'list': '总计',
                '广告销售额': total_ad_sales,
                '广告花费': total_ad_cost,
                'ACOS': f'{overall_acos:.2f}%',
                'SP广告销售额': round(total_sp_sales,2),
                'SP广告花费': total_sp_cost,
                'SP_ACOS': f'{sp_acos:.2f}%',
                'SP广告销售额占比': f'{sp_sales_ratio:.2f}%',
            }

            # 将汇总数据行添加到 DataFrame
            summary_df = pd.DataFrame([summary_data])
            df = pd.concat([df, summary_df], ignore_index=True)

            # 保存到CSV文件
            output_filename = f'{self.brand}_{market}_listing_sp_summary_data.csv'
            csv_path = os.path.join(get_export_path(), output_filename)
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            print("get_listing_sp_summary_data successfully!")
            return csv_path
        except Exception as error:
            print("get_listing_sp_summary_data Error while query data:", error)

    def get_listing_sp_summary_data_supplier(self, market, start_date, end_date):
        # 低于 平均ACOS值 30% 以上的  campaign 广告活动
        # 建议执行的操作：预算提升30%
        try:
            conn = self.conn
            # 暂时忽略了market转化 US
            query = f"""
WITH a AS (
    SELECT
        CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END AS parent_asins_or_asin,
        DATE(CONVERT_TZ(endTime,'+08:00', '{self.load_config_info()['timezone_offset']}')) AS event_date,
        ROUND(SUM(orderedRevenue), 2) AS 总销售额
    FROM
        amazon_vendor_get_real_time_sales_report agffa
    LEFT JOIN
        amazon_product_info_extended apie ON agffa.asin = apie.asin
    WHERE
        CONVERT_TZ(endTime,'+08:00', '{self.load_config_info()['timezone_offset']}') >= '{end_date}'
        AND CONVERT_TZ(endTime,'+08:00', '{self.load_config_info()['timezone_offset']}') < '{start_date}' + INTERVAL 1 DAY
        AND agffa.market = '{market}'
        AND apie.market = '{market}'
    GROUP BY
        agffa.market,
        CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END
),
b AS (
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
                CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END AS parent_asins_or_asin,
                adpr.market,
                adpr.DATE,
                SUM(cost) AS sum_cost,
                SUM(sales7d) AS sum_sales
            FROM
                amazon_advertised_product_reports_sp adpr
            LEFT JOIN
                amazon_product_info_extended apie ON adpr.advertisedAsin = apie.asin AND adpr.market = apie.market
            WHERE
                adpr.DATE >= '{end_date}'
                AND adpr.DATE <= '{start_date}'
            GROUP BY
                adpr.market,
                CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END
        ) AS sp
    LEFT JOIN (
        SELECT
            CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END AS parent_asins_or_asin,
            adpr.market,
            adpr.DATE,
            SUM(cost) AS sum_cost,
            SUM(sales) AS sum_sales
        FROM
            amazon_advertised_product_reports_sd adpr
        LEFT JOIN
            amazon_product_info_extended apie ON adpr.promotedAsin = apie.asin AND adpr.market = apie.market
        WHERE
            adpr.DATE >= '{end_date}'
            AND adpr.DATE <= '{start_date}'
        GROUP BY
            adpr.market,
            CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END
    ) AS sd ON sd.parent_asins_or_asin = sp.parent_asins_or_asin AND sd.market = sp.market
    WHERE
        sp.market = '{market}'
    ORDER BY
        sp.DATE
),
c AS (
    -- 计算广告数据
    SELECT
        sp.market AS 国家,
        sp.parent_asins_or_asin,
        sp.DATE,
        (COALESCE(sp.sum_cost, 0)) AS 广告总花费,
        (COALESCE(sp.sum_sales, 0)) AS 广告总销售额,
        ROUND(
            (COALESCE(sp.sum_cost, 0)) /
            (COALESCE(sp.sum_sales, 0)),
            4
        ) AS 广告总ACOS
    FROM
        (
            SELECT
                CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END AS parent_asins_or_asin,
                adpr.market,
                adpr.DATE,
                SUM(cost) AS sum_cost,
                SUM(sales7d) AS sum_sales
            FROM
                amazon_advertised_product_reports_sp adpr
            LEFT JOIN
                amazon_product_info_extended apie ON adpr.advertisedAsin = apie.asin AND adpr.market = apie.market
            WHERE
                adpr.DATE >= '{end_date}'
                AND adpr.DATE <= '{start_date}'
            GROUP BY
                adpr.market,
                CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END
        ) AS sp
    WHERE
        sp.market = '{market}'
    ORDER BY
        sp.DATE
)

SELECT
    a.parent_asins_or_asin AS list,
    COALESCE(b.广告总销售额, 0) AS 广告销售额,
    COALESCE(b.广告总花费, 0) AS 广告花费,
    CONCAT(
        CASE
            WHEN ROUND(b.广告总ACOS * 100, 2) = ROUND(b.广告总ACOS * 100, 0)
            THEN ROUND(b.广告总ACOS * 100, 0)
            ELSE ROUND(b.广告总ACOS * 100, 2)
        END, '%'
    ) AS ACOS,
    COALESCE(c.广告总销售额, 0) AS SP广告销售额,
    COALESCE(c.广告总花费, 0) AS SP广告花费,
    CONCAT(
        CASE
            WHEN ROUND(c.广告总ACOS * 100, 2) = ROUND(c.广告总ACOS * 100, 0)
            THEN ROUND(c.广告总ACOS * 100, 0)
            ELSE ROUND(c.广告总ACOS * 100, 2)
        END, '%'
    ) AS SP_ACOS,
    CONCAT(
        CASE
            WHEN ROUND(((COALESCE(c.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 2) = ROUND(((COALESCE(c.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 0)
            THEN ROUND(((COALESCE(c.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 0)
            ELSE ROUND(((COALESCE(c.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 2)
        END, '%'
    ) AS SP广告销售额占比
FROM a
LEFT JOIN b ON a.parent_asins_or_asin = b.parent_asins_or_asin
LEFT JOIN c ON a.parent_asins_or_asin = c.parent_asins_or_asin
ORDER BY
    COALESCE(b.广告总销售额, 0) DESC

             """
            df = pd.read_sql(query, con=conn)
            # 计算汇总数据
            total_ad_sales = df['广告销售额'].sum()
            total_ad_cost = df['广告花费'].sum()
            total_sp_sales = df['SP广告销售额'].sum()
            total_sp_cost = df['SP广告花费'].sum()

            # 计算汇总行的各项指标
            if total_sp_sales > 0:
                sp_acos = (total_sp_cost / total_sp_sales) * 100
                overall_acos = (total_ad_cost / total_ad_sales) * 100 if total_ad_sales > 0 else 0
                sp_sales_ratio = (total_sp_sales / total_ad_sales) * 100 if total_ad_sales > 0 else 0
            else:
                sp_acos = 0
                overall_acos = 0
                sp_sales_ratio = 0

            # 创建汇总数据行
            summary_data = {
                'list': '总计',
                '广告销售额': total_ad_sales,
                '广告花费': total_ad_cost,
                'ACOS': f'{overall_acos:.2f}%',
                'SP广告销售额': round(total_sp_sales, 2),
                'SP广告花费': total_sp_cost,
                'SP_ACOS': f'{sp_acos:.2f}%',
                'SP广告销售额占比': f'{sp_sales_ratio:.2f}%',
            }

            # 将汇总数据行添加到 DataFrame
            summary_df = pd.DataFrame([summary_data])
            df = pd.concat([df, summary_df], ignore_index=True)

            # 保存到CSV文件
            output_filename = f'{self.brand}_{market}_listing_sp_summary_data.csv'
            csv_path = os.path.join(get_export_path(), output_filename)
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            print("get_listing_sp_summary_data successfully!")
            return csv_path
        except Exception as error:
            print("get_listing_sp_summary_data Error while query data:", error)


    def get_listing_sp_specific_data(self, market,start_date,end_date):
        # 低于 平均ACOS值 30% 以上的  campaign 广告活动
        # 建议执行的操作：预算提升30%
        try:
            conn = self.conn
            # 暂时忽略了market转化 US
            query = f"""
WITH a AS (
    SELECT
        sales_channel AS 国家,
        CASE
            WHEN apie.parent_asins = '' THEN apie.asin
            ELSE apie.parent_asins
        END AS parent_asins_or_asin,
        DATE(CONVERT_TZ(purchase_date, '+08:00', '{self.load_config_info()['timezone_offset']}')) AS event_date,
        ROUND(SUM(item_price), 2) AS 总销售额
    FROM
        amazon_get_flat_file_all_orders_data_by_last_update_general agffa
    LEFT JOIN
        amazon_product_info_extended apie ON agffa.asin = apie.asin
    WHERE
        CONVERT_TZ(purchase_date, '+08:00', '{self.load_config_info()['timezone_offset']}') >= '{end_date}'
        AND CONVERT_TZ(purchase_date, '+08:00', '{self.load_config_info()['timezone_offset']}') < '{start_date}' + INTERVAL 1 DAY
        AND sales_channel = '{self.load_config_info()['sales_channel']}'
        AND apie.market = '{market}'
    GROUP BY
        sales_channel,
        CASE
            WHEN apie.parent_asins = '' THEN apie.asin
            ELSE apie.parent_asins
        END
),
b AS (
    SELECT
        sp.market AS 国家,
        sp.parent_asins_or_asin,
        sp.DATE,
        COALESCE(sp.sum_cost, 0) AS 广告总花费,
        COALESCE(sp.sum_sales, 0) AS 广告总销售额,
        ROUND(COALESCE(sp.sum_cost, 0) / COALESCE(sp.sum_sales, 0), 4) AS 广告总ACOS
    FROM (
        SELECT
            CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END AS parent_asins_or_asin,
            adpr.market,
            adpr.DATE,
            SUM(cost) AS sum_cost,
            SUM(sales7d) AS sum_sales
        FROM
            amazon_advertised_product_reports_sp adpr
        LEFT JOIN
            amazon_product_info_extended apie ON adpr.advertisedAsin = apie.asin AND adpr.market = apie.market
        WHERE
            adpr.DATE >= '{end_date}'
            AND adpr.DATE <= '{start_date}'
        GROUP BY
            adpr.market,
            CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END
    ) AS sp
    WHERE
        sp.market = '{market}'
    ORDER BY
        sp.DATE
),
c AS (
    SELECT
        sp.market AS 国家,
        sp.parent_asins_or_asin,
        sp.DATE,
        COALESCE(sp.sum_cost, 0) AS 广告总花费,
        COALESCE(sp.sum_sales, 0) AS 广告总销售额,
        ROUND(COALESCE(sp.sum_cost, 0) / COALESCE(sp.sum_sales, 0), 4) AS 广告总ACOS
    FROM (
        SELECT
            CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END AS parent_asins_or_asin,
            adpr.market,
            adpr.DATE,
            SUM(cost) AS sum_cost,
            SUM(sales7d) AS sum_sales
        FROM
            amazon_advertised_product_reports_sp adpr
        LEFT JOIN
            amazon_product_info_extended apie ON adpr.advertisedAsin = apie.asin AND adpr.market = apie.market
        LEFT JOIN
            amazon_campaigns_list_sp acl ON adpr.campaignId = acl.campaignId
        WHERE
            adpr.DATE >= '{end_date}'
            AND adpr.DATE <= '{start_date}'
            AND acl.targetingType = 'MANUAL'
        GROUP BY
            adpr.market,
            CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END
    ) AS sp
    WHERE
        sp.market = '{market}'
    ORDER BY
        sp.DATE
),
d AS (
    SELECT
        sp.market AS 国家,
        sp.parent_asins_or_asin,
        sp.DATE,
        COALESCE(sp.sum_cost, 0) AS 广告总花费,
        COALESCE(sp.sum_sales, 0) AS 广告总销售额,
        ROUND(COALESCE(sp.sum_cost, 0) / COALESCE(sp.sum_sales, 0), 4) AS 广告总ACOS
    FROM (
        SELECT
            CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END AS parent_asins_or_asin,
            adpr.market,
            adpr.DATE,
            SUM(cost) AS sum_cost,
            SUM(sales7d) AS sum_sales
        FROM
            amazon_advertised_product_reports_sp adpr
        LEFT JOIN
            amazon_product_info_extended apie ON adpr.advertisedAsin = apie.asin AND adpr.market = apie.market
        LEFT JOIN
            amazon_campaigns_list_sp acl ON adpr.campaignId = acl.campaignId
        WHERE
            adpr.DATE >= '{end_date}'
            AND adpr.DATE <= '{start_date}'
            AND acl.targetingType = 'AUTO'
        GROUP BY
            adpr.market,
            CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END
    ) AS sp
    WHERE
        sp.market = '{market}'
    ORDER BY
        sp.DATE
)
SELECT
    a.parent_asins_or_asin AS list,
    COALESCE(b.广告总销售额, 0) AS SP广告销售额,
    COALESCE(b.广告总花费, 0) AS SP广告花费,
    CONCAT(
        CASE
            WHEN ROUND(b.广告总ACOS * 100, 2) = ROUND(b.广告总ACOS * 100, 0) THEN ROUND(b.广告总ACOS * 100, 0)
            ELSE ROUND(b.广告总ACOS * 100, 2)
        END, '%'
    ) AS SP_ACOS,
    COALESCE(c.广告总销售额, 0) AS SP手动广告销售额,
    COALESCE(c.广告总花费, 0) AS SP手动广告花费,
    CONCAT(
        CASE
            WHEN ROUND(c.广告总ACOS * 100, 2) = ROUND(c.广告总ACOS * 100, 0) THEN ROUND(c.广告总ACOS * 100, 0)
            ELSE ROUND(c.广告总ACOS * 100, 2)
        END, '%'
    ) AS SP手动_ACOS,
    COALESCE(d.广告总销售额, 0) AS SP自动广告销售额,
    COALESCE(d.广告总花费, 0) AS SP自动广告花费,
    CONCAT(
        CASE
            WHEN ROUND(d.广告总ACOS * 100, 2) = ROUND(d.广告总ACOS * 100, 0) THEN ROUND(d.广告总ACOS * 100, 0)
            ELSE ROUND(d.广告总ACOS * 100, 2)
        END, '%'
    ) AS SP自动_ACOS,
    CONCAT(
        CASE
            WHEN ROUND(((COALESCE(c.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 2) = ROUND(((COALESCE(c.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 0)
            THEN ROUND(((COALESCE(c.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 0)
            ELSE ROUND(((COALESCE(c.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 2)
        END, '%'
    ) AS SP手动广告销售额占比
FROM a
LEFT JOIN b ON a.parent_asins_or_asin = b.parent_asins_or_asin
LEFT JOIN c ON a.parent_asins_or_asin = c.parent_asins_or_asin
LEFT JOIN d ON a.parent_asins_or_asin = d.parent_asins_or_asin
ORDER BY
    COALESCE(b.广告总销售额, 0) DESC

             """
            df = pd.read_sql(query, con=conn)
            # 计算汇总数据
            total_sp_sales = df['SP广告销售额'].sum()
            total_sp_cost = df['SP广告花费'].sum()
            total_sp_manual_sales = df['SP手动广告销售额'].sum()
            total_sp_manual_cost = df['SP手动广告花费'].sum()
            total_sp_auto_sales = df['SP自动广告销售额'].sum()
            total_sp_auto_cost = df['SP自动广告花费'].sum()

            # 计算汇总行的各项指标
            if total_sp_sales > 0:
                sp_acos = (total_sp_cost / total_sp_sales) * 100
                sp_manual_acos = (total_sp_manual_cost / total_sp_manual_sales) * 100 if total_sp_manual_sales > 0 else 0
                sp_auto_acos = (total_sp_auto_cost / total_sp_auto_sales) * 100 if total_sp_auto_sales > 0 else 0
                sp_manual_sales_ratio = (total_sp_manual_sales / total_sp_sales) * 100
            else:
                sp_acos = 0
                sp_manual_acos = 0
                sp_auto_acos = 0
                sp_manual_sales_ratio = 0

            # 创建汇总数据行
            summary_data = {
                'list': '总计',
                'SP广告销售额': round(total_sp_sales,2),
                'SP广告花费': total_sp_cost,
                'SP_ACOS': f'{sp_acos:.2f}%',
                'SP手动广告销售额': round(total_sp_manual_sales,2),
                'SP手动广告花费': total_sp_manual_cost,
                'SP手动_ACOS': f'{sp_manual_acos:.2f}%',
                'SP自动广告销售额': round(total_sp_auto_sales, 2),
                'SP自动广告花费': total_sp_auto_cost,
                'SP自动_ACOS': f'{sp_auto_acos:.2f}%',
                'SP手动广告销售额占比': f'{sp_manual_sales_ratio:.2f}%',
            }

            # 将汇总数据行添加到 DataFrame
            summary_df = pd.DataFrame([summary_data])
            df = pd.concat([df, summary_df], ignore_index=True)

            # 保存到CSV文件
            output_filename = f'{self.brand}_{market}_listing_sp_specific_data.csv'
            csv_path = os.path.join(get_export_path(), output_filename)
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            print("get_listing_sp_specific_data successfully!")
            return csv_path
        except Exception as error:
            print("get_listing_sp_specific_data Error while query data:", error)

    def get_listing_sp_specific_data_supplier(self, market, start_date, end_date):
        # 低于 平均ACOS值 30% 以上的  campaign 广告活动
        # 建议执行的操作：预算提升30%
        try:
            conn = self.conn
            # 暂时忽略了market转化 US
            query = f"""
WITH a AS (
    SELECT
        CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END AS parent_asins_or_asin,
        DATE(CONVERT_TZ(endTime,'+08:00', '{self.load_config_info()['timezone_offset']}')) AS event_date,
        ROUND(SUM(orderedRevenue), 2) AS 总销售额
    FROM
        amazon_vendor_get_real_time_sales_report agffa
    LEFT JOIN
        amazon_product_info_extended apie ON agffa.asin = apie.asin
    WHERE
        CONVERT_TZ(endTime,'+08:00', '{self.load_config_info()['timezone_offset']}') >= '{end_date}'
        AND CONVERT_TZ(endTime,'+08:00', '{self.load_config_info()['timezone_offset']}') < '{start_date}' + INTERVAL 1 DAY
        AND agffa.market = '{market}'
        AND apie.market = '{market}'
    GROUP BY
        agffa.market,
        CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END
),
b AS (
    SELECT
        sp.market AS 国家,
        sp.parent_asins_or_asin,
        sp.DATE,
        COALESCE(sp.sum_cost, 0) AS 广告总花费,
        COALESCE(sp.sum_sales, 0) AS 广告总销售额,
        ROUND(COALESCE(sp.sum_cost, 0) / COALESCE(sp.sum_sales, 0), 4) AS 广告总ACOS
    FROM (
        SELECT
            CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END AS parent_asins_or_asin,
            adpr.market,
            adpr.DATE,
            SUM(cost) AS sum_cost,
            SUM(sales7d) AS sum_sales
        FROM
            amazon_advertised_product_reports_sp adpr
        LEFT JOIN
            amazon_product_info_extended apie ON adpr.advertisedAsin = apie.asin AND adpr.market = apie.market
        WHERE
            adpr.DATE >= '{end_date}'
            AND adpr.DATE <= '{start_date}'
        GROUP BY
            adpr.market,
            CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END
    ) AS sp
    WHERE
        sp.market = '{market}'
    ORDER BY
        sp.DATE
),
c AS (
    SELECT
        sp.market AS 国家,
        sp.parent_asins_or_asin,
        sp.DATE,
        COALESCE(sp.sum_cost, 0) AS 广告总花费,
        COALESCE(sp.sum_sales, 0) AS 广告总销售额,
        ROUND(COALESCE(sp.sum_cost, 0) / COALESCE(sp.sum_sales, 0), 4) AS 广告总ACOS
    FROM (
        SELECT
            CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END AS parent_asins_or_asin,
            adpr.market,
            adpr.DATE,
            SUM(cost) AS sum_cost,
            SUM(sales7d) AS sum_sales
        FROM
            amazon_advertised_product_reports_sp adpr
        LEFT JOIN
            amazon_product_info_extended apie ON adpr.advertisedAsin = apie.asin AND adpr.market = apie.market
        LEFT JOIN
            amazon_campaigns_list_sp acl ON adpr.campaignId = acl.campaignId
        WHERE
            adpr.DATE >= '{end_date}'
            AND adpr.DATE <= '{start_date}'
            AND acl.targetingType = 'MANUAL'
        GROUP BY
            adpr.market,
            CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END
    ) AS sp
    WHERE
        sp.market = '{market}'
    ORDER BY
        sp.DATE
),
d AS (
    SELECT
        sp.market AS 国家,
        sp.parent_asins_or_asin,
        sp.DATE,
        COALESCE(sp.sum_cost, 0) AS 广告总花费,
        COALESCE(sp.sum_sales, 0) AS 广告总销售额,
        ROUND(COALESCE(sp.sum_cost, 0) / COALESCE(sp.sum_sales, 0), 4) AS 广告总ACOS
    FROM (
        SELECT
            CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END AS parent_asins_or_asin,
            adpr.market,
            adpr.DATE,
            SUM(cost) AS sum_cost,
            SUM(sales7d) AS sum_sales
        FROM
            amazon_advertised_product_reports_sp adpr
        LEFT JOIN
            amazon_product_info_extended apie ON adpr.advertisedAsin = apie.asin AND adpr.market = apie.market
        LEFT JOIN
            amazon_campaigns_list_sp acl ON adpr.campaignId = acl.campaignId
        WHERE
            adpr.DATE >= '{end_date}'
            AND adpr.DATE <= '{start_date}'
            AND acl.targetingType = 'AUTO'
        GROUP BY
            adpr.market,
            CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END
    ) AS sp
    WHERE
        sp.market = '{market}'
    ORDER BY
        sp.DATE
)
SELECT
    a.parent_asins_or_asin AS list,
    COALESCE(b.广告总销售额, 0) AS SP广告销售额,
    COALESCE(b.广告总花费, 0) AS SP广告花费,
    CONCAT(
        CASE
            WHEN ROUND(b.广告总ACOS * 100, 2) = ROUND(b.广告总ACOS * 100, 0) THEN ROUND(b.广告总ACOS * 100, 0)
            ELSE ROUND(b.广告总ACOS * 100, 2)
        END, '%'
    ) AS SP_ACOS,
    COALESCE(c.广告总销售额, 0) AS SP手动广告销售额,
    COALESCE(c.广告总花费, 0) AS SP手动广告花费,
    CONCAT(
        CASE
            WHEN ROUND(c.广告总ACOS * 100, 2) = ROUND(c.广告总ACOS * 100, 0) THEN ROUND(c.广告总ACOS * 100, 0)
            ELSE ROUND(c.广告总ACOS * 100, 2)
        END, '%'
    ) AS SP手动_ACOS,
    COALESCE(d.广告总销售额, 0) AS SP自动广告销售额,
    COALESCE(d.广告总花费, 0) AS SP自动广告花费,
    CONCAT(
        CASE
            WHEN ROUND(d.广告总ACOS * 100, 2) = ROUND(d.广告总ACOS * 100, 0) THEN ROUND(d.广告总ACOS * 100, 0)
            ELSE ROUND(d.广告总ACOS * 100, 2)
        END, '%'
    ) AS SP自动_ACOS,
    CONCAT(
        CASE
            WHEN ROUND(((COALESCE(c.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 2) = ROUND(((COALESCE(c.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 0)
            THEN ROUND(((COALESCE(c.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 0)
            ELSE ROUND(((COALESCE(c.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 2)
        END, '%'
    ) AS SP手动广告销售额占比
FROM a
LEFT JOIN b ON a.parent_asins_or_asin = b.parent_asins_or_asin
LEFT JOIN c ON a.parent_asins_or_asin = c.parent_asins_or_asin
LEFT JOIN d ON a.parent_asins_or_asin = d.parent_asins_or_asin
ORDER BY
    COALESCE(b.广告总销售额, 0) DESC

             """
            df = pd.read_sql(query, con=conn)
            # 计算汇总数据
            total_sp_sales = df['SP广告销售额'].sum()
            total_sp_cost = df['SP广告花费'].sum()
            total_sp_manual_sales = df['SP手动广告销售额'].sum()
            total_sp_manual_cost = df['SP手动广告花费'].sum()
            total_sp_auto_sales = df['SP自动广告销售额'].sum()
            total_sp_auto_cost = df['SP自动广告花费'].sum()

            # 计算汇总行的各项指标
            if total_sp_sales > 0:
                sp_acos = (total_sp_cost / total_sp_sales) * 100
                sp_manual_acos = (
                                         total_sp_manual_cost / total_sp_manual_sales) * 100 if total_sp_manual_sales > 0 else 0
                sp_auto_acos = (total_sp_auto_cost / total_sp_auto_sales) * 100 if total_sp_auto_sales > 0 else 0
                sp_manual_sales_ratio = (total_sp_manual_sales / total_sp_sales) * 100
            else:
                sp_acos = 0
                sp_manual_acos = 0
                sp_auto_acos = 0
                sp_manual_sales_ratio = 0

            # 创建汇总数据行
            summary_data = {
                'list': '总计',
                'SP广告销售额': round(total_sp_sales, 2),
                'SP广告花费': total_sp_cost,
                'SP_ACOS': f'{sp_acos:.2f}%',
                'SP手动广告销售额': round(total_sp_manual_sales, 2),
                'SP手动广告花费': total_sp_manual_cost,
                'SP手动_ACOS': f'{sp_manual_acos:.2f}%',
                'SP自动广告销售额': round(total_sp_auto_sales, 2),
                'SP自动广告花费': total_sp_auto_cost,
                'SP自动_ACOS': f'{sp_auto_acos:.2f}%',
                'SP手动广告销售额占比': f'{sp_manual_sales_ratio:.2f}%',
            }

            # 将汇总数据行添加到 DataFrame
            summary_df = pd.DataFrame([summary_data])
            df = pd.concat([df, summary_df], ignore_index=True)

            # 保存到CSV文件
            output_filename = f'{self.brand}_{market}_listing_sp_specific_data.csv'
            csv_path = os.path.join(get_export_path(), output_filename)
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            print("get_listing_sp_specific_data successfully!")
            return csv_path
        except Exception as error:
            print("get_listing_sp_specific_data Error while query data:", error)


    def get_listing_sd_summary_data(self, market,start_date,end_date):
        # 低于 平均ACOS值 30% 以上的  campaign 广告活动
        # 建议执行的操作：预算提升30%
        try:
            conn = self.conn
            # 暂时忽略了market转化 US
            query = f"""
WITH a AS (
    SELECT
        sales_channel AS 国家,
        CASE
            WHEN apie.parent_asins = '' THEN apie.asin
            ELSE apie.parent_asins
        END AS parent_asins_or_asin,
        DATE(CONVERT_TZ(purchase_date, '+08:00', '{self.load_config_info()['timezone_offset']}')) AS event_date,
        ROUND(SUM(item_price), 2) AS 总销售额
    FROM
        amazon_get_flat_file_all_orders_data_by_last_update_general agffa
    LEFT JOIN
        amazon_product_info_extended apie ON agffa.asin = apie.asin
    WHERE
        CONVERT_TZ(purchase_date, '+08:00', '{self.load_config_info()['timezone_offset']}') >= '{end_date}'
        AND CONVERT_TZ(purchase_date, '+08:00', '{self.load_config_info()['timezone_offset']}') < '{start_date}' + INTERVAL 1 DAY
        AND sales_channel = '{self.load_config_info()['sales_channel']}'
        AND apie.market = '{market}'
    GROUP BY
        sales_channel,
        CASE
            WHEN apie.parent_asins = '' THEN apie.asin
            ELSE apie.parent_asins
        END
),
b AS (
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
                CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END AS parent_asins_or_asin,
                adpr.market,
                adpr.DATE,
                SUM(cost) AS sum_cost,
                SUM(sales7d) AS sum_sales
            FROM
                amazon_advertised_product_reports_sp adpr
            LEFT JOIN
                amazon_product_info_extended apie ON adpr.advertisedAsin = apie.asin AND adpr.market = apie.market
            WHERE
                adpr.DATE >= '{end_date}'
                AND adpr.DATE <= '{start_date}'
            GROUP BY
                adpr.market,
                CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END
        ) AS sp
    LEFT JOIN (
        SELECT
            CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END AS parent_asins_or_asin,
            adpr.market,
            adpr.DATE,
            SUM(cost) AS sum_cost,
            SUM(sales) AS sum_sales
        FROM
            amazon_advertised_product_reports_sd adpr
        LEFT JOIN
            amazon_product_info_extended apie ON adpr.promotedAsin = apie.asin AND adpr.market = apie.market
        WHERE
            adpr.DATE >= '{end_date}'
            AND adpr.DATE <= '{start_date}'
        GROUP BY
            adpr.market,
            CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END
    ) AS sd ON sd.parent_asins_or_asin = sp.parent_asins_or_asin AND sd.market = sp.market
    WHERE
        sp.market = '{market}'
    ORDER BY
        sp.DATE
),
c AS (
    -- 计算广告数据
    SELECT
        sp.market AS 国家,
        sp.parent_asins_or_asin,
        sp.DATE,
        (COALESCE(sp.sum_cost, 0)) AS 广告总花费,
        (COALESCE(sp.sum_sales, 0)) AS 广告总销售额,
        ROUND(
            (COALESCE(sp.sum_cost, 0)) /
            (COALESCE(sp.sum_sales, 0)),
            4
        ) AS 广告总ACOS
    FROM
        (
            SELECT
                CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END AS parent_asins_or_asin,
                adpr.market,
                adpr.DATE,
                SUM(cost) AS sum_cost,
                SUM(sales) AS sum_sales
            FROM
                amazon_advertised_product_reports_sd adpr
            LEFT JOIN
                amazon_product_info_extended apie ON adpr.promotedAsin = apie.asin AND adpr.market = apie.market
            WHERE
                adpr.DATE >= '{end_date}'
                AND adpr.DATE <= '{start_date}'
            GROUP BY
                adpr.market,
                CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END
        ) AS sp
    WHERE
        sp.market = '{market}'
    ORDER BY
        sp.DATE
)
SELECT
    a.parent_asins_or_asin AS list,
    COALESCE(b.广告总销售额, 0) AS 广告销售额,
    COALESCE(b.广告总花费, 0) AS 广告花费,
    CONCAT(
        CASE
            WHEN ROUND(b.广告总ACOS * 100, 2) = ROUND(b.广告总ACOS * 100, 0) THEN ROUND(b.广告总ACOS * 100, 0)
            ELSE ROUND(b.广告总ACOS * 100, 2)
        END, '%'
    ) AS ACOS,
    COALESCE(c.广告总销售额, 0) AS SD广告销售额,
    COALESCE(c.广告总花费, 0) AS SD广告花费,
    CONCAT(
        CASE
            WHEN ROUND(c.广告总ACOS * 100, 2) = ROUND(c.广告总ACOS * 100, 0) THEN ROUND(c.广告总ACOS * 100, 0)
            ELSE ROUND(c.广告总ACOS * 100, 2)
        END, '%'
    ) AS SD_ACOS,
    CONCAT(
        CASE
            WHEN ROUND(((COALESCE(c.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 2) = ROUND(((COALESCE(c.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 0)
            THEN ROUND(((COALESCE(c.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 0)
            ELSE ROUND(((COALESCE(c.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 2)
        END, '%'
    ) AS SD广告销售额占比
FROM a
LEFT JOIN b ON a.parent_asins_or_asin = b.parent_asins_or_asin
LEFT JOIN c ON a.parent_asins_or_asin = c.parent_asins_or_asin
ORDER BY
    COALESCE(b.广告总销售额, 0) DESC

             """
            df = pd.read_sql(query, con=conn)
            # 计算汇总数据
            total_ad_sales = df['广告销售额'].sum()
            total_ad_cost = df['广告花费'].sum()
            total_sp_sales = df['SD广告销售额'].sum()
            total_sp_cost = df['SD广告花费'].sum()

            # 计算汇总行的各项指标
            if total_sp_sales > 0:
                sp_acos = (total_sp_cost / total_sp_sales) * 100
                overall_acos = (total_ad_cost / total_ad_sales) * 100 if total_ad_sales > 0 else 0
                sp_sales_ratio = (total_sp_sales / total_ad_sales) * 100 if total_ad_sales > 0 else 0
            else:
                sp_acos = 0
                overall_acos = 0
                sp_sales_ratio = 0

            # 创建汇总数据行
            summary_data = {
                'list': '总计',
                '广告销售额': total_ad_sales,
                '广告花费': total_ad_cost,
                'ACOS': f'{overall_acos:.2f}%',
                'SD广告销售额': round(total_sp_sales,2),
                'SD广告花费': total_sp_cost,
                'SD_ACOS': f'{sp_acos:.2f}%',
                'SD广告销售额占比': f'{sp_sales_ratio:.2f}%',
            }

            # 将汇总数据行添加到 DataFrame
            summary_df = pd.DataFrame([summary_data])
            df = pd.concat([df, summary_df], ignore_index=True)

            # 保存到CSV文件
            output_filename = f'{self.brand}_{market}_listing_sd_summary_data.csv'
            csv_path = os.path.join(get_export_path(), output_filename)
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            print("get_listing_sd_summary_data successfully!")
            return csv_path
        except Exception as error:
            print("get_listing_sd_summary_data Error while query data:", error)

    def get_listing_sd_summary_data_supplier(self, market, start_date, end_date):
        # 低于 平均ACOS值 30% 以上的  campaign 广告活动
        # 建议执行的操作：预算提升30%
        try:
            conn = self.conn
            # 暂时忽略了market转化 US
            query = f"""
WITH a AS (
    SELECT
        CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END AS parent_asins_or_asin,
        DATE(CONVERT_TZ(endTime,'+08:00', '{self.load_config_info()['timezone_offset']}')) AS event_date,
        ROUND(SUM(orderedRevenue), 2) AS 总销售额
    FROM
        amazon_vendor_get_real_time_sales_report agffa
    LEFT JOIN
        amazon_product_info_extended apie ON agffa.asin = apie.asin
    WHERE
        CONVERT_TZ(endTime,'+08:00', '{self.load_config_info()['timezone_offset']}') >= '{end_date}'
        AND CONVERT_TZ(endTime,'+08:00', '{self.load_config_info()['timezone_offset']}') < '{start_date}' + INTERVAL 1 DAY
        AND agffa.market = '{market}'
        AND apie.market = '{market}'
    GROUP BY
        agffa.market,
        CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END
),
b AS (
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
                CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END AS parent_asins_or_asin,
                adpr.market,
                adpr.DATE,
                SUM(cost) AS sum_cost,
                SUM(sales7d) AS sum_sales
            FROM
                amazon_advertised_product_reports_sp adpr
            LEFT JOIN
                amazon_product_info_extended apie ON adpr.advertisedAsin = apie.asin AND adpr.market = apie.market
            WHERE
                adpr.DATE >= '{end_date}'
                AND adpr.DATE <= '{start_date}'
            GROUP BY
                adpr.market,
                CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END
        ) AS sp
    LEFT JOIN (
        SELECT
            CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END AS parent_asins_or_asin,
            adpr.market,
            adpr.DATE,
            SUM(cost) AS sum_cost,
            SUM(sales) AS sum_sales
        FROM
            amazon_advertised_product_reports_sd adpr
        LEFT JOIN
            amazon_product_info_extended apie ON adpr.promotedAsin = apie.asin AND adpr.market = apie.market
        WHERE
            adpr.DATE >= '{end_date}'
            AND adpr.DATE <= '{start_date}'
        GROUP BY
            adpr.market,
            CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END
    ) AS sd ON sd.parent_asins_or_asin = sp.parent_asins_or_asin AND sd.market = sp.market
    WHERE
        sp.market = '{market}'
    ORDER BY
        sp.DATE
),
c AS (
    -- 计算广告数据
    SELECT
        sp.market AS 国家,
        sp.parent_asins_or_asin,
        sp.DATE,
        (COALESCE(sp.sum_cost, 0)) AS 广告总花费,
        (COALESCE(sp.sum_sales, 0)) AS 广告总销售额,
        ROUND(
            (COALESCE(sp.sum_cost, 0)) /
            (COALESCE(sp.sum_sales, 0)),
            4
        ) AS 广告总ACOS
    FROM
        (
            SELECT
                CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END AS parent_asins_or_asin,
                adpr.market,
                adpr.DATE,
                SUM(cost) AS sum_cost,
                SUM(sales) AS sum_sales
            FROM
                amazon_advertised_product_reports_sd adpr
            LEFT JOIN
                amazon_product_info_extended apie ON adpr.promotedAsin = apie.asin AND adpr.market = apie.market
            WHERE
                adpr.DATE >= '{end_date}'
                AND adpr.DATE <= '{start_date}'
            GROUP BY
                adpr.market,
                CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END
        ) AS sp
    WHERE
        sp.market = '{market}'
    ORDER BY
        sp.DATE
)
SELECT
    a.parent_asins_or_asin AS list,
    COALESCE(b.广告总销售额, 0) AS 广告销售额,
    COALESCE(b.广告总花费, 0) AS 广告花费,
    CONCAT(
        CASE
            WHEN ROUND(b.广告总ACOS * 100, 2) = ROUND(b.广告总ACOS * 100, 0) THEN ROUND(b.广告总ACOS * 100, 0)
            ELSE ROUND(b.广告总ACOS * 100, 2)
        END, '%'
    ) AS ACOS,
    COALESCE(c.广告总销售额, 0) AS SD广告销售额,
    COALESCE(c.广告总花费, 0) AS SD广告花费,
    CONCAT(
        CASE
            WHEN ROUND(c.广告总ACOS * 100, 2) = ROUND(c.广告总ACOS * 100, 0) THEN ROUND(c.广告总ACOS * 100, 0)
            ELSE ROUND(c.广告总ACOS * 100, 2)
        END, '%'
    ) AS SD_ACOS,
    CONCAT(
        CASE
            WHEN ROUND(((COALESCE(c.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 2) = ROUND(((COALESCE(c.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 0)
            THEN ROUND(((COALESCE(c.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 0)
            ELSE ROUND(((COALESCE(c.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 2)
        END, '%'
    ) AS SD广告销售额占比
FROM a
LEFT JOIN b ON a.parent_asins_or_asin = b.parent_asins_or_asin
LEFT JOIN c ON a.parent_asins_or_asin = c.parent_asins_or_asin
ORDER BY
    COALESCE(b.广告总销售额, 0) DESC

             """
            df = pd.read_sql(query, con=conn)
            # 计算汇总数据
            total_ad_sales = df['广告销售额'].sum()
            total_ad_cost = df['广告花费'].sum()
            total_sp_sales = df['SD广告销售额'].sum()
            total_sp_cost = df['SD广告花费'].sum()

            # 计算汇总行的各项指标
            if total_sp_sales > 0:
                sp_acos = (total_sp_cost / total_sp_sales) * 100
                overall_acos = (total_ad_cost / total_ad_sales) * 100 if total_ad_sales > 0 else 0
                sp_sales_ratio = (total_sp_sales / total_ad_sales) * 100 if total_ad_sales > 0 else 0
            else:
                sp_acos = 0
                overall_acos = 0
                sp_sales_ratio = 0

            # 创建汇总数据行
            summary_data = {
                'list': '总计',
                '广告销售额': total_ad_sales,
                '广告花费': total_ad_cost,
                'ACOS': f'{overall_acos:.2f}%',
                'SD广告销售额': round(total_sp_sales, 2),
                'SD广告花费': total_sp_cost,
                'SD_ACOS': f'{sp_acos:.2f}%',
                'SD广告销售额占比': f'{sp_sales_ratio:.2f}%',
            }

            # 将汇总数据行添加到 DataFrame
            summary_df = pd.DataFrame([summary_data])
            df = pd.concat([df, summary_df], ignore_index=True)

            # 保存到CSV文件
            output_filename = f'{self.brand}_{market}_listing_sd_summary_data.csv'
            csv_path = os.path.join(get_export_path(), output_filename)
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            print("get_listing_sd_summary_data successfully!")
            return csv_path
        except Exception as error:
            print("get_listing_sd_summary_data Error while query data:", error)


    def get_expected_sales(self, market,start_date,end_date, sp_expectation=65, sd_expectation=25, spacos_expectation=24, sdacos_expectation=8):
        # 低于 平均ACOS值 30% 以上的  campaign 广告活动
        # 建议执行的操作：预算提升30%
        try:
            conn = self.conn
            # 暂时忽略了market转化 US
            query = f"""
WITH a AS (
    SELECT
        sales_channel AS 国家,
        CASE
            WHEN apie.parent_asins = '' THEN apie.asin
            ELSE apie.parent_asins
        END AS parent_asins_or_asin,
        DATE(CONVERT_TZ(purchase_date, '+08:00', '{self.load_config_info()['timezone_offset']}')) AS event_date,
        ROUND(SUM(item_price), 2) AS 总销售额
    FROM
        amazon_get_flat_file_all_orders_data_by_last_update_general agffa
        LEFT JOIN amazon_product_info_extended apie ON agffa.asin = apie.asin
    WHERE
        CONVERT_TZ(purchase_date, '+08:00', '{self.load_config_info()['timezone_offset']}') >= '{end_date}'
        AND CONVERT_TZ(purchase_date, '+08:00', '{self.load_config_info()['timezone_offset']}') < '{start_date}' + INTERVAL 1 DAY
        AND sales_channel = '{self.load_config_info()['sales_channel']}'
        AND apie.market = '{market}'
    GROUP BY
        sales_channel,
        CASE
            WHEN apie.parent_asins = '' THEN apie.asin
            ELSE apie.parent_asins
        END
),
b AS (
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
                CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END AS parent_asins_or_asin,
                adpr.market,
                adpr.DATE,
                SUM(cost) AS sum_cost,
                SUM(sales7d) AS sum_sales
            FROM
                amazon_advertised_product_reports_sp adpr
                LEFT JOIN amazon_product_info_extended apie ON adpr.advertisedAsin = apie.asin AND adpr.market = apie.market
            WHERE
                adpr.DATE >= '{end_date}'
                AND adpr.DATE <= '{start_date}'
            GROUP BY
                adpr.market,
                CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END
        ) AS sp
    LEFT JOIN (
        SELECT
            CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END AS parent_asins_or_asin,
            adpr.market,
            adpr.DATE,
            SUM(cost) AS sum_cost,
            SUM(sales) AS sum_sales
        FROM
            amazon_advertised_product_reports_sd adpr
            LEFT JOIN amazon_product_info_extended apie ON adpr.promotedAsin = apie.asin AND adpr.market = apie.market
        WHERE
            adpr.DATE >= '{end_date}'
            AND adpr.DATE <= '{start_date}'
        GROUP BY
            adpr.market,
            CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END
    ) AS sd ON sd.parent_asins_or_asin = sp.parent_asins_or_asin AND sd.market = sp.market
    WHERE
        sp.market = '{market}'
    ORDER BY
        sp.DATE
),
c AS (
    -- 计算广告数据
    SELECT
        sp.market AS 国家,
        sp.parent_asins_or_asin,
        sp.DATE,
        (COALESCE(sp.sum_cost, 0)) AS 广告总花费,
        (COALESCE(sp.sum_sales, 0)) AS 广告总销售额,
        ROUND(
            (COALESCE(sp.sum_cost, 0)) /
            (COALESCE(sp.sum_sales, 0)),
            4
        ) AS 广告总ACOS
    FROM
        (
            SELECT
                CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END AS parent_asins_or_asin,
                adpr.market,
                adpr.DATE,
                SUM(cost) AS sum_cost,
                SUM(sales7d) AS sum_sales
            FROM
                amazon_advertised_product_reports_sp adpr
                LEFT JOIN amazon_product_info_extended apie ON adpr.advertisedAsin = apie.asin AND adpr.market = apie.market
            WHERE
                adpr.DATE >= '{end_date}'
                AND adpr.DATE <= '{start_date}'
            GROUP BY
                adpr.market,
                CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END
        ) AS sp
    WHERE
        sp.market = '{market}'
    ORDER BY
        sp.DATE
),
d AS (
    -- 计算广告数据
    SELECT
        sp.market AS 国家,
        sp.parent_asins_or_asin,
        sp.DATE,
        (COALESCE(sp.sum_cost, 0)) AS 广告总花费,
        (COALESCE(sp.sum_sales, 0)) AS 广告总销售额,
        ROUND(
            (COALESCE(sp.sum_cost, 0)) /
            (COALESCE(sp.sum_sales, 0)),
            4
        ) AS 广告总ACOS
    FROM
        (
            SELECT
                CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END AS parent_asins_or_asin,
                adpr.market,
                adpr.DATE,
                SUM(cost) AS sum_cost,
                SUM(sales) AS sum_sales
            FROM
                amazon_advertised_product_reports_sd adpr
                LEFT JOIN amazon_product_info_extended apie ON adpr.promotedAsin = apie.asin AND adpr.market = apie.market
            WHERE
                adpr.DATE >= '{end_date}'
                AND adpr.DATE <= '{start_date}'
            GROUP BY
                adpr.market,
                CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END
        ) AS sp
    WHERE
        sp.market = '{market}'
    ORDER BY
        sp.DATE
)

SELECT
    a.parent_asins_or_asin AS list,
    COALESCE(c.广告总销售额, 0) AS SP广告销售额,
    CASE
        WHEN ROUND(((COALESCE(c.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 2) = ROUND(((COALESCE(c.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 0) THEN CONCAT(ROUND(((COALESCE(c.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 0), '%')
        ELSE CONCAT(ROUND(((COALESCE(c.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 2), '%')
    END AS SP广告销售额占比,
    CASE
        WHEN ROUND(c.广告总ACOS * 100, 2) = ROUND(c.广告总ACOS * 100, 0) THEN CONCAT(ROUND(c.广告总ACOS * 100, 0), '%')
        ELSE CONCAT(ROUND(c.广告总ACOS * 100, 2), '%')
    END AS SP_ACOS,
    COALESCE(d.广告总销售额, 0) AS SD广告销售额,
    CASE
        WHEN ROUND(((COALESCE(d.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 2) = ROUND(((COALESCE(d.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 0) THEN CONCAT(ROUND(((COALESCE(d.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 0), '%')
        ELSE CONCAT(ROUND(((COALESCE(d.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 2), '%')
    END AS SD广告销售额占比,
    CASE
        WHEN ROUND(d.广告总ACOS * 100, 2) = ROUND(d.广告总ACOS * 100, 0) THEN CONCAT(ROUND(d.广告总ACOS * 100, 0), '%')
        ELSE CONCAT(ROUND(d.广告总ACOS * 100, 2), '%')
    END AS SD_ACOS,
    COALESCE(b.广告总销售额, 0) AS 广告销售额,
    CASE
        WHEN ROUND(((COALESCE(c.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 2) > {sp_expectation} THEN COALESCE(c.广告总销售额, 0)
        ELSE ROUND(COALESCE(d.广告总销售额, 0) / ({sd_expectation} / 100) * {sp_expectation} / 100, 2)
    END AS 期望SP销售额,
    CASE
        WHEN ROUND(((COALESCE(d.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 2) > {sd_expectation} THEN COALESCE(d.广告总销售额, 0)
        ELSE ROUND(COALESCE(c.广告总销售额, 0) / ({sp_expectation} / 100) * {sd_expectation} / 100, 2)
    END AS 期望SD销售额,
    CASE
        WHEN ROUND(((COALESCE(c.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 2) > {sp_expectation} THEN COALESCE(c.广告总销售额, 0)
        ELSE ROUND(COALESCE(d.广告总销售额, 0) / ({sd_expectation} / 100) * {sp_expectation} / 100, 2)
    END + CASE
        WHEN ROUND(((COALESCE(d.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 2) > {sd_expectation} THEN COALESCE(d.广告总销售额, 0)
        ELSE ROUND(COALESCE(c.广告总销售额, 0) / ({sp_expectation} / 100) * {sd_expectation} / 100, 2)
    END AS 期望广告销售额,
    COALESCE(b.广告总花费, 0) AS 广告花费,
    ROUND(
        (CASE
            WHEN ROUND(((COALESCE(d.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 2) > {sd_expectation}
            THEN COALESCE(d.广告总销售额, 0)
            ELSE ROUND(COALESCE(c.广告总销售额, 0) / ({spacos_expectation} / 100) * {sdacos_expectation} / 100, 2)
        END * LEAST({sdacos_expectation} / 100, ROUND(COALESCE(d.广告总ACOS, {sdacos_expectation} / 100) * 100, 2) / 100))
        +
        (CASE
            WHEN ROUND(((COALESCE(c.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 2) > {sp_expectation}
            THEN COALESCE(c.广告总销售额, 0)
            ELSE ROUND(COALESCE(d.广告总销售额, 0) / ({sdacos_expectation} / 100) * {spacos_expectation} / 100, 2)
        END * LEAST({spacos_expectation} / 100, ROUND(COALESCE(c.广告总ACOS, {spacos_expectation} / 100) * 100, 2) / 100)),
    2) AS 期望广告花费
FROM a
LEFT JOIN b ON a.parent_asins_or_asin = b.parent_asins_or_asin
LEFT JOIN c ON a.parent_asins_or_asin = c.parent_asins_or_asin
LEFT JOIN d ON a.parent_asins_or_asin = d.parent_asins_or_asin
ORDER BY
    COALESCE(b.广告总销售额, 0) DESC;

             """
            df = pd.read_sql(query, con=conn)
            # 计算汇总数据
            total_sp_sales = df['SP广告销售额'].sum()
            total_sd_sales = df['SD广告销售额'].sum()
            total_ad_sales = df['广告销售额'].sum()
            expect_sp_sales = df['期望SP销售额'].sum()
            expect_sd_sales = df['期望SD销售额'].sum()
            expect_ad_sales = df['期望广告销售额'].sum()
            total_ad_cost = df['广告花费'].sum()
            expect_ad_cost = df['期望广告花费'].sum()

            # 计算汇总行的各项指标
            if total_ad_sales > 0:
                sp_sales_ratio = (total_sp_sales / total_ad_sales) * 100
                sd_sales_ratio = (total_sd_sales / total_ad_sales) * 100
            else:
                sp_sales_ratio = 0
                sd_sales_ratio = 0

            # 创建汇总数据行
            summary_data = {
                'list': '总计',
                'SP广告销售额': total_sp_sales,
                'SP广告销售额占比': f'{sp_sales_ratio:.2f}%',
                'SP_ACOS': '',
                'SD广告销售额': round(total_sd_sales,2),
                'SD广告销售额占比': f'{sd_sales_ratio:.2f}%',
                'SD_ACOS': '',
                '广告销售额': round(total_ad_sales, 2),
                '期望SP销售额': round(expect_sp_sales, 2),
                '期望SD销售额': round(expect_sd_sales, 2),
                '期望广告销售额': round(expect_ad_sales, 2),
                '广告花费': round(total_ad_cost, 2),
                '期望广告花费': round(expect_ad_cost, 2)
            }

            # 将汇总数据行添加到 DataFrame
            summary_df = pd.DataFrame([summary_data])
            df = pd.concat([df, summary_df], ignore_index=True)

            # 保存到CSV文件
            output_filename = f'{self.brand}_{market}_expected_sales_data.csv'
            csv_path = os.path.join(get_export_path(), output_filename)
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            print("get_expected_sales successfully!")
            return csv_path,round(expect_ad_sales, 2),round(total_ad_sales,2)
        except Exception as error:
            print("get_expected_sales Error while query data:", error)

    def get_expected_sales_supplier(self, market, start_date, end_date, sp_expectation=65, sd_expectation=25,
                           spacos_expectation=24, sdacos_expectation=8):
        # 低于 平均ACOS值 30% 以上的  campaign 广告活动
        # 建议执行的操作：预算提升30%
        try:
            conn = self.conn
            # 暂时忽略了market转化 US
            query = f"""
WITH a AS (
    SELECT
        CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END AS parent_asins_or_asin,
        DATE(CONVERT_TZ(endTime,'+08:00', '{self.load_config_info()['timezone_offset']}')) AS event_date,
        ROUND(SUM(orderedRevenue), 2) AS 总销售额
    FROM
        amazon_vendor_get_real_time_sales_report agffa
    LEFT JOIN
        amazon_product_info_extended apie ON agffa.asin = apie.asin
    WHERE
        CONVERT_TZ(endTime,'+08:00', '{self.load_config_info()['timezone_offset']}') >= '{end_date}'
        AND CONVERT_TZ(endTime,'+08:00', '{self.load_config_info()['timezone_offset']}') < '{start_date}' + INTERVAL 1 DAY
        AND agffa.market = '{market}'
        AND apie.market = '{market}'
    GROUP BY
        agffa.market,
        CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END
),
b AS (
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
                CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END AS parent_asins_or_asin,
                adpr.market,
                adpr.DATE,
                SUM(cost) AS sum_cost,
                SUM(sales7d) AS sum_sales
            FROM
                amazon_advertised_product_reports_sp adpr
                LEFT JOIN amazon_product_info_extended apie ON adpr.advertisedAsin = apie.asin AND adpr.market = apie.market
            WHERE
                adpr.DATE >= '{end_date}'
                AND adpr.DATE <= '{start_date}'
            GROUP BY
                adpr.market,
                CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END
        ) AS sp
    LEFT JOIN (
        SELECT
            CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END AS parent_asins_or_asin,
            adpr.market,
            adpr.DATE,
            SUM(cost) AS sum_cost,
            SUM(sales) AS sum_sales
        FROM
            amazon_advertised_product_reports_sd adpr
            LEFT JOIN amazon_product_info_extended apie ON adpr.promotedAsin = apie.asin AND adpr.market = apie.market
        WHERE
            adpr.DATE >= '{end_date}'
            AND adpr.DATE <= '{start_date}'
        GROUP BY
            adpr.market,
            CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END
    ) AS sd ON sd.parent_asins_or_asin = sp.parent_asins_or_asin AND sd.market = sp.market
    WHERE
        sp.market = '{market}'
    ORDER BY
        sp.DATE
),
c AS (
    -- 计算广告数据
    SELECT
        sp.market AS 国家,
        sp.parent_asins_or_asin,
        sp.DATE,
        (COALESCE(sp.sum_cost, 0)) AS 广告总花费,
        (COALESCE(sp.sum_sales, 0)) AS 广告总销售额,
        ROUND(
            (COALESCE(sp.sum_cost, 0)) /
            (COALESCE(sp.sum_sales, 0)),
            4
        ) AS 广告总ACOS
    FROM
        (
            SELECT
                CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END AS parent_asins_or_asin,
                adpr.market,
                adpr.DATE,
                SUM(cost) AS sum_cost,
                SUM(sales7d) AS sum_sales
            FROM
                amazon_advertised_product_reports_sp adpr
                LEFT JOIN amazon_product_info_extended apie ON adpr.advertisedAsin = apie.asin AND adpr.market = apie.market
            WHERE
                adpr.DATE >= '{end_date}'
                AND adpr.DATE <= '{start_date}'
            GROUP BY
                adpr.market,
                CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END
        ) AS sp
    WHERE
        sp.market = '{market}'
    ORDER BY
        sp.DATE
),
d AS (
    -- 计算广告数据
    SELECT
        sp.market AS 国家,
        sp.parent_asins_or_asin,
        sp.DATE,
        (COALESCE(sp.sum_cost, 0)) AS 广告总花费,
        (COALESCE(sp.sum_sales, 0)) AS 广告总销售额,
        ROUND(
            (COALESCE(sp.sum_cost, 0)) /
            (COALESCE(sp.sum_sales, 0)),
            4
        ) AS 广告总ACOS
    FROM
        (
            SELECT
                CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END AS parent_asins_or_asin,
                adpr.market,
                adpr.DATE,
                SUM(cost) AS sum_cost,
                SUM(sales) AS sum_sales
            FROM
                amazon_advertised_product_reports_sd adpr
                LEFT JOIN amazon_product_info_extended apie ON adpr.promotedAsin = apie.asin AND adpr.market = apie.market
            WHERE
                adpr.DATE >= '{end_date}'
                AND adpr.DATE <= '{start_date}'
            GROUP BY
                adpr.market,
                CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END
        ) AS sp
    WHERE
        sp.market = '{market}'
    ORDER BY
        sp.DATE
)

SELECT
    a.parent_asins_or_asin AS list,
    COALESCE(c.广告总销售额, 0) AS SP广告销售额,
    CASE
        WHEN ROUND(((COALESCE(c.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 2) = ROUND(((COALESCE(c.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 0) THEN CONCAT(ROUND(((COALESCE(c.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 0), '%')
        ELSE CONCAT(ROUND(((COALESCE(c.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 2), '%')
    END AS SP广告销售额占比,
    CASE
        WHEN ROUND(c.广告总ACOS * 100, 2) = ROUND(c.广告总ACOS * 100, 0) THEN CONCAT(ROUND(c.广告总ACOS * 100, 0), '%')
        ELSE CONCAT(ROUND(c.广告总ACOS * 100, 2), '%')
    END AS SP_ACOS,
    COALESCE(d.广告总销售额, 0) AS SD广告销售额,
    CASE
        WHEN ROUND(((COALESCE(d.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 2) = ROUND(((COALESCE(d.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 0) THEN CONCAT(ROUND(((COALESCE(d.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 0), '%')
        ELSE CONCAT(ROUND(((COALESCE(d.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 2), '%')
    END AS SD广告销售额占比,
    CASE
        WHEN ROUND(d.广告总ACOS * 100, 2) = ROUND(d.广告总ACOS * 100, 0) THEN CONCAT(ROUND(d.广告总ACOS * 100, 0), '%')
        ELSE CONCAT(ROUND(d.广告总ACOS * 100, 2), '%')
    END AS SD_ACOS,
    COALESCE(b.广告总销售额, 0) AS 广告销售额,
    CASE
        WHEN ROUND(((COALESCE(c.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 2) > {sp_expectation} THEN COALESCE(c.广告总销售额, 0)
        ELSE ROUND(COALESCE(d.广告总销售额, 0) / ({sd_expectation} / 100) * {sp_expectation} / 100, 2)
    END AS 期望SP销售额,
    CASE
        WHEN ROUND(((COALESCE(d.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 2) > {sd_expectation} THEN COALESCE(d.广告总销售额, 0)
        ELSE ROUND(COALESCE(c.广告总销售额, 0) / ({sp_expectation} / 100) * {sd_expectation} / 100, 2)
    END AS 期望SD销售额,
    CASE
        WHEN ROUND(((COALESCE(c.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 2) > {sp_expectation} THEN COALESCE(c.广告总销售额, 0)
        ELSE ROUND(COALESCE(d.广告总销售额, 0) / ({sd_expectation} / 100) * {sp_expectation} / 100, 2)
    END + CASE
        WHEN ROUND(((COALESCE(d.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 2) > {sd_expectation} THEN COALESCE(d.广告总销售额, 0)
        ELSE ROUND(COALESCE(c.广告总销售额, 0) / ({sp_expectation} / 100) * {sd_expectation} / 100, 2)
    END AS 期望广告销售额,
    COALESCE(b.广告总花费, 0) AS 广告花费,
    ROUND(
        (CASE
            WHEN ROUND(((COALESCE(d.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 2) > {sd_expectation}
            THEN COALESCE(d.广告总销售额, 0)
            ELSE ROUND(COALESCE(c.广告总销售额, 0) / ({spacos_expectation} / 100) * {sdacos_expectation} / 100, 2)
        END * LEAST({sdacos_expectation} / 100, ROUND(COALESCE(d.广告总ACOS, {sdacos_expectation} / 100) * 100, 2) / 100))
        +
        (CASE
            WHEN ROUND(((COALESCE(c.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 2) > {sp_expectation}
            THEN COALESCE(c.广告总销售额, 0)
            ELSE ROUND(COALESCE(d.广告总销售额, 0) / ({sdacos_expectation} / 100) * {spacos_expectation} / 100, 2)
        END * LEAST({spacos_expectation} / 100, ROUND(COALESCE(c.广告总ACOS, {spacos_expectation} / 100) * 100, 2) / 100)),
    2) AS 期望广告花费
FROM a
LEFT JOIN b ON a.parent_asins_or_asin = b.parent_asins_or_asin
LEFT JOIN c ON a.parent_asins_or_asin = c.parent_asins_or_asin
LEFT JOIN d ON a.parent_asins_or_asin = d.parent_asins_or_asin
ORDER BY
    COALESCE(b.广告总销售额, 0) DESC;

             """
            df = pd.read_sql(query, con=conn)
            # 计算汇总数据
            total_sp_sales = df['SP广告销售额'].sum()
            total_sd_sales = df['SD广告销售额'].sum()
            total_ad_sales = df['广告销售额'].sum()
            expect_sp_sales = df['期望SP销售额'].sum()
            expect_sd_sales = df['期望SD销售额'].sum()
            expect_ad_sales = df['期望广告销售额'].sum()
            total_ad_cost = df['广告花费'].sum()
            expect_ad_cost = df['期望广告花费'].sum()

            # 计算汇总行的各项指标
            if total_ad_sales > 0:
                sp_sales_ratio = (total_sp_sales / total_ad_sales) * 100
                sd_sales_ratio = (total_sd_sales / total_ad_sales) * 100
            else:
                sp_sales_ratio = 0
                sd_sales_ratio = 0

            # 创建汇总数据行
            summary_data = {
                'list': '总计',
                'SP广告销售额': total_sp_sales,
                'SP广告销售额占比': f'{sp_sales_ratio:.2f}%',
                'SP_ACOS': '',
                'SD广告销售额': round(total_sd_sales, 2),
                'SD广告销售额占比': f'{sd_sales_ratio:.2f}%',
                'SD_ACOS': '',
                '广告销售额': round(total_ad_sales, 2),
                '期望SP销售额': round(expect_sp_sales, 2),
                '期望SD销售额': round(expect_sd_sales, 2),
                '期望广告销售额': round(expect_ad_sales, 2),
                '广告花费': round(total_ad_cost, 2),
                '期望广告花费': round(expect_ad_cost, 2)
            }

            # 将汇总数据行添加到 DataFrame
            summary_df = pd.DataFrame([summary_data])
            df = pd.concat([df, summary_df], ignore_index=True)

            # 保存到CSV文件
            output_filename = f'{self.brand}_{market}_expected_sales_data.csv'
            csv_path = os.path.join(get_export_path(), output_filename)
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            print("get_expected_sales successfully!")
            return csv_path, round(expect_ad_sales, 2), round(total_ad_sales, 2)
        except Exception as error:
            print("get_expected_sales Error while query data:", error)


    def get_expected_cost(self, market,start_date,end_date, spacos_expectation=24, sdacos_expectation=8):
        # 低于 平均ACOS值 30% 以上的  campaign 广告活动
        # 建议执行的操作：预算提升30%
        try:
            conn = self.conn
            # 暂时忽略了market转化 US
            query = f"""
 WITH a AS  (
	SELECT
		sales_channel AS 国家,
	CASE
		WHEN apie.parent_asins = '' THEN
		apie.asin ELSE apie.parent_asins
		END AS parent_asins_or_asin,
		DATE(
		CONVERT_TZ( purchase_date, '+08:00', '{self.load_config_info()['timezone_offset']}' )) AS event_date,
		ROUND( SUM( item_price ), 2 ) AS 总销售额
	FROM
		amazon_get_flat_file_all_orders_data_by_last_update_general agffa
		LEFT JOIN amazon_product_info_extended apie ON agffa.asin = apie.asin
	WHERE
		CONVERT_TZ( purchase_date, '+08:00', '{self.load_config_info()['timezone_offset']}' ) >= '{end_date}'
		AND CONVERT_TZ( purchase_date, '+08:00', '{self.load_config_info()['timezone_offset']}' ) < '{start_date}' + INTERVAL 1 DAY
		AND sales_channel = '{self.load_config_info()['sales_channel']}'
		AND apie.market = '{market}'
	GROUP BY
		sales_channel,
	CASE

			WHEN apie.parent_asins = '' THEN
			apie.asin ELSE apie.parent_asins
		END
    ),
b AS (
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
										CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END AS parent_asins_or_asin,
                    adpr.market,
                    adpr.DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales7d) AS sum_sales
                FROM
                    amazon_advertised_product_reports_sp adpr
								LEFT JOIN
										amazon_product_info_extended apie ON adpr.advertisedAsin = apie.asin AND adpr.market = apie.market
                WHERE
                    adpr.DATE >= '{end_date}'
                    AND adpr.DATE <= '{start_date}'
                GROUP BY
                    adpr.market,
										CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END
            ) AS sp
        LEFT JOIN (
                SELECT
										CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END AS parent_asins_or_asin,
                    adpr.market,
                    adpr.DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales) AS sum_sales
                FROM
                    amazon_advertised_product_reports_sd adpr
								LEFT JOIN
										amazon_product_info_extended apie ON adpr.promotedAsin = apie.asin AND adpr.market = apie.market
                WHERE
                    adpr.DATE >= '{end_date}'
                    AND adpr.DATE <= '{start_date}'
                GROUP BY
                    adpr.market,
										CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END
            ) AS sd ON sd.parent_asins_or_asin = sp.parent_asins_or_asin AND sd.market = sp.market
        WHERE
            sp.market = '{market}'
        ORDER BY
            sp.DATE
    ),
c AS (
        -- 计算广告数据
        SELECT
            sp.market AS 国家,
            sp.parent_asins_or_asin,
						sp.DATE,
            (COALESCE(sp.sum_cost, 0)) AS 广告总花费,
            (COALESCE(sp.sum_sales, 0)) AS 广告总销售额,
            ROUND(
                (COALESCE(sp.sum_cost, 0)) /
                (COALESCE(sp.sum_sales, 0)),
                4
            ) AS 广告总ACOS
        FROM
            (
                SELECT
										CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END AS parent_asins_or_asin,
                    adpr.market,
                    adpr.DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales7d) AS sum_sales
                FROM
                    amazon_advertised_product_reports_sp adpr
								LEFT JOIN
										amazon_product_info_extended apie ON adpr.advertisedAsin = apie.asin AND adpr.market = apie.market
                WHERE
                    adpr.DATE >= '{end_date}'
                    AND adpr.DATE <= '{start_date}'
                GROUP BY
                    adpr.market,
										CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END
            ) AS sp
        WHERE
            sp.market = '{market}'
        ORDER BY
            sp.DATE
),
d AS (
        -- 计算广告数据
        SELECT
            sp.market AS 国家,
            sp.parent_asins_or_asin,
						sp.DATE,
            (COALESCE(sp.sum_cost, 0)) AS 广告总花费,
            (COALESCE(sp.sum_sales, 0)) AS 广告总销售额,
            ROUND(
                (COALESCE(sp.sum_cost, 0)) /
                (COALESCE(sp.sum_sales, 0)),
                4
            ) AS 广告总ACOS
        FROM
            (
                SELECT
										CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END AS parent_asins_or_asin,
                    adpr.market,
                    adpr.DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales) AS sum_sales
                FROM
                    amazon_advertised_product_reports_sd adpr
								LEFT JOIN
										amazon_product_info_extended apie ON adpr.promotedAsin = apie.asin AND adpr.market = apie.market
                WHERE
                    adpr.DATE >= '{end_date}'
                    AND adpr.DATE <= '{start_date}'
                GROUP BY
                    adpr.market,
										CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END
            ) AS sp
        WHERE
            sp.market = '{market}'
        ORDER BY
            sp.DATE
)
		SELECT
        a.parent_asins_or_asin as list,
				CASE
				WHEN ROUND(((COALESCE(c.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 2) > {spacos_expectation}
 THEN COALESCE(c.广告总销售额, 0)
        ELSE ROUND(COALESCE(d.广告总销售额, 0) / ({sdacos_expectation}
 / 100)* {spacos_expectation}
/100,2)
				END AS 期望SP销售额,
				CONCAT(ROUND(c.广告总ACOS * 100, 2), '%') AS SP_ACOS,
				CONCAT({spacos_expectation}
,'%') AS 期望SPAcos,
				CASE
        WHEN ROUND(((COALESCE(d.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 2) > {sdacos_expectation}
 THEN COALESCE(d.广告总销售额, 0)
        ELSE ROUND(COALESCE(c.广告总销售额, 0) / ({spacos_expectation}
 / 100)* {sdacos_expectation}
/100,2)
				END AS 期望SD销售额,
				CONCAT(ROUND(d.广告总ACOS * 100, 2), '%') AS SD_ACOS,
				CONCAT({sdacos_expectation}
,'%') AS 期望SDAcos,
				COALESCE(b.广告总花费, 0) AS 广告花费,
				ROUND(
        (CASE
            WHEN ROUND(((COALESCE(d.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 2) > {sdacos_expectation}

            THEN COALESCE(d.广告总销售额, 0)
            ELSE ROUND(COALESCE(c.广告总销售额, 0) / ({spacos_expectation}
 / 100) * {sdacos_expectation}
 / 100, 2)
        END * LEAST({sdacos_expectation}
 / 100, ROUND(COALESCE(d.广告总ACOS, {sdacos_expectation}
 / 100) * 100, 2) / 100))
        +
        (CASE
            WHEN ROUND(((COALESCE(c.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 2) > {spacos_expectation}

            THEN COALESCE(c.广告总销售额, 0)
            ELSE ROUND(COALESCE(d.广告总销售额, 0) / ({sdacos_expectation}
 / 100) * {spacos_expectation}
 / 100, 2)
        END * LEAST({spacos_expectation}
 / 100, ROUND(COALESCE(c.广告总ACOS, {spacos_expectation}
 / 100) * 100, 2) / 100)),
    2) AS 期望广告花费,
    ROUND(
        (CASE
            WHEN ROUND(((COALESCE(d.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 2) > {sdacos_expectation}

            THEN COALESCE(d.广告总销售额, 0)
            ELSE ROUND(COALESCE(c.广告总销售额, 0) / ({spacos_expectation}
 / 100) * {sdacos_expectation}
 / 100, 2)
        END * LEAST({sdacos_expectation}
 / 100, ROUND(COALESCE(d.广告总ACOS, {sdacos_expectation}
 / 100) * 100, 2) / 100))
        +
        (CASE
            WHEN ROUND(((COALESCE(c.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 2) > {spacos_expectation}

            THEN COALESCE(c.广告总销售额, 0)
            ELSE ROUND(COALESCE(d.广告总销售额, 0) / ({sdacos_expectation}
 / 100) * {spacos_expectation}
 / 100, 2)
        END * LEAST({spacos_expectation}
 / 100, ROUND(COALESCE(c.广告总ACOS, {spacos_expectation}
 / 100) * 100, 2) / 100))
        - COALESCE(b.广告总花费, 0),
    2) AS 期望广告新增花费
		FROM a
		LEFT JOIN b ON a.parent_asins_or_asin = b.parent_asins_or_asin
		LEFT JOIN c ON a.parent_asins_or_asin = c.parent_asins_or_asin
		LEFT JOIN d ON a.parent_asins_or_asin = d.parent_asins_or_asin
	ORDER BY
COALESCE(b.广告总花费, 0) DESC
             """
            df = pd.read_sql(query, con=conn)
            # 计算汇总数据
            expect_sp_sales = df['期望SP销售额'].sum()
            expect_sd_sales = df['期望SD销售额'].sum()
            total_ad_cost = df['广告花费'].sum()
            expect_ad_cost = df['期望广告花费'].sum()
            expect_ad_added_cost = df['期望广告新增花费'].sum()

            # 创建汇总数据行
            summary_data = {
                'list': '总计',
                '期望SP销售额': round(expect_sp_sales, 2),
                'SP_ACOS': '',
                '期望SPAcos': f'{spacos_expectation}%',
                '期望SD销售额': round(expect_sd_sales, 2),
                'SD_ACOS': '',
                '期望SDAcos': f'{sdacos_expectation}%',
                '广告花费': round(total_ad_cost, 2),
                '期望广告花费': round(expect_ad_cost, 2),
                '期望广告新增花费': round(expect_ad_added_cost, 2)
            }

            # 将汇总数据行添加到 DataFrame
            summary_df = pd.DataFrame([summary_data])
            df = pd.concat([df, summary_df], ignore_index=True)

            # 保存到CSV文件
            output_filename = f'{self.brand}_{market}_expected_sales_cost.csv'
            csv_path = os.path.join(get_export_path(), output_filename)
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            print("get_expected_cost successfully!")
            return csv_path,round(expect_ad_cost, 2),round(total_ad_cost, 2)
        except Exception as error:
            print("get_expected_cost Error while query data:", error)


    def get_expected_cost_supplier(self, market,start_date,end_date, spacos_expectation=24, sdacos_expectation=8):
        # 低于 平均ACOS值 30% 以上的  campaign 广告活动
        # 建议执行的操作：预算提升30%
        try:
            conn = self.conn
            # 暂时忽略了market转化 US
            query = f"""
 WITH a AS  (
    SELECT
        CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END AS parent_asins_or_asin,
        DATE(CONVERT_TZ(endTime,'+08:00', '{self.load_config_info()['timezone_offset']}')) AS event_date,
        ROUND(SUM(orderedRevenue), 2) AS 总销售额
    FROM
        amazon_vendor_get_real_time_sales_report agffa
    LEFT JOIN
        amazon_product_info_extended apie ON agffa.asin = apie.asin
    WHERE
        CONVERT_TZ(endTime,'+08:00', '{self.load_config_info()['timezone_offset']}') >= '{end_date}'
        AND CONVERT_TZ(endTime,'+08:00', '{self.load_config_info()['timezone_offset']}') < '{start_date}' + INTERVAL 1 DAY
        AND agffa.market = '{market}'
        AND apie.market = '{market}'
    GROUP BY
        agffa.market,
        CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END
    ),
b AS (
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
										CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END AS parent_asins_or_asin,
                    adpr.market,
                    adpr.DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales7d) AS sum_sales
                FROM
                    amazon_advertised_product_reports_sp adpr
								LEFT JOIN
										amazon_product_info_extended apie ON adpr.advertisedAsin = apie.asin AND adpr.market = apie.market
                WHERE
                    adpr.DATE >= '{end_date}'
                    AND adpr.DATE <= '{start_date}'
                GROUP BY
                    adpr.market,
										CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END
            ) AS sp
        LEFT JOIN (
                SELECT
										CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END AS parent_asins_or_asin,
                    adpr.market,
                    adpr.DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales) AS sum_sales
                FROM
                    amazon_advertised_product_reports_sd adpr
								LEFT JOIN
										amazon_product_info_extended apie ON adpr.promotedAsin = apie.asin AND adpr.market = apie.market
                WHERE
                    adpr.DATE >= '{end_date}'
                    AND adpr.DATE <= '{start_date}'
                GROUP BY
                    adpr.market,
										CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END
            ) AS sd ON sd.parent_asins_or_asin = sp.parent_asins_or_asin AND sd.market = sp.market
        WHERE
            sp.market = '{market}'
        ORDER BY
            sp.DATE
    ),
c AS (
        -- 计算广告数据
        SELECT
            sp.market AS 国家,
            sp.parent_asins_or_asin,
						sp.DATE,
            (COALESCE(sp.sum_cost, 0)) AS 广告总花费,
            (COALESCE(sp.sum_sales, 0)) AS 广告总销售额,
            ROUND(
                (COALESCE(sp.sum_cost, 0)) /
                (COALESCE(sp.sum_sales, 0)),
                4
            ) AS 广告总ACOS
        FROM
            (
                SELECT
										CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END AS parent_asins_or_asin,
                    adpr.market,
                    adpr.DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales7d) AS sum_sales
                FROM
                    amazon_advertised_product_reports_sp adpr
								LEFT JOIN
										amazon_product_info_extended apie ON adpr.advertisedAsin = apie.asin AND adpr.market = apie.market
                WHERE
                    adpr.DATE >= '{end_date}'
                    AND adpr.DATE <= '{start_date}'
                GROUP BY
                    adpr.market,
										CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END
            ) AS sp
        WHERE
            sp.market = '{market}'
        ORDER BY
            sp.DATE
),
d AS (
        -- 计算广告数据
        SELECT
            sp.market AS 国家,
            sp.parent_asins_or_asin,
						sp.DATE,
            (COALESCE(sp.sum_cost, 0)) AS 广告总花费,
            (COALESCE(sp.sum_sales, 0)) AS 广告总销售额,
            ROUND(
                (COALESCE(sp.sum_cost, 0)) /
                (COALESCE(sp.sum_sales, 0)),
                4
            ) AS 广告总ACOS
        FROM
            (
                SELECT
										CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END AS parent_asins_or_asin,
                    adpr.market,
                    adpr.DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales) AS sum_sales
                FROM
                    amazon_advertised_product_reports_sd adpr
								LEFT JOIN
										amazon_product_info_extended apie ON adpr.promotedAsin = apie.asin AND adpr.market = apie.market
                WHERE
                    adpr.DATE >= '{end_date}'
                    AND adpr.DATE <= '{start_date}'
                GROUP BY
                    adpr.market,
										CASE WHEN apie.parent_asins = '' THEN apie.asin ELSE apie.parent_asins END
            ) AS sp
        WHERE
            sp.market = '{market}'
        ORDER BY
            sp.DATE
)
		SELECT
        a.parent_asins_or_asin as list,
				CASE
				WHEN ROUND(((COALESCE(c.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 2) > {spacos_expectation}
 THEN COALESCE(c.广告总销售额, 0)
        ELSE ROUND(COALESCE(d.广告总销售额, 0) / ({sdacos_expectation}
 / 100)* {spacos_expectation}
/100,2)
				END AS 期望SP销售额,
				CONCAT(ROUND(c.广告总ACOS * 100, 2), '%') AS SP_ACOS,
				CONCAT({spacos_expectation}
,'%') AS 期望SPAcos,
				CASE
        WHEN ROUND(((COALESCE(d.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 2) > {sdacos_expectation}
 THEN COALESCE(d.广告总销售额, 0)
        ELSE ROUND(COALESCE(c.广告总销售额, 0) / ({spacos_expectation}
 / 100)* {sdacos_expectation}
/100,2)
				END AS 期望SD销售额,
				CONCAT(ROUND(d.广告总ACOS * 100, 2), '%') AS SD_ACOS,
				CONCAT({sdacos_expectation}
,'%') AS 期望SDAcos,
				COALESCE(b.广告总花费, 0) AS 广告花费,
				ROUND(
        (CASE
            WHEN ROUND(((COALESCE(d.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 2) > {sdacos_expectation}

            THEN COALESCE(d.广告总销售额, 0)
            ELSE ROUND(COALESCE(c.广告总销售额, 0) / ({spacos_expectation}
 / 100) * {sdacos_expectation}
 / 100, 2)
        END * LEAST({sdacos_expectation}
 / 100, ROUND(COALESCE(d.广告总ACOS, {sdacos_expectation}
 / 100) * 100, 2) / 100))
        +
        (CASE
            WHEN ROUND(((COALESCE(c.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 2) > {spacos_expectation}

            THEN COALESCE(c.广告总销售额, 0)
            ELSE ROUND(COALESCE(d.广告总销售额, 0) / ({sdacos_expectation}
 / 100) * {spacos_expectation}
 / 100, 2)
        END * LEAST({spacos_expectation}
 / 100, ROUND(COALESCE(c.广告总ACOS, {spacos_expectation}
 / 100) * 100, 2) / 100)),
    2) AS 期望广告花费,
    ROUND(
        (CASE
            WHEN ROUND(((COALESCE(d.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 2) > {sdacos_expectation}

            THEN COALESCE(d.广告总销售额, 0)
            ELSE ROUND(COALESCE(c.广告总销售额, 0) / ({spacos_expectation}
 / 100) * {sdacos_expectation}
 / 100, 2)
        END * LEAST({sdacos_expectation}
 / 100, ROUND(COALESCE(d.广告总ACOS, {sdacos_expectation}
 / 100) * 100, 2) / 100))
        +
        (CASE
            WHEN ROUND(((COALESCE(c.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 2) > {spacos_expectation}

            THEN COALESCE(c.广告总销售额, 0)
            ELSE ROUND(COALESCE(d.广告总销售额, 0) / ({sdacos_expectation}
 / 100) * {spacos_expectation}
 / 100, 2)
        END * LEAST({spacos_expectation}
 / 100, ROUND(COALESCE(c.广告总ACOS, {spacos_expectation}
 / 100) * 100, 2) / 100))
        - COALESCE(b.广告总花费, 0),
    2) AS 期望广告新增花费
		FROM a
		LEFT JOIN b ON a.parent_asins_or_asin = b.parent_asins_or_asin
		LEFT JOIN c ON a.parent_asins_or_asin = c.parent_asins_or_asin
		LEFT JOIN d ON a.parent_asins_or_asin = d.parent_asins_or_asin
	ORDER BY
COALESCE(b.广告总花费, 0) DESC
             """
            df = pd.read_sql(query, con=conn)
            # 计算汇总数据
            expect_sp_sales = df['期望SP销售额'].sum()
            expect_sd_sales = df['期望SD销售额'].sum()
            total_ad_cost = df['广告花费'].sum()
            expect_ad_cost = df['期望广告花费'].sum()
            expect_ad_added_cost = df['期望广告新增花费'].sum()

            # 创建汇总数据行
            summary_data = {
                'list': '总计',
                '期望SP销售额': round(expect_sp_sales, 2),
                'SP_ACOS': '',
                '期望SPAcos': f'{spacos_expectation}%',
                '期望SD销售额': round(expect_sd_sales, 2),
                'SD_ACOS': '',
                '期望SDAcos': f'{sdacos_expectation}%',
                '广告花费': round(total_ad_cost, 2),
                '期望广告花费': round(expect_ad_cost, 2),
                '期望广告新增花费': round(expect_ad_added_cost, 2)
            }

            # 将汇总数据行添加到 DataFrame
            summary_df = pd.DataFrame([summary_data])
            df = pd.concat([df, summary_df], ignore_index=True)

            # 保存到CSV文件
            output_filename = f'{self.brand}_{market}_expected_sales_cost.csv'
            csv_path = os.path.join(get_export_path(), output_filename)
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            print("get_expected_cost successfully!")
            return csv_path,round(expect_ad_cost, 2),round(total_ad_cost, 2)
        except Exception as error:
            print("get_expected_cost Error while query data:", error)

    def get_review_and_goals_data(self, market,start_date,end_date):
        # 低于 平均ACOS值 30% 以上的  campaign 广告活动
        # 建议执行的操作：预算提升30%
        try:
            print(start_date)
            print(end_date)
            conn = self.conn
            # 暂时忽略了market转化 US
            query = f"""
SELECT
                                CONCAT(
                                    DATE_FORMAT('{start_date}', '%m.%d'), '-', DATE_FORMAT('{end_date}', '%m.%d')
                                                ) AS 日期,
--         all_order.总销售额,
                                ROUND(all_order.总销售额/(DATEDIFF('{end_date}', '{start_date}')+1),2) AS 日均总销售额,
--         ad_order.广告总销售额 AS 广告销售额,
                                ROUND(ad_order.广告总销售额/(DATEDIFF('{end_date}', '{start_date}')+1),2) AS 日均广告销售额,
--         ROUND((all_order.总销售额 - ad_order.广告总销售额),2) AS 自然销售额,
--         CONCAT(ROUND(((1 - ad_order.广告总销售额 / all_order.总销售额) * 100), 2),'%') AS 自然销售额比例,
                                ad_order.广告总花费 AS 广告花费,
                                CONCAT(ROUND(ad_order.广告总ACOS * 100, 2), '%') AS 广告总ACOS,
                                CONCAT(ROUND(COALESCE(((ad_order.广告总花费 / all_order.总销售额) * 100), 0), 2), '%') AS Tacos,
--                                 COALESCE(deepbi_order.DeepBI计划销量, 0) AS DEEP_BI销售额,
                                ROUND(COALESCE(deepbi_order.DeepBI计划销量, 0)/(DATEDIFF('{end_date}', '{start_date}')+1),2) AS DeepBI计划日均销售额,
                                CONCAT(ROUND(COALESCE(deepbi_order.新开计划acos, 0) * 100, 2), '%') AS DeepBI计划ACOS,
                                CONCAT(ROUND(COALESCE(((deepbi_order.DeepBI计划销量 / ad_order.广告总销售额) * 100), 0), 2), '%') AS DeepBI计划销售额占比,
--                                 COALESCE(deepbi_sp_order.DeepBIsp计划销量, 0) AS DeepBIsp计划销量,
                                CONCAT(ROUND(COALESCE((deepbi_sp_order.DeepBIsp计划销量/deepbi_order.DeepBI计划销量)* 100, 0), 2), '%') AS DeepBI_SP广告占比,
                                CONCAT(ROUND(COALESCE((1 - deepbi_sp_order.DeepBIsp计划销量/deepbi_order.DeepBI计划销量)* 100, 0), 2), '%') AS DeepBI_SD广告占比
    FROM
    (
        -- 计算总销售额
        SELECT
            sales_channel AS 国家,
            '{market}' AS market,
            DATE(CONVERT_TZ(purchase_date ,'+08:00', '{self.load_config_info()['timezone_offset']}')) AS event_date,
            ROUND(SUM(item_price), 2) AS 总销售额
        FROM
            amazon_get_flat_file_all_orders_data_by_last_update_general
        WHERE
            CONVERT_TZ(purchase_date ,'+08:00', '{self.load_config_info()['timezone_offset']}') >= '{start_date}'
            AND CONVERT_TZ(purchase_date ,'+08:00', '{self.load_config_info()['timezone_offset']}') < '{end_date}' + INTERVAL 1 DAY
            AND sales_channel = '{self.load_config_info()['sales_channel']}'
        GROUP BY
            sales_channel
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
                    DATE >= '{start_date}'
                    AND DATE <= '{end_date}'
                GROUP BY
                    market
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
                    DATE >= '{start_date}'
                    AND DATE <= '{end_date}'
                GROUP BY
                    market
            ) AS sd ON sd.market = sp.market
        LEFT JOIN (
                SELECT
                    market,
                    DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales) AS sum_sales
                FROM
                    amazon_campaign_reports_sb
                WHERE
                    DATE >= '{start_date}'
                    AND DATE <= '{end_date}'
                GROUP BY
                    market
            ) AS sb ON sb.market = sp.market
        WHERE
            sp.market = '{market}'
        ORDER BY
            sp.DATE
    ) AS ad_order ON all_order.market = ad_order.国家
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
                    DATE >= '{start_date}'
                    AND DATE <= '{end_date}'
                    AND campaignName LIKE 'DeepBI_%'
                GROUP BY
                    market
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
                    DATE >= '{start_date}'
                    AND DATE <= '{end_date}'
                    AND campaignName LIKE 'DeepBI_%'
                GROUP BY
                    market
            ) AS sd ON sd.market = sp.market
        LEFT JOIN (
                SELECT
                    market,
                    DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales) AS sum_sales
                FROM
                    amazon_campaign_reports_sb
                WHERE
                    DATE >= '{start_date}'
                    AND DATE <= '{end_date}'
                    AND campaignName LIKE 'DeepBI_%'
                GROUP BY
                    market
            ) AS sb ON sb.market = sp.market
        WHERE
            sp.market = '{market}'
        ORDER BY
            sp.DATE
    ) AS deepbi_order ON all_order.market = deepbi_order.国家
                                    LEFT JOIN
    (
        -- 计算广告数据
        SELECT
            sp.market AS 国家,
            sp.DATE,
            (COALESCE(sp.sum_cost, 0) ) AS DeepBIsp计划花费,
            (COALESCE(sp.sum_sales, 0) ) AS DeepBIsp计划销量,
            ROUND(
                (COALESCE(sp.sum_cost, 0) ) /
                (COALESCE(sp.sum_sales, 0) ),
                4
            ) AS sp计划acos
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
                    DATE >= '{start_date}'
                    AND DATE <= '{end_date}'
                    AND campaignName LIKE 'DeepBI_%'
                GROUP BY
                    market
            ) AS sp
        WHERE
            sp.market = '{market}'
        ORDER BY
            sp.DATE
    ) AS deepbi_sp_order ON all_order.market = deepbi_sp_order.国家

             """
            df = pd.read_sql(query, con=conn)
            output_filename = f'{self.brand}_{market}_{end_date}_review_and_goals_data.csv'
            csv_path = os.path.join(get_export_path(), output_filename)
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            print("get_review_and_goals_data successfully!")
            return csv_path
        except Exception as error:
            print("get_review_and_goals_data Error while query data:", error)

    def get_store_sales_status(self, market, start_date, end_date, difference):
        # 低于 平均ACOS值 30% 以上的  campaign 广告活动
        # 建议执行的操作：预算提升30%
        try:
            conn = self.conn
            # 暂时忽略了market转化 US
            query = f"""
SELECT
        all_order.event_date as 日期,
        COALESCE(ad_order.广告总销售额, 0) AS 广告销售额,
        COALESCE(ad_order.广告总花费, 0) AS 广告总花费,
        CONCAT(ROUND(ad_order.广告总ACOS * 100, 2), '%') AS 广告总ACOS,
#         COALESCE(deepbi_order.DeepBI计划花费, 0) AS DeepBI计划花费,
#         COALESCE(deepbi_order.DeepBI计划销量, 0) AS DeepBI计划销量,
#         CONCAT(ROUND(COALESCE(deepbi_order.新开计划acos, 0) * 100, 2), '%') AS 新开计划acos,
#         CONCAT(ROUND(COALESCE(((deepbi_order.DeepBI计划销量 / ad_order.广告总销售额) * 100), 0), 2), '%') AS 新开计划销量占比,
#         ROUND((ad_order.广告总销售额 - COALESCE(deepbi_order.DeepBI计划销量, 0)), 2) AS 旧计划销售额,
#         ROUND((ad_order.广告总花费 - COALESCE(deepbi_order.DeepBI计划花费, 0)), 2) AS 旧计划花费,
#         CONCAT(ROUND(((ad_order.广告总花费 - COALESCE(deepbi_order.DeepBI计划花费, 0)) / (ad_order.广告总销售额 - COALESCE(deepbi_order.DeepBI计划销量, 0))) * 100, 2), '%') AS 旧计划acos,
#         CONCAT(ROUND(((1 - COALESCE(deepbi_order.DeepBI计划销量, 0) / ad_order.广告总销售额) * 100), 2), '%') AS 旧计划销量占比,
        COALESCE(all_order.总销售额, 0) AS 总销售额,
#         ad_order.广告总销售额 AS 广告销售额,
        ROUND((COALESCE(all_order.总销售额, 0) - COALESCE(ad_order.广告总销售额, 0)),2) AS 自然销售额,
        CONCAT(ROUND(((1 - ad_order.广告总销售额 / all_order.总销售额) * 100), 2),'%') AS 自然销售额比例
#         CONCAT(ROUND(((COALESCE(ad_order.广告总花费, 0) / all_order.总销售额) * 100), 2), '%') AS tacos
    FROM
    (
        -- 计算总销售额
        SELECT
            sales_channel AS 国家,
            DATE(CONVERT_TZ(purchase_date ,'+08:00', '{self.load_config_info()['timezone_offset']}')) AS event_date,
            ROUND(SUM(item_price), 2) AS 总销售额
        FROM
            amazon_get_flat_file_all_orders_data_by_last_update_general
        WHERE
            CONVERT_TZ(purchase_date ,'+08:00', '{self.load_config_info()['timezone_offset']}') >= '{start_date}'
            AND CONVERT_TZ(purchase_date ,'+08:00', '{self.load_config_info()['timezone_offset']}') < '{end_date}' + INTERVAL 1 DAY
            AND sales_channel = '{self.load_config_info()['sales_channel']}'
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
                    DATE >= '{start_date}'
                    AND DATE < '{end_date}'
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
                    DATE >= '{start_date}'
                    AND DATE < '{end_date}'
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
                    DATE >= '{start_date}'
                    AND DATE < '{end_date}'
                GROUP BY
                    market,
                    DATE
            ) AS sb ON sb.market = sp.market AND sb.DATE = sp.DATE
        WHERE
            sp.market = '{market}'
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
                    DATE >= '{start_date}'
                    AND DATE < '{end_date}'
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
                    DATE >= '{start_date}'
                    AND DATE < '{end_date}'
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
                    DATE >= '{start_date}'
                    AND DATE < '{end_date}'
                               AND campaignName LIKE 'DeepBI_%'
                GROUP BY
                    market,
                    DATE
            ) AS sb ON sb.market = sp.market AND sb.DATE = sp.DATE
        WHERE
            sp.market = '{market}'
        ORDER BY
            sp.DATE
    ) AS deepbi_order ON all_order.event_date = deepbi_order.DATE;
             """
            df = pd.read_sql(query, con=conn)
            # 计算汇总数据
            total_ad_sales = df['广告销售额'].sum()
            total_ad_cost = df['广告总花费'].sum()
            total_sales = df['总销售额'].sum()

            # 计算汇总行的各项指标
            if total_ad_sales > 0:
                ad_sales_ratio = (total_ad_cost / total_ad_sales) * 100
                nature_sales_ratio = ((total_sales - total_ad_sales) / total_sales) * 100
            else:
                ad_sales_ratio = 0
                nature_sales_ratio = 0

            # 创建汇总数据行
            summary_data = {
                '日期': '总计',
                '广告销售额': total_ad_sales,
                '广告总花费': total_ad_cost,
                '广告总ACOS': f'{ad_sales_ratio:.2f}%',
                '总销售额': round(total_sales, 2),
                '自然销售额': round(total_sales - total_ad_sales, 2),
                '自然销售额比例': f'{nature_sales_ratio:.2f}%'
            }
            average_data = {
                '日期': '日均',
                '广告销售额': total_ad_sales/difference,
                '广告总花费': total_ad_cost/difference,
                '广告总ACOS': f'{ad_sales_ratio:.2f}%',
                '总销售额': round(total_sales/difference, 2),
                '自然销售额': round((total_sales - total_ad_sales)/difference, 2),
                '自然销售额比例': f'{nature_sales_ratio:.2f}%'
            }

            # 将汇总数据行添加到 DataFrame
            summary_df = pd.DataFrame([summary_data])
            average_df = pd.DataFrame([average_data])
            df = pd.concat([df, summary_df], ignore_index=True)
            df = pd.concat([df, average_df], ignore_index=True)

            # 保存到CSV文件
            output_filename = f'{self.brand}_{market}_store_sales_data.csv'
            csv_path = os.path.join(get_export_path(), output_filename)
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            print("get_expected_sales successfully!")
            return csv_path
        except Exception as error:
            print("get_store_sales_status Error while query data:", error)

    def get_managed_listing_current_data(self, market,start_date,end_date):
        # 低于 平均ACOS值 30% 以上的  campaign 广告活动
        # 建议执行的操作：预算提升30%
        try:
            print(start_date)
            print(end_date)
            conn = self.conn
            # 暂时忽略了market转化 US
            query = f"""
SELECT
        CONCAT(DATE_FORMAT(@start_date, '%m.%d'), '-', DATE_FORMAT(@end_date, '%m.%d')) AS 日期,
--         COALESCE(ad_order.广告总销售额, 0) AS 广告总销售额,
                                ROUND(all_order.总销售额/(DATEDIFF(@end_date, @start_date)+1),2) AS 托管listing日均销售额,
--         COALESCE(ad_order.广告总花费, 0) AS 广告总花费,
--         CONCAT(ROUND(ad_order.广告总ACOS * 100, 2), '%%') AS 广告总ACOS,
--         COALESCE(deepbi_order.DeepBI计划花费, 0) AS DeepBI计划花费,
--         COALESCE(deepbi_order.DeepBI计划销量, 0) AS DeepBI计划销量,
                                ROUND(COALESCE(deepbi_order.DeepBI计划销量, 0)/(DATEDIFF(@end_date, @start_date)+1),2) AS DeepBI计划日均销售额,
        CONCAT(ROUND(COALESCE(deepbi_order.新开计划acos, 0) * 100, 2), '%%') AS DeepBI计划ACOS,
        CONCAT(ROUND(COALESCE(((deepbi_order.DeepBI计划销量 / ad_order.广告总销售额) * 100), 0), 2), '%%') AS DeepBI计划销售额占比,
--         ROUND((ad_order.广告总销售额 - COALESCE(deepbi_order.DeepBI计划销量, 0)), 2) AS 旧计划销售额,
--         ROUND((ad_order.广告总花费 - COALESCE(deepbi_order.DeepBI计划花费, 0)), 2) AS 旧计划花费,
--         CONCAT(ROUND(((ad_order.广告总花费 - COALESCE(deepbi_order.DeepBI计划花费, 0)) / (ad_order.广告总销售额 - COALESCE(deepbi_order.DeepBI计划销量, 0))) * 100, 2),'%%') AS 旧计划acos,
--         CONCAT(ROUND(((1 - COALESCE(deepbi_order.DeepBI计划销量, 0) / ad_order.广告总销售额) * 100), 2), '%%') AS 旧计划销量占比,
--         all_order.总销售额 as 总销售额,
--         COALESCE(ad_order.广告总销售额, 0) AS 广告销售额,
--         ROUND((all_order.总销售额 - COALESCE(ad_order.广告总销售额, 0)),2) AS 自然销售额,
--         CONCAT(ROUND(((1 - COALESCE(ad_order.广告总销售额, 0) / all_order.总销售额) * 100), 2),'%%') AS 自然销售额比例,
--         CONCAT(ROUND(((COALESCE(ad_order.广告总花费, 0) / all_order.总销售额) * 100), 2), '%%') AS tacos,
                                CONCAT(ROUND(COALESCE((deepbi_sp_order.DeepBIsp计划销量/deepbi_order.DeepBI计划销量)* 100, 0), 2), '%') AS DeepBI_SP广告占比,
                                CONCAT(ROUND(COALESCE((1 - deepbi_sp_order.DeepBIsp计划销量/deepbi_order.DeepBI计划销量)* 100, 0), 2), '%') AS DeepBI_SD广告占比
    FROM
    (
                        SELECT
                                                                                        sales_channel AS 国家,
                                                                                        'SA' AS  market,
                                                                                        CASE WHEN apie.parent_asins = '' THEN CONCAT(apie.asin,'(asin)') ELSE apie.parent_asins END AS parent_asins_or_asin,
                                                                                        DATE(CONVERT_TZ(purchase_date,'+08:00', '+03:00')) AS event_date,
                                                                                        ROUND(SUM(item_price), 2) AS 总销售额
                        FROM
                                                                                        amazon_get_flat_file_all_orders_data_by_last_update_general agffa
                        LEFT JOIN
                                                                                        amazon_product_info_extended apie ON agffa.asin = apie.asin
                        WHERE
                                                                                        CONVERT_TZ(purchase_date,'+08:00', '+03:00') >= @start_date
                                                                                        AND CONVERT_TZ(purchase_date,'+08:00', '+03:00') < @end_date + INTERVAL 1 DAY
                                                                                        AND sales_channel = 'Amazon.sa'
                                                                                        AND apie.market = 'SA'
                                                                                        AND apie.asin IN ('B0CQXPP44Y', 'B0D1YFL94W', 'B0D1YF9G3T', 'B0CZL4CW9X', 'B0CL47PF47')
                        GROUP BY
                                                                                        sales_channel
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
                    adpr.DATE >= @start_date
                    AND adpr.DATE < @end_date
                                                                                AND adpr.market = 'SA'

                                                                                AND apie.asin IN ('B0CQXPP44Y', 'B0D1YFL94W', 'B0D1YF9G3T', 'B0CZL4CW9X', 'B0CL47PF47')
                GROUP BY
                    adpr.market
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
                    adpr.DATE >= @start_date
                    AND adpr.DATE < @end_date
                                                                                AND apie.asin IN ('B0CQXPP44Y', 'B0D1YFL94W', 'B0D1YF9G3T', 'B0CZL4CW9X', 'B0CL47PF47')
                GROUP BY
                    adpr.market
            ) AS sd ON sd.market = sp.market
        WHERE
            sp.market = 'SA'

        ORDER BY
            sp.DATE
    ) AS ad_order ON all_order.market = ad_order.国家
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
                    adpr.DATE >= @start_date
                    AND adpr.DATE < @end_date
                                                                                AND adpr.market = 'SA'

                                                                                AND adpr.campaignName LIKE 'DeepBI_%%'
                                                                                AND apie.asin IN ('B0CQXPP44Y', 'B0D1YFL94W', 'B0D1YF9G3T', 'B0CZL4CW9X', 'B0CL47PF47')
                GROUP BY
                    adpr.market
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
                    adpr.DATE >= @start_date
                    AND adpr.DATE < @end_date
                                                                                AND adpr.campaignName LIKE 'DeepBI_%%'
                                                                                AND apie.asin IN ('B0CQXPP44Y', 'B0D1YFL94W', 'B0D1YF9G3T', 'B0CZL4CW9X', 'B0CL47PF47')
                GROUP BY
                    adpr.market
            ) AS sd ON sd.market = sp.market AND sd.DATE = sp.DATE
        WHERE
            sp.market = 'SA'

        ORDER BY
            sp.DATE
    ) AS deepbi_order ON all_order.market = deepbi_order.国家
                   LEFT JOIN
    (
        -- 计算广告数据
        SELECT
            sp.market AS 国家,
            sp.parent_asins_or_asin,
                                                sp.DATE,
            (COALESCE(sp.sum_cost, 0) ) AS DeepBIsp计划花费,
            (COALESCE(sp.sum_sales, 0) ) AS DeepBIsp计划销量,
            ROUND(
                (COALESCE(sp.sum_cost, 0) ) /
                (COALESCE(sp.sum_sales, 0) ),
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
                    adpr.DATE >= @start_date
                    AND adpr.DATE < @end_date
                                                                                AND adpr.market = 'SA'

                                                                                AND adpr.campaignName LIKE 'DeepBI_%%'
                                                                                AND apie.asin IN ('B0CQXPP44Y', 'B0D1YFL94W', 'B0D1YF9G3T', 'B0CZL4CW9X', 'B0CL47PF47')
                GROUP BY
                    adpr.market
            ) AS sp
        WHERE
            sp.market = 'SA'

        ORDER BY
            sp.DATE
    ) AS deepbi_sp_order ON all_order.market = deepbi_sp_order.国家;
             """
            df = pd.read_sql(query, con=conn)
            output_filename = f'{self.brand}_{market}_{end_date}_review_and_goals_data.csv'
            csv_path = os.path.join(get_export_path(), output_filename)
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            print("get_review_and_goals_data successfully!")
            return csv_path
        except Exception as error:
            print("get_review_and_goals_data Error while query data:", error)

if __name__ == '__main__':
    # res = DbToolsCsv('LAPASA', 'DE').load_config_info()
    # print(res)
    DbToolsCsv('DELOMO','{market}').get_listing_sp_specific_data('{market}')
