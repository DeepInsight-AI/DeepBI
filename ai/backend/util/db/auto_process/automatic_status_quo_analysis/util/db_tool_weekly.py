import json
import os
from datetime import datetime

import pandas as pd
import pymysql
from ai.backend.util.db.configuration.path import get_config_path
from ai.backend.util.db.auto_process.automatic_status_quo_analysis.export.export_path import get_export_path
from ai.backend.util.db.configuration.marketplaces import get_region_info
from ai.backend.util.db.auto_process.db_api import BaseDb


def get_timestamp():
    # 获取当前时间
    current_time = datetime.now()
    timestamp = int(current_time.timestamp())
    date_string = current_time.strftime("%Y-%m-%d")
    # 组合日期和时间戳
    date_timestamp_string = f"{date_string}_{timestamp}"
    return date_timestamp_string

class DbToolsCsv(BaseDb):

    def __init__(self, db, brand, market):
        super().__init__(db, brand, market)

    def load_config_info(self):
        # 从 JSON 文件加载数据库信息
        time_zone_info_path = os.path.join(get_config_path(), 'time_zone_information.json')
        with open(time_zone_info_path, 'r') as f:
            time_zone_info_json = json.load(f)
        return time_zone_info_json.get(self.market, "国家代码不存在")

    def load_summary_info(self):
        # 从 JSON 文件加载数据库信息
        time_zone_info_path = os.path.join(get_config_path(), 'summary.json')
        with open(time_zone_info_path, 'r') as f:
            time_zone_info_json = json.load(f)

        # 获取品牌信息
        brand_info = time_zone_info_json.get(self.brand)
        if brand_info:
            # 获取市场信息
            market_info = brand_info.get(self.market)
            if market_info:
                return market_info
            else:
                return "市场信息不存在"
        else:
            return "品牌代码不存在"

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
--                              all_order.总销售额,
                                ROUND(all_order.总销售额/(DATEDIFF('{end_date}', '{start_date}')+1),2) AS 日均总销售额,
--                              ad_order.广告总销售额 AS 广告销售额,
                                ROUND(ad_order.广告总销售额/(DATEDIFF('{end_date}', '{start_date}')+1),2) AS 日均广告销售额,
                                ROUND((all_order.总销售额 - ad_order.广告总销售额)/(DATEDIFF('{end_date}', '{start_date}')+1),2) AS 日均自然销售额,
                                CONCAT(ROUND(((1 - ad_order.广告总销售额 / all_order.总销售额) * 100), 2),'%') AS 自然销售额比例,
--                              ad_order.广告总花费 AS 广告花费,
                                ROUND(ad_order.广告总花费/(DATEDIFF('{end_date}', '{start_date}')+1),2) AS 日均广告花费,
                                CONCAT(ROUND(ad_order.广告总ACOS * 100, 2), '%') AS 广告总ACOS,
                                CONCAT(ROUND(COALESCE(((ad_order.广告总花费 / all_order.总销售额) * 100), 0), 2), '%') AS Tacos,
--                              COALESCE(deepbi_order.DeepBI计划销量, 0) AS DEEP_BI销售额,
                                ROUND(COALESCE(deepbi_order.DeepBI计划销量, 0)/(DATEDIFF('{end_date}', '{start_date}')+1),2) AS DeepBI计划日均销售额,
                                CONCAT(ROUND(COALESCE(deepbi_order.新开计划acos, 0) * 100, 2), '%') AS DeepBI计划ACOS,
                                CONCAT(ROUND(COALESCE(((deepbi_order.DeepBI计划销量 / ad_order.广告总销售额) * 100), 0), 2), '%') AS DeepBI计划销售额占比,
--                              COALESCE(deepbi_sp_order.DeepBIsp计划销量, 0) AS DeepBIsp计划销量,
                                CONCAT(ROUND(COALESCE((deepbi_sp_order.DeepBIsp计划销量/deepbi_order.DeepBI计划销量)* 100, 0), 2), '%') AS DeepBI_SP广告占比,
                                CONCAT(ROUND(COALESCE(deepbi_sp_order.sp计划acos, 0) * 100, 2), '%') AS DeepBI_SP广告ACOS,
                                CONCAT(ROUND(COALESCE((1 - deepbi_sp_order.DeepBIsp计划销量/deepbi_order.DeepBI计划销量)* 100, 0), 2), '%') AS DeepBI_SD广告占比,
                                CONCAT(ROUND(COALESCE(deepbi_sd_order.sd计划acos, 0) * 100, 2), '%') AS DeepBI_SD广告ACOS
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
                                    LEFT JOIN
    (
        -- 计算广告数据
        SELECT
            sp.market AS 国家,
            sp.DATE,
            (COALESCE(sp.sum_cost, 0) ) AS DeepBIsd计划花费,
            (COALESCE(sp.sum_sales, 0) ) AS DeepBIsd计划销量,
            ROUND(
                (COALESCE(sp.sum_cost, 0) ) /
                (COALESCE(sp.sum_sales, 0) ),
                4
            ) AS sd计划acos
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
    ) AS deepbi_sd_order ON all_order.market = deepbi_sd_order.国家

             """
            df = pd.read_sql(query, con=conn)
            output_filename = f'{self.brand}_{market}_{end_date}_review_and_goals_data.csv'
            csv_path = os.path.join(get_export_path(), output_filename)
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            print("get_review_and_goals_data successfully!")
            return csv_path
        except Exception as error:
            print("get_review_and_goals_data Error while query data:", error)

    def get_review_and_goals_data_summary(self, market, start_date, end_date):
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
					DATE_FORMAT('{start_date}', '%%m.%%d'), '-', DATE_FORMAT('{end_date}', '%%m.%%d')
											) AS 日期,
--    all_order.总销售额,
			ROUND(all_order.总销售额/(DATEDIFF('{end_date}', '{start_date}')+1),2) AS 日均总销售额,
--    ad_order.广告总销售额 AS 广告销售额,
			ROUND(ad_order.广告总销售额/(DATEDIFF('{end_date}', '{start_date}')+1),2) AS 日均广告销售额,
			ROUND((all_order.总销售额 - ad_order.广告总销售额)/(DATEDIFF('{end_date}', '{start_date}')+1),2) AS 日均自然销售额,
			CONCAT(ROUND(((1 - ad_order.广告总销售额 / all_order.总销售额) * 100), 2),'%%') AS 自然销售额比例,
--    ad_order.广告总花费 AS 广告花费,
			ROUND(ad_order.广告总花费/(DATEDIFF('{end_date}', '{start_date}')+1),2) AS 日均广告花费,
			CONCAT(ROUND(ad_order.广告总ACOS * 100, 2), '%%') AS 广告总ACOS,
			CONCAT(ROUND(COALESCE(((ad_order.广告总花费 / all_order.总销售额) * 100), 0), 2), '%%') AS Tacos,
--    COALESCE(deepbi_order.DeepBI计划销量, 0) AS DEEP_BI销售额,
			ROUND(COALESCE(deepbi_order.DeepBI计划销量, 0)/(DATEDIFF('{end_date}', '{start_date}')+1),2) AS DeepBI计划日均销售额,
			CONCAT(ROUND(COALESCE(deepbi_order.新开计划acos, 0) * 100, 2), '%%') AS DeepBI计划ACOS,
			CONCAT(ROUND(COALESCE(((deepbi_order.DeepBI计划销量 / ad_order.广告总销售额) * 100), 0), 2), '%%') AS DeepBI计划销售额占比,
--    COALESCE(deepbi_sp_order.DeepBIsp计划销量, 0) AS DeepBIsp计划销量,
			CONCAT(ROUND(COALESCE((deepbi_sp_order.DeepBIsp计划销量/deepbi_order.DeepBI计划销量)* 100, 0), 2), '%%') AS DeepBI_SP广告占比,
			CONCAT(ROUND(COALESCE(deepbi_sp_order.sp计划acos, 0) * 100, 2), '%%') AS DeepBI_SP广告ACOS,
			CONCAT(ROUND(COALESCE((1 - deepbi_sp_order.DeepBIsp计划销量/deepbi_order.DeepBI计划销量)* 100, 0), 2), '%%') AS DeepBI_SD广告占比,
			CONCAT(ROUND(COALESCE(deepbi_sd_order.sd计划acos, 0) * 100, 2), '%%') AS DeepBI_SD广告ACOS
    FROM
    (
        -- 计算总销售额
        SELECT
            sales_channel AS 国家,
            '{market}' AS market,
            DATE(CONVERT_TZ(purchase_date ,'+08:00', '{self.load_summary_info()['timezone_offset']}')) AS event_date,
            ROUND(SUM(item_price), 2) AS 总销售额
        FROM
            amazon_get_flat_file_all_orders_data_by_last_update_general
        WHERE
            CONVERT_TZ(purchase_date ,'+08:00', '{self.load_summary_info()['timezone_offset']}') >= '{start_date}'
            AND CONVERT_TZ(purchase_date ,'+08:00', '{self.load_summary_info()['timezone_offset']}') < '{end_date}' + INTERVAL 1 DAY
            AND sales_channel IN %(column1_values1)s
    ) AS all_order
    LEFT JOIN
    (
        -- 计算广告数据
        SELECT
            '{market}' AS 国家,
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
                    '{market}' AS market,
                    DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales7d) AS sum_sales
                FROM
                    amazon_campaign_reports_sp
                WHERE
                    DATE >= '{start_date}'
                    AND DATE <= '{end_date}'
										AND market IN %(column2_values2)s
            ) AS sp
        LEFT JOIN (
                SELECT
                    '{market}' AS market,
                    DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales) AS sum_sales
                FROM
                    amazon_campaign_reports_sd
                WHERE
                    DATE >= '{start_date}'
                    AND DATE <= '{end_date}'
										AND market IN %(column2_values2)s
            ) AS sd ON sd.market = sp.market
        LEFT JOIN (
                SELECT
                    '{market}' AS market,
                    DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales) AS sum_sales
                FROM
                    amazon_campaign_reports_sb
                WHERE
                    DATE >= '{start_date}'
                    AND DATE <= '{end_date}'
										AND market IN %(column2_values2)s
            ) AS sb ON sb.market = sp.market
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
                    '{market}' AS market,
                    DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales7d) AS sum_sales
                FROM
                    amazon_campaign_reports_sp
                WHERE
                    DATE >= '{start_date}'
                    AND DATE <= '{end_date}'
                    AND campaignName LIKE 'DeepBI_%%'
										AND market IN %(column2_values2)s
            ) AS sp
        LEFT JOIN (
                SELECT
                    '{market}' AS market,
                    DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales) AS sum_sales
                FROM
                    amazon_campaign_reports_sd
                WHERE
                    DATE >= '{start_date}'
                    AND DATE <= '{end_date}'
                    AND campaignName LIKE 'DeepBI_%%'
										AND market IN %(column2_values2)s
            ) AS sd ON sd.market = sp.market
        LEFT JOIN (
                SELECT
                    '{market}' AS market,
                    DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales) AS sum_sales
                FROM
                    amazon_campaign_reports_sb
                WHERE
                    DATE >= '{start_date}'
                    AND DATE <= '{end_date}'
                    AND campaignName LIKE 'DeepBI_%%'
										AND market IN %(column2_values2)s
            ) AS sb ON sb.market = sp.market
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
                    '{market}' AS market,
                    DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales7d) AS sum_sales
                FROM
                    amazon_campaign_reports_sp
                WHERE
                    DATE >= '{start_date}'
                    AND DATE <= '{end_date}'
                    AND campaignName LIKE 'DeepBI_%%'
										AND market IN %(column2_values2)s
            ) AS sp
        ORDER BY
            sp.DATE
    ) AS deepbi_sp_order ON all_order.market = deepbi_sp_order.国家
                                    LEFT JOIN
    (
        -- 计算广告数据
        SELECT
            sp.market AS 国家,
            sp.DATE,
            (COALESCE(sp.sum_cost, 0) ) AS DeepBIsd计划花费,
            (COALESCE(sp.sum_sales, 0) ) AS DeepBIsd计划销量,
            ROUND(
                (COALESCE(sp.sum_cost, 0) ) /
                (COALESCE(sp.sum_sales, 0) ),
                4
            ) AS sd计划acos
        FROM
            (
                SELECT
                    '{market}' AS market,
                    DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales) AS sum_sales
                FROM
                    amazon_campaign_reports_sd
                WHERE
                    DATE >= '{start_date}'
                    AND DATE <= '{end_date}'
                    AND campaignName LIKE 'DeepBI_%%'
										AND market IN %(column2_values2)s
            ) AS sp
        ORDER BY
            sp.DATE
    ) AS deepbi_sd_order ON all_order.market = deepbi_sd_order.国家
             """
            df = pd.read_sql(query, con=conn, params={'column1_values1': self.load_summary_info()['sales_channel'], 'column2_values2': self.load_summary_info()['country']})
            output_filename = f'{self.brand}_{market}_{end_date}_review_and_goals_summary_data.csv'
            csv_path = os.path.join(get_export_path(), output_filename)
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            print("get_review_and_goals_data_summary successfully!")
            return csv_path
        except Exception as error:
            print("get_review_and_goals_data_summary Error while query data:", error)

    def get_store_sales_status(self, market, start_date, end_date, difference):
        # 低于 平均ACOS值 30% 以上的  campaign 广告活动
        # 建议执行的操作：预算提升30%
        try:
            conn = self.conn
            # 暂时忽略了market转化 US
            query = f"""
SELECT
        all_order.event_date as 日期,
        COALESCE(all_order.总销售额, 0) AS 总销售额,
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
#         ad_order.广告总销售额 AS 广告销售额,
        ROUND((COALESCE(all_order.总销售额, 0) - COALESCE(ad_order.广告总销售额, 0)),2) AS 自然销售额,
        CONCAT(ROUND(((1 - ad_order.广告总销售额 / all_order.总销售额) * 100), 2),'%') AS 自然销售额比例,
        CONCAT(ROUND(((COALESCE(ad_order.广告总花费, 0) / all_order.总销售额) * 100), 2), '%') AS TAcos
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
                    AND DATE <= '{end_date}'
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
                    AND DATE <= '{end_date}'
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
                    AND DATE <= '{end_date}'
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
                    AND DATE <= '{end_date}'
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
                    AND DATE <= '{end_date}'
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
                    AND DATE <= '{end_date}'
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
            tacos = (total_ad_cost / total_sales) * 100 if total_sales > 0 else 0

            # 创建汇总数据行
            summary_data = {
                '日期': '总计',
                '广告销售额': total_ad_sales,
                '广告总花费': total_ad_cost,
                '广告总ACOS': f'{ad_sales_ratio:.2f}%',
                '总销售额': round(total_sales, 2),
                '自然销售额': round(total_sales - total_ad_sales, 2),
                '自然销售额比例': f'{nature_sales_ratio:.2f}%',
                'TAcos': f'{tacos:.2f}%'
            }
            average_data = {
                '日期': '日均',
                '广告销售额': total_ad_sales/difference,
                '广告总花费': total_ad_cost/difference,
                '广告总ACOS': f'{ad_sales_ratio:.2f}%',
                '总销售额': round(total_sales/difference, 2),
                '自然销售额': round((total_sales - total_ad_sales)/difference, 2),
                '自然销售额比例': f'{nature_sales_ratio:.2f}%',
                'TAcos': f'{tacos:.2f}%'
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

    def get_store_sales_status_summary(self, market, start_date, end_date, difference):

        try:
            conn = self.conn
            # 暂时忽略了market转化 US
            query = f"""
SELECT
        all_order.event_date as 日期,
        COALESCE(all_order.总销售额, 0) AS 总销售额,
        COALESCE(ad_order.广告总销售额, 0) AS 广告销售额,
        COALESCE(ad_order.广告总花费, 0) AS 广告总花费,
        CONCAT(ROUND(ad_order.广告总ACOS * 100, 2), '%%') AS 广告总ACOS,
        ROUND((COALESCE(all_order.总销售额, 0) - COALESCE(ad_order.广告总销售额, 0)),2) AS 自然销售额,
        CONCAT(ROUND(((1 - ad_order.广告总销售额 / all_order.总销售额) * 100), 2),'%%') AS 自然销售额比例,
        CONCAT(ROUND(((COALESCE(ad_order.广告总花费, 0) / all_order.总销售额) * 100), 2), '%%') AS TAcos
    FROM
    (
        -- 计算总销售额
        SELECT
            sales_channel AS 国家,
            '{market}' AS market,
            DATE(CONVERT_TZ(purchase_date ,'+08:00', '{self.load_summary_info()['timezone_offset']}')) AS event_date,
            ROUND(SUM(item_price), 2) AS 总销售额
        FROM
            amazon_get_flat_file_all_orders_data_by_last_update_general
        WHERE
            CONVERT_TZ(purchase_date ,'+08:00', '{self.load_summary_info()['timezone_offset']}') >= '{start_date}'
            AND CONVERT_TZ(purchase_date ,'+08:00', '{self.load_summary_info()['timezone_offset']}') < '{end_date}' + INTERVAL 1 DAY
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
                    '{market}' AS market,
                    DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales7d) AS sum_sales
                FROM
                    amazon_campaign_reports_sp
                WHERE
                    DATE >= '{start_date}'
                    AND DATE <= '{end_date}'
                    AND market IN %(column2_values2)s
                GROUP BY
                    DATE
            ) AS sp
        LEFT JOIN (
                SELECT
                    '{market}' AS market,
                    DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales) AS sum_sales
                FROM
                    amazon_campaign_reports_sd
                WHERE
                    DATE >= '{start_date}'
                    AND DATE <= '{end_date}'
                    AND market IN %(column2_values2)s
                GROUP BY
                    DATE
            ) AS sd ON sd.market = sp.market AND sd.DATE = sp.DATE
        LEFT JOIN (
                SELECT
                    '{market}' AS market,
                    DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales) AS sum_sales
                FROM
                    amazon_campaign_reports_sb
                WHERE
                    DATE >= '{start_date}'
                    AND DATE <= '{end_date}'
                    AND market IN %(column2_values2)s
                GROUP BY
                    DATE
            ) AS sb ON sb.market = sp.market AND sb.DATE = sp.DATE
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
                    '{market}' AS market,
                    DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales7d) AS sum_sales
                FROM
                    amazon_campaign_reports_sp
                WHERE
                    DATE >= '{start_date}'
                    AND DATE <= '{end_date}'
                    AND campaignName LIKE 'DeepBI_%%'
                    AND market IN %(column2_values2)s
                GROUP BY
                    DATE
            ) AS sp
        LEFT JOIN (
                SELECT
                    '{market}' AS market,
                    DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales) AS sum_sales
                FROM
                    amazon_campaign_reports_sd
                WHERE
                    DATE >= '{start_date}'
                    AND DATE <= '{end_date}'
                       AND campaignName LIKE 'DeepBI_%%'
                       AND market IN %(column2_values2)s
                GROUP BY
                    DATE
            ) AS sd ON sd.market = sp.market AND sd.DATE = sp.DATE
        LEFT JOIN (
                SELECT
                    '{market}' AS market,
                    DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales) AS sum_sales
                FROM
                    amazon_campaign_reports_sb
                WHERE
                    DATE >= '{start_date}'
                    AND DATE <= '{end_date}'
                               AND campaignName LIKE 'DeepBI_%%'
                               AND market IN %(column2_values2)s
                GROUP BY
                    DATE
            ) AS sb ON sb.market = sp.market AND sb.DATE = sp.DATE
        ORDER BY
            sp.DATE
    ) AS deepbi_order ON all_order.event_date = deepbi_order.DATE;
             """
            df = pd.read_sql(query, con=conn, params={'column1_values1': self.load_summary_info()['sales_channel'], 'column2_values2': self.load_summary_info()['country']})
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
            tacos = (total_ad_cost / total_sales) * 100 if total_sales > 0 else 0

            # 创建汇总数据行
            summary_data = {
                '日期': '总计',
                '广告销售额': total_ad_sales,
                '广告总花费': total_ad_cost,
                '广告总ACOS': f'{ad_sales_ratio:.2f}%',
                '总销售额': round(total_sales, 2),
                '自然销售额': round(total_sales - total_ad_sales, 2),
                '自然销售额比例': f'{nature_sales_ratio:.2f}%',
                'TAcos': f'{tacos:.2f}%'
            }
            average_data = {
                '日期': '日均',
                '广告销售额': total_ad_sales / difference,
                '广告总花费': total_ad_cost / difference,
                '广告总ACOS': f'{ad_sales_ratio:.2f}%',
                '总销售额': round(total_sales / difference, 2),
                '自然销售额': round((total_sales - total_ad_sales) / difference, 2),
                '自然销售额比例': f'{nature_sales_ratio:.2f}%',
                'TAcos': f'{tacos:.2f}%'
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
            return csv_path, self.load_summary_info()['country']
        except Exception as error:
            print("get_store_sales_status Error while query data:", error)

    def get_managed_listing_current_data(self, market,start_date,end_date,asin_info,start_date1,end_date1):
        # 低于 平均ACOS值 30% 以上的  campaign 广告活动
        # 建议执行的操作：预算提升30%
        try:
            conn = self.conn
            # 暂时忽略了market转化 US
            query = f"""
SELECT
	CONCAT( DATE_FORMAT( '{start_date}', '%%m.%%d' ), '-', DATE_FORMAT( '{end_date}', '%%m.%%d' ) ) AS 日期,
	--         COALESCE(ad_order.广告总销售额, 0) AS 广告总销售额,
	ROUND( all_order.总销售额 /( DATEDIFF( '{end_date}', '{start_date}' )+ 1 ), 2 ) AS 托管listing日均总销售额,
	ROUND( ad_order.广告总销售额 /( DATEDIFF( '{end_date}', '{start_date}' )+ 1 ), 2 ) AS 托管listing日均广告销售额,
    COALESCE(ad_order.广告总花费/( DATEDIFF( '{end_date}', '{start_date}' )+ 1 ), 0) AS 托管listing日均广告总花费,
    CONCAT(ROUND(ad_order.广告总ACOS * 100, 2), '%%') AS 广告总ACOS,
    CONCAT(ROUND(((COALESCE(ad_order.广告总花费, 0) / all_order.总销售额) * 100), 2), '%%') AS tacos,
	ROUND( COALESCE ( deepbi_order.DeepBI计划销量, 0 )/( DATEDIFF( '{end_date}', '{start_date}' )+ 1 ), 2 ) AS DeepBI计划日均销售额,
    COALESCE(deepbi_order.DeepBI计划花费/( DATEDIFF( '{end_date}', '{start_date}' )+ 1 ), 0) AS DeepBI计划日均花费,
	CONCAT( ROUND( COALESCE ( deepbi_order.新开计划acos, 0 ) * 100, 2 ), '%%' ) AS DeepBI计划ACOS,
	CONCAT( ROUND( COALESCE ((( deepbi_order.DeepBI计划销量 / ad_order.广告总销售额 ) * 100 ), 0 ), 2 ), '%%' ) AS DeepBI计划销售额占比,
--         ROUND((ad_order.广告总销售额 - COALESCE(deepbi_order.DeepBI计划销量, 0)), 2) AS 旧计划销售额,
--         ROUND((ad_order.广告总花费 - COALESCE(deepbi_order.DeepBI计划花费, 0)), 2) AS 旧计划花费,
--         CONCAT(ROUND(((ad_order.广告总花费 - COALESCE(deepbi_order.DeepBI计划花费, 0)) / (ad_order.广告总销售额 - COALESCE(deepbi_order.DeepBI计划销量, 0))) * 100, 2),'%%') AS 旧计划acos,
--         CONCAT(ROUND(((1 - COALESCE(deepbi_order.DeepBI计划销量, 0) / ad_order.广告总销售额) * 100), 2), '%%') AS 旧计划销量占比,
--         all_order.总销售额 as 总销售额,
--         COALESCE(ad_order.广告总销售额, 0) AS 广告销售额,
--         ROUND((all_order.总销售额 - COALESCE(ad_order.广告总销售额, 0)),2) AS 自然销售额,
--         CONCAT(ROUND(((1 - COALESCE(ad_order.广告总销售额, 0) / all_order.总销售额) * 100), 2),'%%') AS 自然销售额比例,
	CONCAT( ROUND( COALESCE (( deepbi_sp_order.DeepBIsp计划销量 / deepbi_order.DeepBI计划销量 )* 100, 0 ), 2 ), '%%' ) AS DeepBI_SP广告占比,
	CONCAT( ROUND( COALESCE (( 1 - deepbi_sp_order.DeepBIsp计划销量 / deepbi_order.DeepBI计划销量 )* 100, 0 ), 2 ), '%%' ) AS DeepBI_SD广告占比
FROM
	(
	SELECT
		sales_channel AS 国家,
		'{market}' AS market,
	CASE

			WHEN apie.parent_asins = '' THEN
			CONCAT( apie.asin, '(asin)' ) ELSE apie.parent_asins
		END AS parent_asins_or_asin,
		DATE(
		CONVERT_TZ( purchase_date, '+08:00', '{self.load_config_info()['timezone_offset']}' )) AS event_date,
		ROUND( SUM( item_price ), 2 ) AS 总销售额
	FROM
		amazon_get_flat_file_all_orders_data_by_last_update_general agffa
		LEFT JOIN amazon_product_info_extended apie ON agffa.asin = apie.asin
	WHERE
		CONVERT_TZ( purchase_date, '+08:00', '{self.load_config_info()['timezone_offset']}' ) >= '{start_date}'
		AND CONVERT_TZ( purchase_date, '+08:00', '{self.load_config_info()['timezone_offset']}' ) < '{end_date}' + INTERVAL 1 DAY
		AND sales_channel = '{self.load_config_info()['sales_channel']}'
		AND apie.market = '{market}'
		AND apie.asin IN %(column1_values1)s
	GROUP BY
		sales_channel
	) AS all_order
	LEFT JOIN (-- 计算广告数据
	SELECT
		sp.market AS 国家,
		sp.parent_asins_or_asin,
		sp.DATE,
		(
		COALESCE ( sp.sum_cost, 0 ) + COALESCE ( sd.sum_cost, 0 )) AS 广告总花费,
		(
		COALESCE ( sp.sum_sales, 0 ) + COALESCE ( sd.sum_sales, 0 )) AS 广告总销售额,
		ROUND(
			(
				COALESCE ( sp.sum_cost, 0 ) + COALESCE ( sd.sum_cost, 0 )) / (
			COALESCE ( sp.sum_sales, 0 ) + COALESCE ( sd.sum_sales, 0 )),
			4
		) AS 广告总ACOS
	FROM
		(
		SELECT
		CASE

			WHEN
				apie.parent_asins = '' THEN
					CONCAT( apie.asin, '(asin)' ) ELSE apie.parent_asins
					END AS parent_asins_or_asin,
				adpr.market,
				adpr.DATE,
				SUM( cost ) AS sum_cost,
				SUM( sales7d ) AS sum_sales
			FROM
				amazon_advertised_product_reports_sp adpr
				LEFT JOIN amazon_product_info_extended apie ON adpr.advertisedAsin = apie.asin
				AND adpr.market = apie.market
			WHERE
				adpr.DATE >= '{start_date}'
				AND adpr.DATE <= '{end_date}'
				AND adpr.market = '{market}'
				AND apie.asin IN %(column1_values1)s
			GROUP BY
				adpr.market
			) AS sp
			LEFT JOIN (
			SELECT
			CASE

				WHEN
					apie.parent_asins = '' THEN
						CONCAT( apie.asin, '(asin)' ) ELSE apie.parent_asins
						END AS parent_asins_or_asin,
					adpr.market,
					adpr.DATE,
					SUM( cost ) AS sum_cost,
					SUM( sales ) AS sum_sales
				FROM
					amazon_advertised_product_reports_sd adpr
					LEFT JOIN amazon_product_info_extended apie ON adpr.promotedAsin = apie.asin
					AND adpr.market = apie.market
				WHERE
					adpr.DATE >= '{start_date}'
					AND adpr.DATE <= '{end_date}'
					AND apie.asin IN %(column1_values1)s
				GROUP BY
					adpr.market
				) AS sd ON sp.market = sd.market
			WHERE
				sp.market = '{market}'
			ORDER BY
				sp.DATE
			) AS ad_order ON all_order.market = ad_order.国家
			LEFT JOIN (-- 计算广告数据
			SELECT
				sp.market AS 国家,
				sp.parent_asins_or_asin,
				sp.DATE,
				(
				COALESCE ( sp.sum_cost, 0 ) + COALESCE ( sd.sum_cost, 0 )) AS DeepBI计划花费,
				(
				COALESCE ( sp.sum_sales, 0 ) + COALESCE ( sd.sum_sales, 0 )) AS DeepBI计划销量,
				ROUND(
					(
						COALESCE ( sp.sum_cost, 0 ) + COALESCE ( sd.sum_cost, 0 )) / (
					COALESCE ( sp.sum_sales, 0 ) + COALESCE ( sd.sum_sales, 0 )),
					4
				) AS 新开计划acos
			FROM
				(
				SELECT
				CASE

					WHEN
						apie.parent_asins = '' THEN
							CONCAT( apie.asin, '(asin)' ) ELSE apie.parent_asins
							END AS parent_asins_or_asin,
						adpr.market,
						adpr.DATE,
						SUM( cost ) AS sum_cost,
						SUM( sales7d ) AS sum_sales
					FROM
						amazon_advertised_product_reports_sp adpr
						LEFT JOIN amazon_product_info_extended apie ON adpr.advertisedAsin = apie.asin
						AND adpr.market = apie.market
					WHERE
						adpr.DATE >= '{start_date}'
						AND adpr.DATE <= '{end_date}'
						AND adpr.market = '{market}'
						AND adpr.campaignName LIKE 'DeepBI_%%'
						AND apie.asin IN %(column1_values1)s
					GROUP BY
						adpr.market
					) AS sp
					LEFT JOIN (
					SELECT
					CASE
						WHEN
							apie.parent_asins = '' THEN
								CONCAT( apie.asin, '(asin)' ) ELSE apie.parent_asins
								END AS parent_asins_or_asin,
							adpr.market,
							adpr.DATE,
							SUM( cost ) AS sum_cost,
							SUM( sales ) AS sum_sales
						FROM
							amazon_advertised_product_reports_sd adpr
							LEFT JOIN amazon_product_info_extended apie ON adpr.promotedAsin = apie.asin
							AND adpr.market = apie.market
						WHERE
							adpr.DATE >= '{start_date}'
							AND adpr.DATE <= '{end_date}'
							AND adpr.campaignName LIKE 'DeepBI_%%'
							AND apie.asin IN %(column1_values1)s
						GROUP BY
							adpr.market
						) AS sd ON sp.market = sd.market
					WHERE
						sp.market = '{market}'
					ORDER BY
						sp.DATE
					) AS deepbi_order ON all_order.market = deepbi_order.国家
            LEFT JOIN (-- 计算广告数据
					SELECT
						sp.market AS 国家,
						sp.parent_asins_or_asin,
						sp.DATE,
						( COALESCE ( sp.sum_cost, 0 ) ) AS DeepBIsp计划花费,
						( COALESCE ( sp.sum_sales, 0 ) ) AS DeepBIsp计划销量,
						ROUND( ( COALESCE ( sp.sum_cost, 0 ) ) / ( COALESCE ( sp.sum_sales, 0 ) ), 4 ) AS spacos
					FROM
						(
						SELECT
						CASE

							WHEN
								apie.parent_asins = '' THEN
									CONCAT( apie.asin, '(asin)' ) ELSE apie.parent_asins
									END AS parent_asins_or_asin,
								adpr.market,
								adpr.DATE,
								SUM( cost ) AS sum_cost,
								SUM( sales7d ) AS sum_sales
							FROM
								amazon_advertised_product_reports_sp adpr
								LEFT JOIN amazon_product_info_extended apie ON adpr.advertisedAsin = apie.asin
								AND adpr.market = apie.market
							WHERE
								adpr.DATE >= '{start_date}'
								AND adpr.DATE <= '{end_date}'
								AND adpr.market = '{market}'
								AND adpr.campaignName LIKE 'DeepBI_%%'
								AND apie.asin IN %(column1_values1)s
							GROUP BY
								adpr.market
							) AS sp
						WHERE
							sp.market = '{market}'
						ORDER BY
						sp.DATE
	) AS deepbi_sp_order ON all_order.market = deepbi_sp_order.国家;
             """
            df = pd.read_sql(query, con=conn, params={'column1_values1': asin_info})
            query1 = f"""
SELECT
CONCAT( DATE_FORMAT( '{start_date1}', '%%m.%%d' ), '-', DATE_FORMAT( '{end_date1}', '%%m.%%d' ) ) AS 日期,
--         COALESCE(ad_order.广告总销售额, 0) AS 广告总销售额,
	ROUND( all_order.总销售额 /( DATEDIFF( '{end_date1}', '{start_date1}' )+ 1 ), 2 ) AS 托管listing日均总销售额,
	ROUND( ad_order.广告总销售额 /( DATEDIFF( '{end_date1}', '{start_date1}' )+ 1 ), 2 ) AS 托管listing日均广告销售额,
--         COALESCE(ad_order.广告总花费, 0) AS 广告总花费,
    COALESCE(ad_order.广告总花费/( DATEDIFF( '{end_date1}', '{start_date1}' )+ 1 ), 0) AS 托管listing日均广告总花费,
    CONCAT(ROUND(ad_order.广告总ACOS * 100, 2), '%%') AS 广告总ACOS,
    CONCAT(ROUND(((COALESCE(ad_order.广告总花费, 0) / all_order.总销售额) * 100), 2), '%%') AS tacos,
	ROUND( COALESCE ( deepbi_order.DeepBI计划销量, 0 )/( DATEDIFF( '{end_date1}', '{start_date1}' )+ 1 ), 2 ) AS DeepBI计划日均销售额,
    COALESCE(deepbi_order.DeepBI计划花费/( DATEDIFF( '{end_date1}', '{start_date1}' )+ 1 ), 0) AS DeepBI计划日均花费,
	CONCAT( ROUND( COALESCE ( deepbi_order.新开计划acos, 0 ) * 100, 2 ), '%%' ) AS DeepBI计划ACOS,
	CONCAT( ROUND( COALESCE ((( deepbi_order.DeepBI计划销量 / ad_order.广告总销售额 ) * 100 ), 0 ), 2 ), '%%' ) AS DeepBI计划销售额占比,
--         ROUND((ad_order.广告总销售额 - COALESCE(deepbi_order.DeepBI计划销量, 0)), 2) AS 旧计划销售额,
--         ROUND((ad_order.广告总花费 - COALESCE(deepbi_order.DeepBI计划花费, 0)), 2) AS 旧计划花费,
--         CONCAT(ROUND(((ad_order.广告总花费 - COALESCE(deepbi_order.DeepBI计划花费, 0)) / (ad_order.广告总销售额 - COALESCE(deepbi_order.DeepBI计划销量, 0))) * 100, 2),'%%') AS 旧计划acos,
--         CONCAT(ROUND(((1 - COALESCE(deepbi_order.DeepBI计划销量, 0) / ad_order.广告总销售额) * 100), 2), '%%') AS 旧计划销量占比,
--         all_order.总销售额 as 总销售额,
--         COALESCE(ad_order.广告总销售额, 0) AS 广告销售额,
--         ROUND((all_order.总销售额 - COALESCE(ad_order.广告总销售额, 0)),2) AS 自然销售额,
--         CONCAT(ROUND(((1 - COALESCE(ad_order.广告总销售额, 0) / all_order.总销售额) * 100), 2),'%%') AS 自然销售额比例,
--         CONCAT(ROUND(((COALESCE(ad_order.广告总花费, 0) / all_order.总销售额) * 100), 2), '%%') AS tacos,
	CONCAT( ROUND( COALESCE (( deepbi_sp_order.DeepBIsp计划销量 / deepbi_order.DeepBI计划销量 )* 100, 0 ), 2 ), '%%' ) AS DeepBI_SP广告占比,
	CONCAT( ROUND( COALESCE (( 1 - deepbi_sp_order.DeepBIsp计划销量 / deepbi_order.DeepBI计划销量 )* 100, 0 ), 2 ), '%%' ) AS DeepBI_SD广告占比
FROM
(
SELECT
    sales_channel AS 国家,
    '{market}' AS market,
CASE

        WHEN apie.parent_asins = '' THEN
        CONCAT( apie.asin, '(asin)' ) ELSE apie.parent_asins
    END AS parent_asins_or_asin,
    DATE(
    CONVERT_TZ( purchase_date, '+08:00', '{self.load_config_info()['timezone_offset']}' )) AS event_date,
    ROUND( SUM( item_price ), 2 ) AS 总销售额
FROM
    amazon_get_flat_file_all_orders_data_by_last_update_general agffa
    LEFT JOIN amazon_product_info_extended apie ON agffa.asin = apie.asin
WHERE
    CONVERT_TZ( purchase_date, '+08:00', '{self.load_config_info()['timezone_offset']}' ) >= '{start_date1}'
    AND CONVERT_TZ( purchase_date, '+08:00', '{self.load_config_info()['timezone_offset']}' ) < '{end_date1}' + INTERVAL 1 DAY
    AND sales_channel = '{self.load_config_info()['sales_channel']}'
    AND apie.market = '{market}'
    AND apie.asin IN %(column1_values1)s
GROUP BY
    sales_channel
) AS all_order
LEFT JOIN (-- 计算广告数据
SELECT
    sp.market AS 国家,
    sp.parent_asins_or_asin,
    sp.DATE,
    (
    COALESCE ( sp.sum_cost, 0 ) + COALESCE ( sd.sum_cost, 0 )) AS 广告总花费,
    (
    COALESCE ( sp.sum_sales, 0 ) + COALESCE ( sd.sum_sales, 0 )) AS 广告总销售额,
    ROUND(
        (
            COALESCE ( sp.sum_cost, 0 ) + COALESCE ( sd.sum_cost, 0 )) / (
        COALESCE ( sp.sum_sales, 0 ) + COALESCE ( sd.sum_sales, 0 )),
        4
    ) AS 广告总ACOS
FROM
    (
    SELECT
    CASE

        WHEN
            apie.parent_asins = '' THEN
                CONCAT( apie.asin, '(asin)' ) ELSE apie.parent_asins
                END AS parent_asins_or_asin,
            adpr.market,
            adpr.DATE,
            SUM( cost ) AS sum_cost,
            SUM( sales7d ) AS sum_sales
        FROM
            amazon_advertised_product_reports_sp adpr
            LEFT JOIN amazon_product_info_extended apie ON adpr.advertisedAsin = apie.asin
            AND adpr.market = apie.market
        WHERE
            adpr.DATE >= '{start_date1}'
            AND adpr.DATE <= '{end_date1}'
            AND adpr.market = '{market}'
            AND apie.asin IN %(column1_values1)s
        GROUP BY
            adpr.market
        ) AS sp
        LEFT JOIN (
        SELECT
        CASE

            WHEN
                apie.parent_asins = '' THEN
                    CONCAT( apie.asin, '(asin)' ) ELSE apie.parent_asins
                    END AS parent_asins_or_asin,
                adpr.market,
                adpr.DATE,
                SUM( cost ) AS sum_cost,
                SUM( sales ) AS sum_sales
            FROM
                amazon_advertised_product_reports_sd adpr
                LEFT JOIN amazon_product_info_extended apie ON adpr.promotedAsin = apie.asin
                AND adpr.market = apie.market
            WHERE
                adpr.DATE >= '{start_date1}'
                AND adpr.DATE <= '{end_date1}'
                AND apie.asin IN %(column1_values1)s
            GROUP BY
                adpr.market
            ) AS sd ON sd.market = sp.market
        WHERE
            sp.market = '{market}'
        ORDER BY
            sp.DATE
        ) AS ad_order ON all_order.market = ad_order.国家
        LEFT JOIN (-- 计算广告数据
        SELECT
            sp.market AS 国家,
            sp.parent_asins_or_asin,
            sp.DATE,
            (
            COALESCE ( sp.sum_cost, 0 ) + COALESCE ( sd.sum_cost, 0 )) AS DeepBI计划花费,
            (
            COALESCE ( sp.sum_sales, 0 ) + COALESCE ( sd.sum_sales, 0 )) AS DeepBI计划销量,
            ROUND(
                (
                    COALESCE ( sp.sum_cost, 0 ) + COALESCE ( sd.sum_cost, 0 )) / (
                COALESCE ( sp.sum_sales, 0 ) + COALESCE ( sd.sum_sales, 0 )),
                4
            ) AS 新开计划acos
        FROM
            (
            SELECT
            CASE

                WHEN
                    apie.parent_asins = '' THEN
                        CONCAT( apie.asin, '(asin)' ) ELSE apie.parent_asins
                        END AS parent_asins_or_asin,
                    adpr.market,
                    adpr.DATE,
                    SUM( cost ) AS sum_cost,
                    SUM( sales7d ) AS sum_sales
                FROM
                    amazon_advertised_product_reports_sp adpr
                    LEFT JOIN amazon_product_info_extended apie ON adpr.advertisedAsin = apie.asin
                    AND adpr.market = apie.market
                WHERE
                    adpr.DATE >= '{start_date1}'
                    AND adpr.DATE <= '{end_date1}'
                    AND adpr.market = '{market}'
                    AND adpr.campaignName LIKE 'DeepBI_%%'
                    AND apie.asin IN %(column1_values1)s
                GROUP BY
                    adpr.market
                ) AS sp
                LEFT JOIN (
                SELECT
                CASE

                    WHEN
                        apie.parent_asins = '' THEN
                            CONCAT( apie.asin, '(asin)' ) ELSE apie.parent_asins
                            END AS parent_asins_or_asin,
                        adpr.market,
                        adpr.DATE,
                        SUM( cost ) AS sum_cost,
                        SUM( sales ) AS sum_sales
                    FROM
                        amazon_advertised_product_reports_sd adpr
                        LEFT JOIN amazon_product_info_extended apie ON adpr.promotedAsin = apie.asin
                        AND adpr.market = apie.market
                    WHERE
                        adpr.DATE >= '{start_date1}'
                        AND adpr.DATE <= '{end_date1}'
                        AND adpr.campaignName LIKE 'DeepBI_%%'
                        AND apie.asin IN %(column1_values1)s
                    GROUP BY
                        adpr.market
                    ) AS sd ON sd.market = sp.market
                WHERE
                    sp.market = '{market}'
                ORDER BY
                    sp.DATE
                ) AS deepbi_order ON all_order.market = deepbi_order.国家
                LEFT JOIN (-- 计算广告数据
                SELECT
                    sp.market AS 国家,
                    sp.parent_asins_or_asin,
                    sp.DATE,
                    ( COALESCE ( sp.sum_cost, 0 ) ) AS DeepBIsp计划花费,
                    ( COALESCE ( sp.sum_sales, 0 ) ) AS DeepBIsp计划销量,
                    ROUND( ( COALESCE ( sp.sum_cost, 0 ) ) / ( COALESCE ( sp.sum_sales, 0 ) ), 4 ) AS 新开计划acos
                FROM
                    (
                    SELECT
                    CASE

                        WHEN
                            apie.parent_asins = '' THEN
                                CONCAT( apie.asin, '(asin)' ) ELSE apie.parent_asins
                                END AS parent_asins_or_asin,
                            adpr.market,
                            adpr.DATE,
                            SUM( cost ) AS sum_cost,
                            SUM( sales7d ) AS sum_sales
                        FROM
                            amazon_advertised_product_reports_sp adpr
                            LEFT JOIN amazon_product_info_extended apie ON adpr.advertisedAsin = apie.asin
                            AND adpr.market = apie.market
                        WHERE
                            adpr.DATE >= '{start_date1}'
                            AND adpr.DATE <= '{end_date1}'
                            AND adpr.market = '{market}'
                            AND adpr.campaignName LIKE 'DeepBI_%%'
                            AND apie.asin IN %(column1_values1)s
                        GROUP BY
                            adpr.market
                        ) AS sp
                    WHERE
                        sp.market = '{market}'
                    ORDER BY
                    sp.DATE
) AS deepbi_sp_order ON all_order.market = deepbi_sp_order.国家;
                         """
            df1 = pd.read_sql(query1, con=conn, params={'column1_values1': asin_info})
            merged_df = pd.concat([df1, df], ignore_index=True)
            output_filename = f'{self.brand}_{market}_{end_date}_managed_listing_current_data.csv'
            csv_path = os.path.join(get_export_path(), output_filename)
            merged_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            print("get_managed_listing_current_data successfully!")
            return csv_path
        except Exception as error:
            print("get_managed_listing_current_data Error while query data:", error)

    def get_listing_comparison_data(self, market, start_date, end_date):
        # 低于 平均ACOS值 30% 以上的  campaign 广告活动
        # 建议执行的操作：预算提升30%
        try:
            conn = self.conn
            # 暂时忽略了market转化 US
            query = f"""
SELECT
        all_order.event_date as 日期,
        COALESCE(deepbi_order.DeepBI计划销量, 0) AS DeepBI计划销售额,
        COALESCE(deepbi_order.DeepBI计划花费, 0) AS DeepBI计划花费,
        CONCAT(ROUND(COALESCE(deepbi_order.新开计划acos, 0) * 100, 2), '%') AS DeepBI计划acos,
        CONCAT(ROUND(COALESCE(((deepbi_order.DeepBI计划销量 / ad_order.广告总销售额) * 100), 0), 2), '%') AS DeepBI计划销售额占比,
        ROUND((ad_order.广告总销售额 - COALESCE(deepbi_order.DeepBI计划销量, 0)), 2) AS 旧计划销售额,
        ROUND((ad_order.广告总花费 - COALESCE(deepbi_order.DeepBI计划花费, 0)), 2) AS 旧计划花费,
        CONCAT(ROUND(((ad_order.广告总花费 - COALESCE(deepbi_order.DeepBI计划花费, 0)) / (ad_order.广告总销售额 - COALESCE(deepbi_order.DeepBI计划销量, 0))) * 100, 2), '%') AS 旧计划acos,
        CONCAT(ROUND(((1 - COALESCE(deepbi_order.DeepBI计划销量, 0) / ad_order.广告总销售额) * 100), 2), '%') AS 旧计划销量占比
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
                    AND DATE <= '{end_date}'
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
                    AND DATE <= '{end_date}'
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
                    AND DATE <= '{end_date}'
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
                    AND DATE <= '{end_date}'
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
                    AND DATE <= '{end_date}'
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
                    AND DATE <= '{end_date}'
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
            output_filename = f'{self.brand}_{market}_{end_date}_listing_comparison_data.csv'
            csv_path = os.path.join(get_export_path(), output_filename)
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            print("get_listing_comparison_data successfully!")
            return csv_path
        except Exception as error:
            print("get_listing_comparison_data Error while query data:", error)

    def get_listing_comparison_data_summary(self, market, start_date, end_date):
        try:
            conn = self.conn
            # 暂时忽略了market转化 US
            query = f"""
SELECT
        all_order.event_date as 日期,
        COALESCE(deepbi_order.DeepBI计划销量, 0) AS DeepBI计划销售额,
        COALESCE(deepbi_order.DeepBI计划花费, 0) AS DeepBI计划花费,
        CONCAT(ROUND(COALESCE(deepbi_order.新开计划acos, 0) * 100, 2), '%%') AS DeepBI计划acos,
        CONCAT(ROUND(COALESCE(((deepbi_order.DeepBI计划销量 / ad_order.广告总销售额) * 100), 0), 2), '%%') AS DeepBI计划销售额占比,
        ROUND((ad_order.广告总销售额 - COALESCE(deepbi_order.DeepBI计划销量, 0)), 2) AS 旧计划销售额,
        ROUND((ad_order.广告总花费 - COALESCE(deepbi_order.DeepBI计划花费, 0)), 2) AS 旧计划花费,
        CONCAT(ROUND(((ad_order.广告总花费 - COALESCE(deepbi_order.DeepBI计划花费, 0)) / (ad_order.广告总销售额 - COALESCE(deepbi_order.DeepBI计划销量, 0))) * 100, 2), '%%') AS 旧计划acos,
        CONCAT(ROUND(((1 - COALESCE(deepbi_order.DeepBI计划销量, 0) / ad_order.广告总销售额) * 100), 2), '%%') AS 旧计划销量占比
    FROM
    (
        -- 计算总销售额
        SELECT
            sales_channel AS 国家,
            '{market}' AS market,
            DATE(CONVERT_TZ(purchase_date ,'+08:00', '{self.load_summary_info()['timezone_offset']}')) AS event_date,
            ROUND(SUM(item_price), 2) AS 总销售额
        FROM
            amazon_get_flat_file_all_orders_data_by_last_update_general
        WHERE
            CONVERT_TZ(purchase_date ,'+08:00', '{self.load_summary_info()['timezone_offset']}') >= '{start_date}'
            AND CONVERT_TZ(purchase_date ,'+08:00', '{self.load_summary_info()['timezone_offset']}') < '{end_date}' + INTERVAL 1 DAY
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
                    '{market}' AS market,
                    DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales7d) AS sum_sales
                FROM
                    amazon_campaign_reports_sp
                WHERE
                    DATE >= '{start_date}'
                    AND DATE <= '{end_date}'
                    AND market IN %(column2_values2)s
                GROUP BY
                    DATE
            ) AS sp
        LEFT JOIN (
                SELECT
                    '{market}' AS market,
                    DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales) AS sum_sales
                FROM
                    amazon_campaign_reports_sd
                WHERE
                    DATE >= '{start_date}'
                    AND DATE <= '{end_date}'
                    AND market IN %(column2_values2)s
                GROUP BY
                    DATE
            ) AS sd ON sd.market = sp.market AND sd.DATE = sp.DATE
        LEFT JOIN (
                SELECT
                    '{market}' AS market,
                    DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales) AS sum_sales
                FROM
                    amazon_campaign_reports_sb
                WHERE
                    DATE >= '{start_date}'
                    AND DATE <= '{end_date}'
                    AND market IN %(column2_values2)s
                GROUP BY
                    DATE
            ) AS sb ON sb.market = sp.market AND sb.DATE = sp.DATE
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
                    '{market}' AS market,
                    DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales7d) AS sum_sales
                FROM
                    amazon_campaign_reports_sp
                WHERE
                    DATE >= '{start_date}'
                    AND DATE <= '{end_date}'
                    AND campaignName LIKE 'DeepBI_%%'
                    AND market IN %(column2_values2)s
                GROUP BY
                    DATE
            ) AS sp
        LEFT JOIN (
                SELECT
                    '{market}' AS market,
                    DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales) AS sum_sales
                FROM
                    amazon_campaign_reports_sd
                WHERE
                    DATE >= '{start_date}'
                    AND DATE <= '{end_date}'
                                        AND campaignName LIKE 'DeepBI_%%'
                                        AND market IN %(column2_values2)s
                GROUP BY
                    DATE
            ) AS sd ON sd.market = sp.market AND sd.DATE = sp.DATE
        LEFT JOIN (
                SELECT
                    '{market}' AS market,
                    DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales) AS sum_sales
                FROM
                    amazon_campaign_reports_sb
                WHERE
                    DATE >= '{start_date}'
                    AND DATE <= '{end_date}'
                                        AND campaignName LIKE 'DeepBI_%%'
                                        AND market IN %(column2_values2)s
                GROUP BY
                    DATE
            ) AS sb ON sb.market = sp.market AND sb.DATE = sp.DATE
        ORDER BY
            sp.DATE
    ) AS deepbi_order ON all_order.event_date = deepbi_order.DATE;
             """
            df = pd.read_sql(query, con=conn, params={'column1_values1': self.load_summary_info()['sales_channel'], 'column2_values2': self.load_summary_info()['country']})
            output_filename = f'{self.brand}_{market}_{end_date}_listing_comparison_summary_data.csv'
            csv_path = os.path.join(get_export_path(), output_filename)
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            print("get_listing_comparison_data_summary successfully!")
            return csv_path
        except Exception as error:
            print("get_listing_comparison_data_summary Error while query data:", error)

    def get_managed_listing_comparison_data(self, market,start_date,end_date,asin_info):
        # 低于 平均ACOS值 30% 以上的  campaign 广告活动
        # 建议执行的操作：预算提升30%
        try:
            conn = self.conn
            # 暂时忽略了market转化 US
            query = f"""
SELECT
	all_order.event_date AS 总销售日期,
    COALESCE(all_order.总销售额, 0) AS listing总销售额,
	COALESCE(ad_order.广告总销售额, 0) AS listing广告总销售额,
	COALESCE(all_order.总销售额, 0)-COALESCE(ad_order.广告总销售额, 0) AS listing自然销售额,
	COALESCE ( deepbi_sp_order.DeepBIsp计划销量, 0 ) AS DeepBISP计划销售额,
	COALESCE ( deepbi_sp_order.DeepBIsp计划花费, 0 ) AS DeepBISP计划花费,
	CONCAT( ROUND( COALESCE ( deepbi_sp_order.spacos, 0 ) * 100, 2 ), '%%' ) AS DeepBISPAcos,
	CONCAT( ROUND( COALESCE ((( deepbi_sp_order.DeepBIsp计划销量 / deepbi_order.DeepBI计划销量 ) * 100 ), 0 ), 2 ), '%%' ) AS DeepBISP占比,
	COALESCE ( deepbi_sd_order.DeepBIsd计划销量, 0 ) AS DeepBISD计划销售额,
	COALESCE ( deepbi_sd_order.DeepBIsd计划花费, 0 ) AS DeepBISD计划花费,
	CONCAT( ROUND( COALESCE ( deepbi_sd_order.sdacos, 0 ) * 100, 2 ), '%%' ) AS DeepBISDAcos,
	CONCAT( ROUND( COALESCE ((( deepbi_sd_order.DeepBIsd计划销量 / deepbi_order.DeepBI计划销量 ) * 100 ), 0 ), 2 ), '%%' ) AS DeepBISD占比,
	COALESCE ( deepbi_order.DeepBI计划销量, 0 ) AS DeepBI计划销售额,
	COALESCE ( deepbi_order.DeepBI计划花费, 0 ) AS DeepBI计划花费,
	CONCAT( ROUND( COALESCE ( deepbi_order.新开计划acos, 0 ) * 100, 2 ), '%%' ) AS DeepBI计划Acos,
	CONCAT( ROUND( COALESCE ((( deepbi_order.DeepBI计划销量 / ad_order.广告总销售额 ) * 100 ), 0 ), 2 ), '%%' ) AS DeepBI计划销量占比,
	ROUND(( ad_order.广告总销售额 - COALESCE ( deepbi_order.DeepBI计划销量, 0 )), 2 ) AS 旧计划销售额,
	ROUND(( ad_order.广告总花费 - COALESCE ( deepbi_order.DeepBI计划花费, 0 )), 2 ) AS 旧计划花费,
	CONCAT(
		ROUND(((
					ad_order.广告总花费 - COALESCE ( deepbi_order.DeepBI计划花费, 0 )) / (
					ad_order.广告总销售额 - COALESCE ( deepbi_order.DeepBI计划销量, 0 ))) * 100,
			2
		),
		'%%'
	) AS 旧计划Acos,
	CONCAT( ROUND((( 1 - COALESCE ( deepbi_order.DeepBI计划销量, 0 ) / ad_order.广告总销售额 ) * 100 ), 2 ), '%%' ) AS 旧计划销量占比

FROM
	(
	SELECT
		sales_channel AS 国家,
	CASE

			WHEN apie.parent_asins = '' THEN
			CONCAT( apie.asin, '(asin)' ) ELSE apie.parent_asins
		END AS parent_asins_or_asin,
		DATE(
		CONVERT_TZ( purchase_date, '+08:00', '{self.load_config_info()['timezone_offset']}' )) AS event_date,
		ROUND( SUM( item_price ), 2 ) AS 总销售额
	FROM
		amazon_get_flat_file_all_orders_data_by_last_update_general agffa
		LEFT JOIN amazon_product_info_extended apie ON agffa.asin = apie.asin
	WHERE
		CONVERT_TZ( purchase_date, '+08:00', '{self.load_config_info()['timezone_offset']}' ) >= '{start_date}'
		AND CONVERT_TZ( purchase_date, '+08:00', '{self.load_config_info()['timezone_offset']}' ) < '{end_date}' + INTERVAL 1 DAY
		AND sales_channel = '{self.load_config_info()['sales_channel']}'
		AND apie.market = '{market}'
		AND apie.asin IN %(column1_values1)s
	GROUP BY
		sales_channel,
		event_date
	ORDER BY
		event_date
	) AS all_order
	LEFT JOIN (-- 计算广告数据
	SELECT
		sp.market AS 国家,
		sp.parent_asins_or_asin,
		sp.DATE,
		(
		COALESCE ( sp.sum_cost, 0 ) + COALESCE ( sd.sum_cost, 0 )) AS 广告总花费,
		(
		COALESCE ( sp.sum_sales, 0 ) + COALESCE ( sd.sum_sales, 0 )) AS 广告总销售额,
		ROUND(
			(
				COALESCE ( sp.sum_cost, 0 ) + COALESCE ( sd.sum_cost, 0 )) / (
			COALESCE ( sp.sum_sales, 0 ) + COALESCE ( sd.sum_sales, 0 )),
			4
		) AS 广告总ACOS
	FROM
		(
		SELECT
		CASE

			WHEN
				apie.parent_asins = '' THEN
					CONCAT( apie.asin, '(asin)' ) ELSE apie.parent_asins
					END AS parent_asins_or_asin,
				adpr.market,
				adpr.DATE,
				SUM( cost ) AS sum_cost,
				SUM( sales7d ) AS sum_sales
			FROM
				amazon_advertised_product_reports_sp adpr
				LEFT JOIN amazon_product_info_extended apie ON adpr.advertisedAsin = apie.asin
				AND adpr.market = apie.market
			WHERE
				adpr.DATE >= '{start_date}'
				AND adpr.DATE <= '{end_date}'
				AND adpr.market = '{market}'
				AND apie.asin IN %(column1_values1)s
			GROUP BY
				adpr.market,
				DATE
			) AS sp
			LEFT JOIN (
			SELECT
			CASE

				WHEN
					apie.parent_asins = '' THEN
						CONCAT( apie.asin, '(asin)' ) ELSE apie.parent_asins
						END AS parent_asins_or_asin,
					adpr.market,
					adpr.DATE,
					SUM( cost ) AS sum_cost,
					SUM( sales ) AS sum_sales
				FROM
					amazon_advertised_product_reports_sd adpr
					LEFT JOIN amazon_product_info_extended apie ON adpr.promotedAsin = apie.asin
					AND adpr.market = apie.market
				WHERE
					adpr.DATE >= '{start_date}'
					AND adpr.DATE <= '{end_date}'
					AND apie.asin IN %(column1_values1)s
				GROUP BY
					adpr.market,
					DATE
				) AS sd ON sd.market = sp.market
				AND sd.DATE = sp.DATE
			WHERE
				sp.market = '{market}'
			ORDER BY
				sp.DATE
			) AS ad_order ON all_order.event_date = ad_order.DATE
			LEFT JOIN (-- 计算广告数据
			SELECT
				sp.market AS 国家,
				sp.parent_asins_or_asin,
				sp.DATE,
				(
				COALESCE ( sp.sum_cost, 0 ) + COALESCE ( sd.sum_cost, 0 )) AS DeepBI计划花费,
				(
				COALESCE ( sp.sum_sales, 0 ) + COALESCE ( sd.sum_sales, 0 )) AS DeepBI计划销量,
				ROUND(
					(
						COALESCE ( sp.sum_cost, 0 ) + COALESCE ( sd.sum_cost, 0 )) / (
					COALESCE ( sp.sum_sales, 0 ) + COALESCE ( sd.sum_sales, 0 )),
					4
				) AS 新开计划acos
			FROM
				(
				SELECT
				CASE

					WHEN
						apie.parent_asins = '' THEN
							CONCAT( apie.asin, '(asin)' ) ELSE apie.parent_asins
							END AS parent_asins_or_asin,
						adpr.market,
						adpr.DATE,
						SUM( cost ) AS sum_cost,
						SUM( sales7d ) AS sum_sales
					FROM
						amazon_advertised_product_reports_sp adpr
						LEFT JOIN amazon_product_info_extended apie ON adpr.advertisedAsin = apie.asin
						AND adpr.market = apie.market
					WHERE
						adpr.DATE >= '{start_date}'
						AND adpr.DATE <= '{end_date}'
						AND adpr.market = '{market}'
						AND adpr.campaignName LIKE 'DeepBI_%%'
						AND apie.asin IN %(column1_values1)s
					GROUP BY
						adpr.market,
						DATE
					) AS sp
					LEFT JOIN (
					SELECT
					CASE

						WHEN
							apie.parent_asins = '' THEN
								CONCAT( apie.asin, '(asin)' ) ELSE apie.parent_asins
								END AS parent_asins_or_asin,
							adpr.market,
							adpr.DATE,
							SUM( cost ) AS sum_cost,
							SUM( sales ) AS sum_sales
						FROM
							amazon_advertised_product_reports_sd adpr
							LEFT JOIN amazon_product_info_extended apie ON adpr.promotedAsin = apie.asin
							AND adpr.market = apie.market
						WHERE
							adpr.DATE >= '{start_date}'
							AND adpr.DATE <= '{end_date}'
							AND adpr.campaignName LIKE 'DeepBI_%%'
							AND apie.asin IN %(column1_values1)s
						GROUP BY
							adpr.market,
							DATE
						) AS sd ON sd.market = sp.market
						AND sd.DATE = sp.DATE
					WHERE
						sp.market = '{market}'
					ORDER BY
					sp.DATE
	) AS deepbi_order ON all_order.event_date = deepbi_order.DATE
	LEFT JOIN (-- 计算广告数据
			SELECT
				sp.market AS 国家,
				sp.parent_asins_or_asin,
				sp.DATE,
				(
				COALESCE ( sp.sum_cost, 0 )) AS DeepBIsp计划花费,
				(
				COALESCE ( sp.sum_sales, 0 )) AS DeepBIsp计划销量,
				ROUND(
					(
						COALESCE ( sp.sum_cost, 0 )) / (
					COALESCE ( sp.sum_sales, 0 )),
					4
				) AS spacos
			FROM
				(
				SELECT
				CASE

					WHEN
						apie.parent_asins = '' THEN
							CONCAT( apie.asin, '(asin)' ) ELSE apie.parent_asins
							END AS parent_asins_or_asin,
						adpr.market,
						adpr.DATE,
						SUM( cost ) AS sum_cost,
						SUM( sales7d ) AS sum_sales
					FROM
						amazon_advertised_product_reports_sp adpr
						LEFT JOIN amazon_product_info_extended apie ON adpr.advertisedAsin = apie.asin
						AND adpr.market = apie.market
					WHERE
						adpr.DATE >= '{start_date}'
						AND adpr.DATE <= '{end_date}'
						AND adpr.market = '{market}'
						AND adpr.campaignName LIKE 'DeepBI_%%'
						AND apie.asin IN %(column1_values1)s
					GROUP BY
						adpr.market,
						DATE
					) AS sp
					WHERE
						sp.market = '{market}'
					ORDER BY
					sp.DATE
	) AS deepbi_sp_order ON all_order.event_date = deepbi_sp_order.DATE
LEFT JOIN (-- 计算广告数据
			SELECT
				sp.market AS 国家,
				sp.parent_asins_or_asin,
				sp.DATE,
				(
				COALESCE ( sp.sum_cost, 0 )) AS DeepBIsd计划花费,
				(
				COALESCE ( sp.sum_sales, 0 )) AS DeepBIsd计划销量,
				ROUND(
					(
						COALESCE ( sp.sum_cost, 0 )) / (
					COALESCE ( sp.sum_sales, 0 )),
					4
				) AS sdacos
			FROM
				(
				SELECT
					CASE

						WHEN
							apie.parent_asins = '' THEN
								CONCAT( apie.asin, '(asin)' ) ELSE apie.parent_asins
								END AS parent_asins_or_asin,
							adpr.market,
							adpr.DATE,
							SUM( cost ) AS sum_cost,
							SUM( sales ) AS sum_sales
						FROM
							amazon_advertised_product_reports_sd adpr
							LEFT JOIN amazon_product_info_extended apie ON adpr.promotedAsin = apie.asin
							AND adpr.market = apie.market
						WHERE
							adpr.DATE >= '{start_date}'
							AND adpr.DATE <= '{end_date}'
							AND adpr.campaignName LIKE 'DeepBI_%%'
							AND apie.asin IN %(column1_values1)s
						GROUP BY
							adpr.market,
							DATE
					) AS sp
					WHERE
						sp.market = '{market}'
					ORDER BY
					sp.DATE
	) AS deepbi_sd_order ON all_order.event_date = deepbi_sd_order.DATE
             """
            df = pd.read_sql(query, con=conn, params={'column1_values1': asin_info})
            output_filename = f'{self.brand}_{market}_{end_date}_managed_listing_comparison_data.csv'
            csv_path = os.path.join(get_export_path(), output_filename)
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            print("get_managed_listing_comparison_data successfully!")
            return csv_path
        except Exception as error:
            print("get_managed_listing_comparison_data Error while query data:", error)

    def get_listing_ditial_data(self, market,start_date,end_date):
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
                        CONCAT( apie.asin, '(asin)' ) ELSE apie.parent_asins
                END AS parent_asins_or_asin,
                DATE(
                CONVERT_TZ( purchase_date, '+08:00', '{self.load_config_info()['timezone_offset']}' )) AS event_date,
                ROUND( SUM( item_price ), 2 ) AS 总销售额
        FROM
                amazon_get_flat_file_all_orders_data_by_last_update_general agffa
                LEFT JOIN amazon_product_info_extended apie ON agffa.asin = apie.asin
        WHERE
                CONVERT_TZ( purchase_date, '+08:00', '{self.load_config_info()['timezone_offset']}' ) >= '{start_date}'
                AND CONVERT_TZ( purchase_date, '+08:00', '{self.load_config_info()['timezone_offset']}' ) < '{end_date}' + INTERVAL 1 DAY
                AND sales_channel = '{self.load_config_info()['sales_channel']}'
                AND apie.market = '{market}'
        GROUP BY
                sales_channel,
        CASE

                        WHEN apie.parent_asins = '' THEN
                        CONCAT( apie.asin, '(asin)' ) ELSE apie.parent_asins
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
                    adpr.DATE >= '{start_date}'
                    AND adpr.DATE <= '{end_date}'
                GROUP BY
                    adpr.market,
                                                                                CASE WHEN apie.parent_asins = '' THEN CONCAT(apie.asin,'(asin)') ELSE apie.parent_asins END
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
                    adpr.DATE >= '{start_date}'
                    AND adpr.DATE <= '{end_date}'
                GROUP BY
                    adpr.market,
                                                                                CASE WHEN apie.parent_asins = '' THEN CONCAT(apie.asin,'(asin)') ELSE apie.parent_asins END
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
                    adpr.DATE >= '{start_date}'
                    AND adpr.DATE <= '{end_date}'
                GROUP BY
                    adpr.market,
                                                                                CASE WHEN apie.parent_asins = '' THEN CONCAT(apie.asin,'(asin)') ELSE apie.parent_asins END
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
                    adpr.DATE >= '{start_date}'
                    AND adpr.DATE <= '{end_date}'
                GROUP BY
                    adpr.market,
                                                                                CASE WHEN apie.parent_asins = '' THEN CONCAT(apie.asin,'(asin)') ELSE apie.parent_asins END
            ) AS sp
        WHERE
            sp.market = '{market}'
        ORDER BY
            sp.DATE
)

SELECT
        a.parent_asins_or_asin as list,
        COALESCE(a.总销售额, 0) AS 总销售额,
        COALESCE(b.广告总销售额, 0) AS 广告销售额,
        COALESCE(b.广告总花费, 0) AS 广告花费,
        CONCAT(ROUND(b.广告总ACOS * 100, 2), '%') AS ACOS,
        COALESCE(c.广告总销售额, 0) AS SP广告销售额,
        COALESCE(c.广告总花费, 0) AS SP广告花费,
        CONCAT(ROUND(c.广告总ACOS * 100, 2), '%') AS SP_ACOS,
        CONCAT(ROUND(((COALESCE(c.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 2),'%') AS SP广告销售额占比,
        COALESCE(d.广告总销售额, 0) AS SD广告销售额,
        COALESCE(d.广告总花费, 0) AS SD广告花费,
        CONCAT(ROUND(d.广告总ACOS * 100, 2), '%') AS SD_ACOS,
        CONCAT(ROUND(((COALESCE(d.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 2),'%') AS SD广告销售额占比
FROM a
LEFT JOIN b ON a.parent_asins_or_asin = b.parent_asins_or_asin
LEFT JOIN c ON a.parent_asins_or_asin = c.parent_asins_or_asin
LEFT JOIN d ON a.parent_asins_or_asin = d.parent_asins_or_asin
ORDER BY
COALESCE(b.广告总销售额, 0) DESC

             """
            df = pd.read_sql(query, con=conn)
            # 计算汇总数据
            total_ad_sales = df['广告销售额'].sum()
            total_ad_cost = df['广告花费'].sum()
            total_sp_sales = df['SP广告销售额'].sum()
            total_sp_cost = df['SP广告花费'].sum()
            total_sd_sales = df['SD广告销售额'].sum()
            total_sd_cost = df['SD广告花费'].sum()

            # 计算汇总行的各项指标
            if total_sp_sales > 0:
                sp_acos = (total_sp_cost / total_sp_sales) * 100
                overall_acos = (total_ad_cost / total_ad_sales) * 100 if total_ad_sales > 0 else 0
                sp_sales_ratio = (total_sp_sales / total_ad_sales) * 100 if total_ad_sales > 0 else 0

            else:
                sp_acos = 0
                overall_acos = 0
                sp_sales_ratio = 0
            sd_acos = (total_sd_cost / total_sd_sales) * 100 if total_sd_sales > 0 else 0
            sd_sales_ratio = (total_sd_sales / total_ad_sales) * 100 if total_ad_sales > 0 else 0

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
                'SD广告销售额': round(total_sd_sales, 2),
                'SD广告花费': total_sd_cost,
                'SD_ACOS': f'{sd_acos:.2f}%',
                'SD广告销售额占比': f'{sd_sales_ratio:.2f}%',
            }

            # 将汇总数据行添加到 DataFrame
            summary_df = pd.DataFrame([summary_data])
            df = pd.concat([df, summary_df], ignore_index=True)
            output_filename = f'{self.brand}_{market}_{end_date}_listing_ditial_data.csv'
            csv_path = os.path.join(get_export_path(), output_filename)
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            print("get_listing_ditial_data successfully!")
            return csv_path
        except Exception as error:
            print("get_listing_ditial_data Error while query data:", error)

    def get_listing_ditial_data_summary(self, market, start_date, end_date):

        try:
            conn = self.conn
            # 暂时忽略了market转化 US
            query = f"""
 WITH a AS  (
        SELECT
                sales_channel AS 国家,
                '{market}' AS market,
        CASE

                        WHEN apie.parent_asins = '' THEN
                        CONCAT( apie.asin, '(asin)' ) ELSE apie.parent_asins
                END AS parent_asins_or_asin,
                DATE(
                CONVERT_TZ( purchase_date, '+08:00', '{self.load_summary_info()['timezone_offset']}' )) AS event_date,
                ROUND( SUM( item_price ), 2 ) AS 总销售额
        FROM
                amazon_get_flat_file_all_orders_data_by_last_update_general agffa
                LEFT JOIN amazon_product_info_extended apie ON agffa.asin = apie.asin
        WHERE
                CONVERT_TZ( purchase_date, '+08:00', '{self.load_summary_info()['timezone_offset']}' ) >= '{start_date}'
                AND CONVERT_TZ( purchase_date, '+08:00', '{self.load_summary_info()['timezone_offset']}' ) < '{end_date}' + INTERVAL 1 DAY
                AND sales_channel IN %(column1_values1)s
                AND apie.market = '{self.load_summary_info()['market']}'
        GROUP BY
        CASE

                        WHEN apie.parent_asins = '' THEN
                        CONCAT( apie.asin, '(asin)' ) ELSE apie.parent_asins
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
                                                                                CASE WHEN apie.parent_asins = '' THEN CONCAT(apie.asin,'(asin)') ELSE apie.parent_asins END AS parent_asins_or_asin,
                    '{market}' AS market,
                    adpr.DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales7d) AS sum_sales
                FROM
                    amazon_advertised_product_reports_sp adpr
                                                                LEFT JOIN
                                                                                amazon_product_info_extended apie ON adpr.advertisedAsin = apie.asin
                WHERE
                    adpr.DATE >= '{start_date}'
                    AND adpr.DATE <= '{end_date}'
                    AND adpr.market IN %(column2_values2)s
                    AND apie.market = '{self.load_summary_info()['market']}'
                GROUP BY
                                                                                CASE WHEN apie.parent_asins = '' THEN CONCAT(apie.asin,'(asin)') ELSE apie.parent_asins END
            ) AS sp
        LEFT JOIN (
                SELECT
                                                                                CASE WHEN apie.parent_asins = '' THEN CONCAT(apie.asin,'(asin)') ELSE apie.parent_asins END AS parent_asins_or_asin,
                    '{market}' AS market,
                    adpr.DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales) AS sum_sales
                FROM
                    amazon_advertised_product_reports_sd adpr
                                                                LEFT JOIN
                                                                                amazon_product_info_extended apie ON adpr.promotedAsin = apie.asin
                WHERE
                    adpr.DATE >= '{start_date}'
                    AND adpr.DATE <= '{end_date}'
                    AND adpr.market IN %(column2_values2)s
                    AND apie.market = '{self.load_summary_info()['market']}'
                GROUP BY
                                                                                CASE WHEN apie.parent_asins = '' THEN CONCAT(apie.asin,'(asin)') ELSE apie.parent_asins END
            ) AS sd ON sd.parent_asins_or_asin = sp.parent_asins_or_asin AND sd.market = sp.market
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
                                                                                CASE WHEN apie.parent_asins = '' THEN CONCAT(apie.asin,'(asin)') ELSE apie.parent_asins END AS parent_asins_or_asin,
                    '{market}' AS market,
                    adpr.DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales7d) AS sum_sales
                FROM
                    amazon_advertised_product_reports_sp adpr
                                                                LEFT JOIN
                                                                                amazon_product_info_extended apie ON adpr.advertisedAsin = apie.asin
                WHERE
                    adpr.DATE >= '{start_date}'
                    AND adpr.DATE <= '{end_date}'
                    AND adpr.market IN %(column2_values2)s
                    AND apie.market = '{self.load_summary_info()['market']}'
                GROUP BY
                                                                                CASE WHEN apie.parent_asins = '' THEN CONCAT(apie.asin,'(asin)') ELSE apie.parent_asins END
            ) AS sp
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
                                                                                CASE WHEN apie.parent_asins = '' THEN CONCAT(apie.asin,'(asin)') ELSE apie.parent_asins END AS parent_asins_or_asin,
                    '{market}' AS market,
                    adpr.DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales) AS sum_sales
                FROM
                    amazon_advertised_product_reports_sd adpr
                                                                LEFT JOIN
                                                                                amazon_product_info_extended apie ON adpr.promotedAsin = apie.asin
                WHERE
                    adpr.DATE >= '{start_date}'
                    AND adpr.DATE <= '{end_date}'
                    AND adpr.market IN %(column2_values2)s
                    AND apie.market = '{self.load_summary_info()['market']}'
                GROUP BY
                CASE WHEN apie.parent_asins = '' THEN CONCAT(apie.asin,'(asin)') ELSE apie.parent_asins END
            ) AS sp
        ORDER BY
            sp.DATE
)

SELECT
        a.parent_asins_or_asin as list,
        COALESCE(a.总销售额, 0) AS 总销售额,
        COALESCE(b.广告总销售额, 0) AS 广告销售额,
        COALESCE(b.广告总花费, 0) AS 广告花费,
        CONCAT(ROUND(b.广告总ACOS * 100, 2), '%%') AS ACOS,
        COALESCE(c.广告总销售额, 0) AS SP广告销售额,
        COALESCE(c.广告总花费, 0) AS SP广告花费,
        CONCAT(ROUND(c.广告总ACOS * 100, 2), '%%') AS SP_ACOS,
        CONCAT(ROUND(((COALESCE(c.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 2),'%%') AS SP广告销售额占比,
        COALESCE(d.广告总销售额, 0) AS SD广告销售额,
        COALESCE(d.广告总花费, 0) AS SD广告花费,
        CONCAT(ROUND(d.广告总ACOS * 100, 2), '%%') AS SD_ACOS,
        CONCAT(ROUND(((COALESCE(d.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 2),'%%') AS SD广告销售额占比
FROM a
LEFT JOIN b ON a.parent_asins_or_asin = b.parent_asins_or_asin
LEFT JOIN c ON a.parent_asins_or_asin = c.parent_asins_or_asin
LEFT JOIN d ON a.parent_asins_or_asin = d.parent_asins_or_asin
ORDER BY
COALESCE(b.广告总销售额, 0) DESC

             """
            df = pd.read_sql(query, con=conn, params={'column1_values1': self.load_summary_info()['sales_channel'], 'column2_values2': self.load_summary_info()['country']})
            # 计算汇总数据
            total_ad_sales = df['广告销售额'].sum()
            total_ad_cost = df['广告花费'].sum()
            total_sp_sales = df['SP广告销售额'].sum()
            total_sp_cost = df['SP广告花费'].sum()
            total_sd_sales = df['SD广告销售额'].sum()
            total_sd_cost = df['SD广告花费'].sum()

            # 计算汇总行的各项指标
            if total_sp_sales > 0:
                sp_acos = (total_sp_cost / total_sp_sales) * 100
                overall_acos = (total_ad_cost / total_ad_sales) * 100 if total_ad_sales > 0 else 0
                sp_sales_ratio = (total_sp_sales / total_ad_sales) * 100 if total_ad_sales > 0 else 0

            else:
                sp_acos = 0
                overall_acos = 0
                sp_sales_ratio = 0
            sd_acos = (total_sd_cost / total_sd_sales) * 100 if total_sd_sales > 0 else 0
            sd_sales_ratio = (total_sd_sales / total_ad_sales) * 100 if total_ad_sales > 0 else 0

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
                'SD广告销售额': round(total_sd_sales, 2),
                'SD广告花费': total_sd_cost,
                'SD_ACOS': f'{sd_acos:.2f}%',
                'SD广告销售额占比': f'{sd_sales_ratio:.2f}%',
            }

            # 将汇总数据行添加到 DataFrame
            summary_df = pd.DataFrame([summary_data])
            df = pd.concat([df, summary_df], ignore_index=True)
            output_filename = f'{self.brand}_{market}_{end_date}_listing_ditial_summary_data.csv'
            csv_path = os.path.join(get_export_path(), output_filename)
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            print("get_listing_ditial_data_summary successfully!")
            return csv_path
        except Exception as error:
            print("get_listing_ditial_data_summary Error while query data:", error)

    def get_listing_ditial_data_lapasa(self, market, start_date, end_date):
        # 低于 平均ACOS值 30% 以上的  campaign 广告活动
        # 建议执行的操作：预算提升30%
        try:
            conn = self.conn
            # 暂时忽略了market转化 US
            asin = f'{market.lower()}asin'
            query = f"""
 WITH a AS  (
        SELECT
                sales_channel AS 国家,
                apie.nsspu,
                DATE(
                CONVERT_TZ( purchase_date, '+08:00', '{self.load_config_info()['timezone_offset']}' )) AS event_date,
                ROUND( SUM( item_price ), 2 ) AS 总销售额
        FROM
                amazon_get_flat_file_all_orders_data_by_last_update_general agffa
                LEFT JOIN prod_as_product_base apie ON agffa.asin = apie.{asin}
        WHERE
                CONVERT_TZ( purchase_date, '+08:00', '{self.load_config_info()['timezone_offset']}' ) >= '{start_date}'
                AND CONVERT_TZ( purchase_date, '+08:00', '{self.load_config_info()['timezone_offset']}' ) < '{end_date}' + INTERVAL 1 DAY
                AND sales_channel = '{self.load_config_info()['sales_channel']}'
        GROUP BY
                sales_channel,
                apie.nsspu
    ),
b AS (
        -- 计算广告数据
        SELECT
            sp.market AS 国家,
            sp.nsspu,
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
										apie.nsspu,
                    adpr.market,
                    adpr.DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales7d) AS sum_sales
                FROM
                    amazon_advertised_product_reports_sp adpr
                LEFT JOIN
										prod_as_product_base apie ON adpr.advertisedAsin = apie.{asin}
                WHERE
                    adpr.DATE >= '{start_date}'
                    AND adpr.DATE <= '{end_date}'
                GROUP BY
                    adpr.market,
                    apie.nsspu
            ) AS sp
        LEFT JOIN (
                SELECT
										apie.nsspu,
                    adpr.market,
                    adpr.DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales) AS sum_sales
                FROM
                    amazon_advertised_product_reports_sd adpr
								LEFT JOIN
										prod_as_product_base apie ON adpr.promotedAsin = apie.{asin}
                WHERE
                    adpr.DATE >= '{start_date}'
                    AND adpr.DATE <= '{end_date}'
                GROUP BY
                    adpr.market,
										apie.nsspu
            ) AS sd ON sd.nsspu = sp.nsspu AND sd.market = sp.market
        WHERE
            sp.market = '{market}'
        ORDER BY
            sp.DATE
    ),
c AS (
        -- 计算广告数据
        SELECT
            sp.market AS 国家,
            sp.nsspu,
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
                    apie.nsspu,
                    adpr.market,
                    adpr.DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales7d) AS sum_sales
                FROM
                    amazon_advertised_product_reports_sp adpr
								LEFT JOIN
										prod_as_product_base apie ON adpr.advertisedAsin = apie.{asin}
                WHERE
                    adpr.DATE >= '{start_date}'
                    AND adpr.DATE <= '{end_date}'
                GROUP BY
                    adpr.market,
                    apie.nsspu
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
            sp.nsspu,
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
										apie.nsspu,
                    adpr.market,
                    adpr.DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales) AS sum_sales
                FROM
                    amazon_advertised_product_reports_sd adpr
								LEFT JOIN
										prod_as_product_base apie ON adpr.promotedAsin = apie.{asin}
                WHERE
                    adpr.DATE >= '{start_date}'
                    AND adpr.DATE <= '{end_date}'
                GROUP BY
                    adpr.market,
                    apie.nsspu
            ) AS sp
        WHERE
            sp.market = '{market}'
        ORDER BY
            sp.DATE
)

SELECT
        a.nsspu as list,
        COALESCE(a.总销售额, 0) AS 总销售额,
        COALESCE(b.广告总销售额, 0) AS 广告销售额,
        COALESCE(b.广告总花费, 0) AS 广告花费,
        CONCAT(ROUND(b.广告总ACOS * 100, 2), '%') AS ACOS,
        COALESCE(c.广告总销售额, 0) AS SP广告销售额,
        COALESCE(c.广告总花费, 0) AS SP广告花费,
        CONCAT(ROUND(c.广告总ACOS * 100, 2), '%') AS SP_ACOS,
        CONCAT(ROUND(((COALESCE(c.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 2),'%') AS SP广告销售额占比,
        COALESCE(d.广告总销售额, 0) AS SD广告销售额,
        COALESCE(d.广告总花费, 0) AS SD广告花费,
        CONCAT(ROUND(d.广告总ACOS * 100, 2), '%') AS SD_ACOS,
        CONCAT(ROUND(((COALESCE(d.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 2),'%') AS SD广告销售额占比
FROM a
LEFT JOIN b ON a.nsspu = b.nsspu
LEFT JOIN c ON a.nsspu = c.nsspu
LEFT JOIN d ON a.nsspu = d.nsspu
ORDER BY
COALESCE(b.广告总销售额, 0) DESC
             """
            df = pd.read_sql(query, con=conn)
            # 计算汇总数据
            total_ad_sales = df['广告销售额'].sum()
            total_ad_cost = df['广告花费'].sum()
            total_sp_sales = df['SP广告销售额'].sum()
            total_sp_cost = df['SP广告花费'].sum()
            total_sd_sales = df['SD广告销售额'].sum()
            total_sd_cost = df['SD广告花费'].sum()

            # 计算汇总行的各项指标
            if total_sp_sales > 0:
                sp_acos = (total_sp_cost / total_sp_sales) * 100
                overall_acos = (total_ad_cost / total_ad_sales) * 100 if total_ad_sales > 0 else 0
                sp_sales_ratio = (total_sp_sales / total_ad_sales) * 100 if total_ad_sales > 0 else 0

            else:
                sp_acos = 0
                overall_acos = 0
                sp_sales_ratio = 0
            sd_acos = (total_sd_cost / total_sd_sales) * 100 if total_sd_sales > 0 else 0
            sd_sales_ratio = (total_sd_sales / total_ad_sales) * 100 if total_ad_sales > 0 else 0

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
                'SD广告销售额': round(total_sd_sales, 2),
                'SD广告花费': total_sd_cost,
                'SD_ACOS': f'{sd_acos:.2f}%',
                'SD广告销售额占比': f'{sd_sales_ratio:.2f}%',
            }

            # 将汇总数据行添加到 DataFrame
            summary_df = pd.DataFrame([summary_data])
            df = pd.concat([df, summary_df], ignore_index=True)
            output_filename = f'{self.brand}_{market}_{end_date}_listing_ditial_data_lapasa.csv'
            csv_path = os.path.join(get_export_path(), output_filename)
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            print("get_listing_ditial_data_lapasa successfully!")
            return csv_path
        except Exception as error:
            print("get_listing_ditial_data_lapasa Error while query data:", error)

    def get_listing_ditial_data_lapasa_summary(self, market, start_date, end_date):

        try:
            conn = self.conn
            # 暂时忽略了market转化 US
            asin = f'deasin'
            query = f"""
 WITH a AS  (
        SELECT
                sales_channel AS 国家,
                apie.nsspu,
                DATE(
                CONVERT_TZ( purchase_date, '+08:00', '{self.load_summary_info()['timezone_offset']}' )) AS event_date,
                ROUND( SUM( item_price ), 2 ) AS 总销售额
        FROM
                amazon_get_flat_file_all_orders_data_by_last_update_general agffa
                LEFT JOIN prod_as_product_base apie ON agffa.asin = apie.{asin}
        WHERE
                CONVERT_TZ( purchase_date, '+08:00', '{self.load_summary_info()['timezone_offset']}' ) >= '{start_date}'
                AND CONVERT_TZ( purchase_date, '+08:00', '{self.load_summary_info()['timezone_offset']}' ) < '{end_date}' + INTERVAL 1 DAY
                AND sales_channel IN %(column1_values1)s
        GROUP BY
                apie.nsspu
    ),
b AS (
        -- 计算广告数据
        SELECT
            sp.market AS 国家,
            sp.nsspu,
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
                    apie.nsspu,
                    '{market}' AS market,
                    adpr.DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales7d) AS sum_sales
                FROM
                    amazon_advertised_product_reports_sp adpr
                LEFT JOIN
                                        prod_as_product_base apie ON adpr.advertisedAsin = apie.{asin}
                WHERE
                    adpr.DATE >= '{start_date}'
                    AND adpr.DATE <= '{end_date}'
                    AND adpr.market IN %(column2_values2)s
                GROUP BY
                    apie.nsspu
            ) AS sp
        LEFT JOIN (
                SELECT
                                        apie.nsspu,
                    '{market}' AS market,
                    adpr.DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales) AS sum_sales
                FROM
                    amazon_advertised_product_reports_sd adpr
                                LEFT JOIN
                                        prod_as_product_base apie ON adpr.promotedAsin = apie.{asin}
                WHERE
                    adpr.DATE >= '{start_date}'
                    AND adpr.DATE <= '{end_date}'
                    AND adpr.market IN %(column2_values2)s
                GROUP BY
                                        apie.nsspu
            ) AS sd ON sd.nsspu = sp.nsspu AND sd.market = sp.market
        ORDER BY
            sp.DATE
    ),
c AS (
        -- 计算广告数据
        SELECT
            sp.market AS 国家,
            sp.nsspu,
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
                    apie.nsspu,
                    '{market}' AS market,
                    adpr.DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales7d) AS sum_sales
                FROM
                    amazon_advertised_product_reports_sp adpr
                                LEFT JOIN
                                        prod_as_product_base apie ON adpr.advertisedAsin = apie.{asin}
                WHERE
                    adpr.DATE >= '{start_date}'
                    AND adpr.DATE <= '{end_date}'
                    AND adpr.market IN %(column2_values2)s
                GROUP BY
                    apie.nsspu
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
            sp.nsspu,
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
                                        apie.nsspu,
                    '{market}' AS market,
                    adpr.DATE,
                    SUM(cost) AS sum_cost,
                    SUM(sales) AS sum_sales
                FROM
                    amazon_advertised_product_reports_sd adpr
                                LEFT JOIN
                                        prod_as_product_base apie ON adpr.promotedAsin = apie.{asin}
                WHERE
                    adpr.DATE >= '{start_date}'
                    AND adpr.DATE <= '{end_date}'
                    AND adpr.market IN %(column2_values2)s
                GROUP BY
                    apie.nsspu
            ) AS sp
        ORDER BY
            sp.DATE
)

SELECT
        a.nsspu as list,
        COALESCE(a.总销售额, 0) AS 总销售额,
        COALESCE(b.广告总销售额, 0) AS 广告销售额,
        COALESCE(b.广告总花费, 0) AS 广告花费,
        CONCAT(ROUND(b.广告总ACOS * 100, 2), '%%') AS ACOS,
        COALESCE(c.广告总销售额, 0) AS SP广告销售额,
        COALESCE(c.广告总花费, 0) AS SP广告花费,
        CONCAT(ROUND(c.广告总ACOS * 100, 2), '%%') AS SP_ACOS,
        CONCAT(ROUND(((COALESCE(c.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 2),'%%') AS SP广告销售额占比,
        COALESCE(d.广告总销售额, 0) AS SD广告销售额,
        COALESCE(d.广告总花费, 0) AS SD广告花费,
        CONCAT(ROUND(d.广告总ACOS * 100, 2), '%%') AS SD_ACOS,
        CONCAT(ROUND(((COALESCE(d.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 2),'%%') AS SD广告销售额占比
FROM a
LEFT JOIN b ON a.nsspu = b.nsspu
LEFT JOIN c ON a.nsspu = c.nsspu
LEFT JOIN d ON a.nsspu = d.nsspu
ORDER BY
COALESCE(b.广告总销售额, 0) DESC
             """
            df = pd.read_sql(query, con=conn, params={'column1_values1': self.load_summary_info()['sales_channel'], 'column2_values2': self.load_summary_info()['country']})
            # 计算汇总数据
            total_ad_sales = df['广告销售额'].sum()
            total_ad_cost = df['广告花费'].sum()
            total_sp_sales = df['SP广告销售额'].sum()
            total_sp_cost = df['SP广告花费'].sum()
            total_sd_sales = df['SD广告销售额'].sum()
            total_sd_cost = df['SD广告花费'].sum()

            # 计算汇总行的各项指标
            if total_sp_sales > 0:
                sp_acos = (total_sp_cost / total_sp_sales) * 100
                overall_acos = (total_ad_cost / total_ad_sales) * 100 if total_ad_sales > 0 else 0
                sp_sales_ratio = (total_sp_sales / total_ad_sales) * 100 if total_ad_sales > 0 else 0

            else:
                sp_acos = 0
                overall_acos = 0
                sp_sales_ratio = 0
            sd_acos = (total_sd_cost / total_sd_sales) * 100 if total_sd_sales > 0 else 0
            sd_sales_ratio = (total_sd_sales / total_ad_sales) * 100 if total_ad_sales > 0 else 0

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
                'SD广告销售额': round(total_sd_sales, 2),
                'SD广告花费': total_sd_cost,
                'SD_ACOS': f'{sd_acos:.2f}%',
                'SD广告销售额占比': f'{sd_sales_ratio:.2f}%',
            }

            # 将汇总数据行添加到 DataFrame
            summary_df = pd.DataFrame([summary_data])
            df = pd.concat([df, summary_df], ignore_index=True)
            output_filename = f'{self.brand}_{market}_{end_date}_listing_ditial_data_lapasa.csv'
            csv_path = os.path.join(get_export_path(), output_filename)
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            print("get_listing_ditial_data_lapasa successfully!")
            return csv_path
        except Exception as error:
            print("get_listing_ditial_data_lapasa Error while query data:", error)

    def get_managed_listing_ditial_data(self, market, start_date, end_date, asin_info):
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
                        CONCAT( apie.asin, '(asin)' ) ELSE apie.parent_asins
                END AS parent_asins_or_asin,
                DATE(
                CONVERT_TZ( purchase_date, '+08:00', '{self.load_config_info()['timezone_offset']}' )) AS event_date,
                ROUND( SUM( item_price ), 2 ) AS 总销售额
        FROM
                amazon_get_flat_file_all_orders_data_by_last_update_general agffa
                LEFT JOIN amazon_product_info_extended apie ON agffa.asin = apie.asin
        WHERE
                CONVERT_TZ( purchase_date, '+08:00', '{self.load_config_info()['timezone_offset']}' ) >= '{start_date}'
                AND CONVERT_TZ( purchase_date, '+08:00', '{self.load_config_info()['timezone_offset']}' ) < '{end_date}' + INTERVAL 1 DAY
                AND sales_channel = '{self.load_config_info()['sales_channel']}'
                AND apie.market = '{market}'
                AND agffa.asin IN %(column1_values1)s
        GROUP BY
                sales_channel,
        CASE

                        WHEN apie.parent_asins = '' THEN
                        CONCAT( apie.asin, '(asin)' ) ELSE apie.parent_asins
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
                    adpr.DATE >= '{start_date}'
                    AND adpr.DATE <= '{end_date}'
                    AND adpr.advertisedAsin IN %(column1_values1)s
                GROUP BY
                    adpr.market,
                                                                                CASE WHEN apie.parent_asins = '' THEN CONCAT(apie.asin,'(asin)') ELSE apie.parent_asins END
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
                    adpr.DATE >= '{start_date}'
                    AND adpr.DATE <= '{end_date}'
                    AND adpr.promotedAsin IN %(column1_values1)s
                GROUP BY
                    adpr.market,
                                                                                CASE WHEN apie.parent_asins = '' THEN CONCAT(apie.asin,'(asin)') ELSE apie.parent_asins END
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
                    adpr.DATE >= '{start_date}'
                    AND adpr.DATE <= '{end_date}'
                    AND adpr.advertisedAsin IN %(column1_values1)s
                GROUP BY
                    adpr.market,
                    CASE WHEN apie.parent_asins = '' THEN CONCAT(apie.asin,'(asin)') ELSE apie.parent_asins END
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
                    adpr.DATE >= '{start_date}'
                    AND adpr.DATE <= '{end_date}'
                    AND adpr.promotedAsin IN %(column1_values1)s
                GROUP BY
                    adpr.market,
                    CASE WHEN apie.parent_asins = '' THEN CONCAT(apie.asin,'(asin)') ELSE apie.parent_asins END
            ) AS sp
        WHERE
            sp.market = '{market}'
        ORDER BY
            sp.DATE
)

SELECT
        a.parent_asins_or_asin as list,
        COALESCE(a.总销售额, 0) AS 总销售额,
        COALESCE(a.总销售额, 0)-COALESCE(b.广告总销售额, 0) AS 自然销售额,
        COALESCE(b.广告总销售额, 0) AS 广告销售额,
        COALESCE(b.广告总花费, 0) AS 广告花费,
        CONCAT(ROUND(b.广告总ACOS * 100, 2), '%%') AS ACOS,
        COALESCE(c.广告总销售额, 0) AS SP广告销售额,
        COALESCE(c.广告总花费, 0) AS SP广告花费,
        CONCAT(ROUND(c.广告总ACOS * 100, 2), '%%') AS SP_ACOS,
        CONCAT(ROUND(((COALESCE(c.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 2),'%%') AS SP广告销售额占比,
        COALESCE(d.广告总销售额, 0) AS SD广告销售额,
        COALESCE(d.广告总花费, 0) AS SD广告花费,
        CONCAT(ROUND(d.广告总ACOS * 100, 2), '%%') AS SD_ACOS,
        CONCAT(ROUND(((COALESCE(d.广告总销售额, 0) / COALESCE(b.广告总销售额, 0)) * 100), 2),'%%') AS SD广告销售额占比
FROM a
LEFT JOIN b ON a.parent_asins_or_asin = b.parent_asins_or_asin
LEFT JOIN c ON a.parent_asins_or_asin = c.parent_asins_or_asin
LEFT JOIN d ON a.parent_asins_or_asin = d.parent_asins_or_asin
ORDER BY
COALESCE(b.广告总销售额, 0) DESC

             """
            df = pd.read_sql(query, con=conn, params={'column1_values1': asin_info})
            # 计算汇总数据
            total_ad_sales = df['广告销售额'].sum()
            total_ad_cost = df['广告花费'].sum()
            total_sp_sales = df['SP广告销售额'].sum()
            total_sp_cost = df['SP广告花费'].sum()
            total_sd_sales = df['SD广告销售额'].sum()
            total_sd_cost = df['SD广告花费'].sum()

            # 计算汇总行的各项指标
            if total_sp_sales > 0:
                sp_acos = (total_sp_cost / total_sp_sales) * 100
                overall_acos = (total_ad_cost / total_ad_sales) * 100 if total_ad_sales > 0 else 0
                sp_sales_ratio = (total_sp_sales / total_ad_sales) * 100 if total_ad_sales > 0 else 0

            else:
                sp_acos = 0
                overall_acos = 0
                sp_sales_ratio = 0
            sd_acos = (total_sd_cost / total_sd_sales) * 100 if total_sd_sales > 0 else 0
            sd_sales_ratio = (total_sd_sales / total_ad_sales) * 100 if total_ad_sales > 0 else 0

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
                'SD广告销售额': round(total_sd_sales, 2),
                'SD广告花费': total_sd_cost,
                'SD_ACOS': f'{sd_acos:.2f}%',
                'SD广告销售额占比': f'{sd_sales_ratio:.2f}%',
            }

            # 将汇总数据行添加到 DataFrame
            summary_df = pd.DataFrame([summary_data])
            df = pd.concat([df, summary_df], ignore_index=True)
            output_filename = f'{self.brand}_{market}_{end_date}_managed_listing_ditial_data.csv'
            csv_path = os.path.join(get_export_path(), output_filename)
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            print("get_managed_listing_ditial_data successfully!")
            return csv_path
        except Exception as error:
            print("get_managed_listing_ditial_data Error while query data:", error)


    def get_inventory_data_lapasa(self, market,start_date,end_date):
        # 低于 平均ACOS值 30% 以上的  campaign 广告活动
        # 建议执行的操作：预算提升30%
        try:
            conn = self.conn
            region_info = get_region_info(market)
            # 暂时忽略了market转化 US
            query = f"""
SELECT
        '{start_date}' AS '订单开始时间',-- 显示订单开始时间
        '{end_date}' AS '订单结束时间',-- 显示订单结束时间
        nsspu,-- 产品线编码
        SUM( available ) AS '可售',
  SUM( inbound_working ) AS '待上架',
        sum( available ) + sum( inbound_working ) AS '可售+ 待上架',-- 计算可售库存和待上架库存的总和
        SUM( inv_age_0_to_90_days ) + SUM( inv_age_91_to_180_days ) AS '近半年进货量',
        SUM( all_order.sum_order ) AS 总销量,-- 计算周期内总销量
        snapshot_date AS '库存更新时间',-- 显示库存更新时间
        ROUND((
                        SUM( available ) -- + SUM( inbound_working )
                        ) * (DATEDIFF( '{end_date}', '{start_date}' ) + 1 )/ SUM( all_order.sum_order ),
                2
        ) AS '预估可售天数' -- 计算预估可售天数，并保留两位小数

FROM
        (-- 子查询 b，用于获取每个产品的最新库存数据
        SELECT
                sku,-- 产品 SKU
                nsspu,-- 产品线编码
                available,-- 可售库存
                inv_age_0_to_90_days,
                inv_age_91_to_180_days,
                snapshot_date,-- 库存更新时间
                inbound_working -- 待上架库存

        FROM
                (-- 子查询 a，用于从库存表中筛选数据并使用 ROW_NUMBER() 函数标记每个产品的最新库存记录
                SELECT
                        fba.sku,-- 产品 SKU
                        available,-- 可售库存
                        inv_age_0_to_90_days,
                        inv_age_91_to_180_days,
                        prod_as_product_base.nsspu,-- 产品线编码
                        prod_as_product_base.sspu,-- 产品子分类编码
                        snapshot_date,-- 库存更新时间
                        market,-- 市场
                        inbound_working,-- 待上架库存
                        ROW_NUMBER() OVER (
                                PARTITION BY sku
                        ORDER BY
                                ABS(
                                        TIMESTAMPDIFF(
                                                SECOND,
                                                snapshot_date,
                                        NOW()))) AS rn -- 使用 ROW_NUMBER() 函数为每个产品的最新库存记录标记为 1

                FROM
                        amazon_get_fba_inventory_planning_data AS fba -- 库存数据表
                        LEFT JOIN prod_as_product_base ON prod_as_product_base.{region_info['sku']} = fba.sku -- 左连接产品基础信息表
                        -- UK:uksku
                        -- US:ussku
                WHERE
                        market IN %(column1_values1)s
                ) AS a
        WHERE
                a.rn = 1 -- 只保留每个产品的最新库存记录

        ORDER BY
                sku -- 按产品 SKU 排序

        ) AS b
        LEFT JOIN (-- 子查询 all_order，用于计算每个产品的周期内销量
        SELECT
                sku,-- 产品 SKU
                SUM( quantity ) AS sum_order -- 计算周期内总销量

        FROM
                amazon_get_flat_file_all_orders_data_by_last_update_general -- 订单数据表

        WHERE
                sales_channel IN %(column2_values2)s

        AND purchase_date BETWEEN '{start_date}' AND '{end_date}' -- 筛选指定日期范围内的订单数据

        GROUP BY
                sku -- 按产品 SKU 分组

        ) AS all_order ON b.sku = all_order.sku -- 将库存数据和订单数据关联起来

GROUP BY
        nsspu -- 按产品线分组

ORDER BY
        可售 DESC;-- 按可售数量降序排列
             """
            df = pd.read_sql(query, con=conn, params={'column1_values1': region_info['market'], 'column2_values2': region_info['sales_channel']})
            # 计算汇总数据
            para1 = df['可售'].sum()
            para2 = df['待上架'].sum()
            para3 = df['可售+ 待上架'].sum()
            para4 = df['近半年进货量'].sum()
            para5 = df['总销量'].sum()

            # 创建汇总数据行
            summary_data = {
                '订单开始时间': ' ',
                '订单结束时间': ' ',
                'nsspu': '总计',
                '可售': para1,
                '待上架': para2,
                '可售+ 待上架': para3,
                '近半年进货量': para4,
                '总销量': para5,
                '库存更新时间': ' ',
                '预估可售天数': ' '
            }

            # 将汇总数据行添加到 DataFrame
            summary_df = pd.DataFrame([summary_data])
            df = pd.concat([df, summary_df], ignore_index=True)
            output_filename = f'{self.brand}_{market}_{end_date}_inventory_data_lapasa.csv'
            csv_path = os.path.join(get_export_path(), output_filename)
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            print("get_inventory_data_lapasa successfully!")
            return csv_path
        except Exception as error:
            print("get_inventory_data_lapasa Error while query data:", error)

    def get_inventory_data(self, market,start_date,end_date):
        # 低于 平均ACOS值 30% 以上的  campaign 广告活动
        # 建议执行的操作：预算提升30%
        try:
            conn = self.conn
            region_info = get_region_info(market)
            # 暂时忽略了market转化 US
            query = f"""
SELECT
        '{start_date}' AS '订单开始时间',-- 显示订单开始时间
        '{end_date}' AS '订单结束时间',-- 显示订单结束时间
        parent_asins,-- 产品线编码
        SUM( available ) AS '可售',
  SUM( inbound_working ) AS '待上架',
        sum( available ) + sum( inbound_working ) AS '可售+ 待上架',-- 计算可售库存和待上架库存的总和
        SUM( inv_age_0_to_90_days ) + SUM( inv_age_91_to_180_days ) AS '近半年进货量',
        SUM( all_order.sum_order ) AS 总销量,-- 计算周期内总销量
        snapshot_date AS '库存更新时间',-- 显示库存更新时间
        ROUND((
                        SUM( available ) -- + SUM( inbound_working )
                        ) * (DATEDIFF( '{end_date}', '{start_date}' ) + 1 )/ SUM( all_order.sum_order ),
                2
        ) AS '预估可售天数' -- 计算预估可售天数，并保留两位小数

FROM
        (-- 子查询 b，用于获取每个产品的最新库存数据
        SELECT
                sku,-- 产品 SKU
                parent_asins,-- 产品线编码
                available,-- 可售库存
                inv_age_0_to_90_days,
                inv_age_91_to_180_days,
                snapshot_date,-- 库存更新时间
                inbound_working -- 待上架库存

        FROM
                (-- 子查询 a，用于从库存表中筛选数据并使用 ROW_NUMBER() 函数标记每个产品的最新库存记录
                SELECT
                        fba.sku,-- 产品 SKU
                        available,-- 可售库存
                        inv_age_0_to_90_days,
                        inv_age_91_to_180_days,
                        COALESCE(NULLIF(amazon_product_info_extended.parent_asins,''), amazon_product_info_extended.asin ) AS parent_asins,
                        snapshot_date,-- 库存更新时间
                        fba.market,-- 市场
                        inbound_working,-- 待上架库存
                        ROW_NUMBER() OVER (
                                PARTITION BY sku
                        ORDER BY
                                ABS(
                                        TIMESTAMPDIFF(
                                                SECOND,
                                                snapshot_date,
                                        NOW()))) AS rn -- 使用 ROW_NUMBER() 函数为每个产品的最新库存记录标记为 1

                FROM
                        amazon_get_fba_inventory_planning_data AS fba -- 库存数据表
                        LEFT JOIN amazon_product_info_extended ON amazon_product_info_extended.asin = fba.asin
                        -- UK:uksku
                        -- US:ussku
                WHERE
                        fba.market IN %(column1_values1)s
                        AND  amazon_product_info_extended.market = '{region_info['country']}'
                ) AS a
        WHERE
                a.rn = 1 -- 只保留每个产品的最新库存记录

        ORDER BY
                sku -- 按产品 SKU 排序

        ) AS b
        LEFT JOIN (-- 子查询 all_order，用于计算每个产品的周期内销量
        SELECT
                sku,-- 产品 SKU
                SUM( quantity ) AS sum_order -- 计算周期内总销量

        FROM
                amazon_get_flat_file_all_orders_data_by_last_update_general -- 订单数据表

        WHERE
                sales_channel IN %(column2_values2)s

        AND purchase_date BETWEEN '{start_date}' AND '{end_date}' -- 筛选指定日期范围内的订单数据

        GROUP BY
                sku -- 按产品 SKU 分组

        ) AS all_order ON b.sku = all_order.sku -- 将库存数据和订单数据关联起来

GROUP BY
        parent_asins -- 按产品线分组

ORDER BY
        可售 DESC;-- 按可售数量降序排列
             """
            df = pd.read_sql(query, con=conn, params={'column1_values1': region_info['market'], 'column2_values2': region_info['sales_channel']})
            # 计算汇总数据
            para1 = df['可售'].sum()
            para2 = df['待上架'].sum()
            para3 = df['可售+ 待上架'].sum()
            para4 = df['近半年进货量'].sum()
            para5 = df['总销量'].sum()

            # 创建汇总数据行
            summary_data = {
                '订单开始时间': ' ',
                '订单结束时间': ' ',
                'parent_asins': '总计',
                '可售': para1,
                '待上架': para2,
                '可售+ 待上架': para3,
                '近半年进货量': para4,
                '总销量': para5,
                '库存更新时间': ' ',
                '预估可售天数': ' '
            }

            # 将汇总数据行添加到 DataFrame
            summary_df = pd.DataFrame([summary_data])
            df = pd.concat([df, summary_df], ignore_index=True)
            output_filename = f'{self.brand}_{market}_{end_date}_inventory_data.csv'
            csv_path = os.path.join(get_export_path(), output_filename)
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            print("get_inventory_data successfully!")
            return csv_path
        except Exception as error:
            print("get_inventory_data Error while query data:", error)


    def get_inventory_data_custody(self, market,start_date,end_date,asin_info):
        # 低于 平均ACOS值 30% 以上的  campaign 广告活动
        # 建议执行的操作：预算提升30%
        try:
            conn = self.conn
            region_info = get_region_info(market)
            # 暂时忽略了market转化 US
            query = f"""
SELECT
        '{start_date}' AS '订单开始时间',-- 显示订单开始时间
        '{end_date}' AS '订单结束时间',-- 显示订单结束时间
        parent_asins,-- 产品线编码
        SUM( available ) AS '可售',
  SUM( inbound_working ) AS '待上架',
        sum( available ) + sum( inbound_working ) AS '可售+ 待上架',-- 计算可售库存和待上架库存的总和
        SUM( inv_age_0_to_90_days ) + SUM( inv_age_91_to_180_days ) AS '近半年进货量',
        SUM( all_order.sum_order ) AS 总销量,-- 计算周期内总销量
        snapshot_date AS '库存更新时间',-- 显示库存更新时间
        ROUND((
                        SUM( available ) -- + SUM( inbound_working )
                        ) * (DATEDIFF( '{end_date}', '{start_date}' ) + 1 )/ SUM( all_order.sum_order ),
                2
        ) AS '预估可售天数' -- 计算预估可售天数，并保留两位小数

FROM
        (-- 子查询 b，用于获取每个产品的最新库存数据
        SELECT
                sku,-- 产品 SKU
                parent_asins,-- 产品线编码
                available,-- 可售库存
                inv_age_0_to_90_days,
                inv_age_91_to_180_days,
                snapshot_date,-- 库存更新时间
                inbound_working -- 待上架库存

        FROM
                (-- 子查询 a，用于从库存表中筛选数据并使用 ROW_NUMBER() 函数标记每个产品的最新库存记录
                SELECT
                        fba.sku,-- 产品 SKU
                        available,-- 可售库存
                        inv_age_0_to_90_days,
                        inv_age_91_to_180_days,
                        amazon_product_info_extended.parent_asins,
                        snapshot_date,-- 库存更新时间
                        fba.market,-- 市场
                        inbound_working,-- 待上架库存
                        ROW_NUMBER() OVER (
                                PARTITION BY sku
                        ORDER BY
                                ABS(
                                        TIMESTAMPDIFF(
                                                SECOND,
                                                snapshot_date,
                                        NOW()))) AS rn -- 使用 ROW_NUMBER() 函数为每个产品的最新库存记录标记为 1

                FROM
                        amazon_get_fba_inventory_planning_data AS fba -- 库存数据表
                        LEFT JOIN amazon_product_info_extended ON amazon_product_info_extended.asin = fba.asin
                        -- UK:uksku
                        -- US:ussku
                WHERE
                        fba.market IN %(column1_values1)s
                        AND  amazon_product_info_extended.market = '{region_info['country']}'
                        AND fba.asin IN %(column3_values3)s
                ) AS a
        WHERE
                a.rn = 1 -- 只保留每个产品的最新库存记录

        ORDER BY
                sku -- 按产品 SKU 排序

        ) AS b
        LEFT JOIN (-- 子查询 all_order，用于计算每个产品的周期内销量
        SELECT
                sku,-- 产品 SKU
                SUM( quantity ) AS sum_order -- 计算周期内总销量

        FROM
                amazon_get_flat_file_all_orders_data_by_last_update_general -- 订单数据表

        WHERE
                sales_channel IN %(column2_values2)s

        AND purchase_date BETWEEN '{start_date}' AND '{end_date}' -- 筛选指定日期范围内的订单数据
        AND asin IN %(column3_values3)s
        GROUP BY
                sku -- 按产品 SKU 分组

        ) AS all_order ON b.sku = all_order.sku -- 将库存数据和订单数据关联起来

GROUP BY
        parent_asins -- 按产品线分组

ORDER BY
        可售 DESC;-- 按可售数量降序排列
             """
            df = pd.read_sql(query, con=conn, params={'column1_values1': region_info['market'], 'column2_values2': region_info['sales_channel'], 'column3_values3': asin_info})
            # 计算汇总数据
            para1 = df['可售'].sum()
            para2 = df['待上架'].sum()
            para3 = df['可售+ 待上架'].sum()
            para4 = df['近半年进货量'].sum()
            para5 = df['总销量'].sum()

            # 创建汇总数据行
            summary_data = {
                '订单开始时间': ' ',
                '订单结束时间': ' ',
                'parent_asins': '总计',
                '可售': para1,
                '待上架': para2,
                '可售+ 待上架': para3,
                '近半年进货量': para4,
                '总销量': para5,
                '库存更新时间': ' ',
                '预估可售天数': ' '
            }

            # 将汇总数据行添加到 DataFrame
            summary_df = pd.DataFrame([summary_data])
            df = pd.concat([df, summary_df], ignore_index=True)
            output_filename = f'{self.brand}_{market}_{end_date}_inventory_data_custody.csv'
            csv_path = os.path.join(get_export_path(), output_filename)
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            print("get_inventory_data_custody successfully!")
            return csv_path
        except Exception as error:
            print("get_inventory_data_custody Error while query data:", error)
