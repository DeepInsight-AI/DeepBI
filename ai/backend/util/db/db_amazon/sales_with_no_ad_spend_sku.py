import json
import os
import pymysql
import pandas as pd
from datetime import datetime
import warnings
from ai.backend.util.db.configuration.path import get_config_path

# 忽略特定类型的警告
warnings.filterwarnings("ignore", category=UserWarning)


class SalesWithNoAdSpendSku:

    def __init__(self, brand):
        self.brand = brand
        self.db_info = self.load_db_info(brand)
        self.conn = self.connect(self.db_info)

    def load_db_info(self, brand):
        # 从 JSON 文件加载数据库信息
        db_info_path = os.path.join(get_config_path(), 'db_info.json')
        with open(db_info_path, 'r') as f:
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

    def get_sales_with_no_ad_spend_sku(self, market, date):


        try:
            conn = self.conn
            channel = f'Amazon.{market.lower()}'
            query = f"""
WITH sp_data_cost7 AS (
        SELECT
                advertisedSku,
                SUM( sales1d ) AS total_sales_sp,
                SUM( cost ) AS total_cost_sp,
                market
        FROM
                amazon_advertised_product_reports_sp
        WHERE
                DATE BETWEEN DATE_SUB( CAST('{date}' as date), INTERVAL 7 DAY )
                AND '{date}'
                AND market = '{market}'
        GROUP BY
                advertisedSku,
                market
        ),

-- 子查询用于获取amazon_advertised_product_reports_sd表近七天的数据
        sd_data_cost7 AS (
        SELECT
                promotedSku AS advertisedSku,
                SUM( sales ) AS total_sales_sd,
                SUM( cost ) AS total_cost_sd,
                market
        FROM
                amazon_advertised_product_reports_sd
        WHERE
                DATE BETWEEN DATE_SUB( CAST('{date}' as date), INTERVAL 7 DAY )
                AND '{date}'
                AND market = '{market}'
        GROUP BY
                promotedSku,
                market
        ),

-- 使用LEFT JOIN和RIGHT JOIN来模拟FULL OUTER JOIN
market_data_cost7 AS (
SELECT COALESCE
        ( sp.advertisedSku, sd.advertisedSku ) AS advertisedSku,
        COALESCE ( sp.market, sd.market ) AS market
FROM
        sp_data_cost7 AS sp
        LEFT JOIN sd_data_cost7 AS sd ON sp.advertisedSku = sd.advertisedSku
        AND sp.market = sd.market
WHERE
        (
        COALESCE ( sp.total_cost_sp, 0 ) + COALESCE ( sd.total_cost_sd, 0 )) > 0 UNION
SELECT COALESCE
        ( sp.advertisedSku, sd.advertisedSku ) AS advertisedSku,
        COALESCE ( sp.market, sd.market ) AS market
FROM
        sp_data_cost7 AS sp
        RIGHT JOIN sd_data_cost7 AS sd ON sp.advertisedSku = sd.advertisedSku
        AND sp.market = sd.market
WHERE
        sp.advertisedSku IS NULL
        AND (
        COALESCE ( sp.total_cost_sp, 0 ) + COALESCE ( sd.total_cost_sd, 0 )) > 0
  ),
market_data_sales30 AS  (
SELECT
sku,
SUM(quantity) as sum_order,
UPPER(SUBSTR(sales_channel,8,2)) as market
FROM
amazon_get_flat_file_all_orders_data_by_last_update_general
WHERE
sales_channel = '{channel}'
and
purchase_date >= DATE_SUB( CAST('{date}' as date), INTERVAL 30 DAY )
GROUP BY sku,UPPER(SUBSTR(sales_channel,8,2))
HAVING SUM(quantity) > 0
ORDER BY SUM(quantity) DESC
)

SELECT
sku,
 sum_order,
    market
FROM
market_data_sales30 AS ms
WHERE not EXISTS
(SELECT * from market_data_cost7 AS mc
where mc.advertisedSku=ms.sku AND mc.market=ms.market)
                        """

            df = pd.read_sql(query, con=conn)
            output_filename = f'{self.brand}_{market}_{date}_sales_with_no_ad_spend_sku.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            # return df
            return output_filename

        except Exception as error:
            print("1-1.1Error while inserting data:", error)


SalesWithNoAdSpendSku('OutdoorMaster').get_sales_with_no_ad_spend_sku('IT','2024-08-1')
