import json
import os
import pymysql
import pandas as pd
from datetime import datetime
import warnings

import yaml

from ai.backend.util.db.configuration.path import get_config_path

# 忽略特定类型的警告
warnings.filterwarnings("ignore", category=UserWarning)


class Sd0808Strategy:

    def __init__(self, brand,market):
        self.brand = brand
        self.db_info = self.load_db_info(brand,market)
        self.conn = self.connect(self.db_info)
        self.depository = self.load_depository(brand,market)

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

    def load_depository(self, brand, country=None):
        # 从 JSON 文件加载数据库信息
        Brand_path = os.path.join(get_config_path(), 'Brand.yml')
        with open(Brand_path, 'r') as file:
            Brand_data = yaml.safe_load(file)

        brand_info = Brand_data.get(brand, {})
        if country:
            country_info = brand_info.get(country, {})
            return country_info.get('depository', brand_info.get('default', {}).get('depository'))
        return brand_info.get('depository', brand_info.get('default', {}).get('depository'))


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

    def get_0808_sd_ad(self, market, asin_info, asin:int =0):

        try:
            conn = self.conn
            if asin == 0:
                query = f"""
WITH a AS(
SELECT
adGroupId,
campaignId,
UPPER(searchTerm) AS ASIN,
SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_30d,
SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_7d
FROM amazon_search_term_reports_sp
WHERE searchTerm LIKE 'b0%%' AND LENGTH(searchTerm) = 10
AND market = '{market}'
AND date BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 30 DAY) AND CURRENT_DATE- INTERVAL 1 DAY
GROUP BY
campaignId,
adGroupId,
ASIN
),
b AS (
SELECT
campaignId,
adGroupId,
ASIN,
ACOS_30d,
ACOS_7d
FROM
a
WHERE ASIN NOT IN (
SELECT DISTINCT asin FROM amazon_product_info
WHERE market = '{market}'
)
),
c AS (
SELECT
asp.asin,
b.ASIN AS targeting_asin,
b.ACOS_30d,
b.ACOS_7d
FROM
amazon_sp_productads_list asp
LEFT JOIN b ON asp.campaignId = b.campaignId AND asp.adGroupId = b.adGroupId
WHERE market = '{market}'
HAVING
targeting_asin IS NOT NULL
)
SELECT * FROM c
WHERE
c.asin IN %(column1_values1)s
GROUP BY
asin,
targeting_asin
HAVING
ACOS_30d IS NOT NULL
                            """
            elif asin == 1:
                query = f"""
WITH a AS(
SELECT
adGroupId,
campaignId,
UPPER(searchTerm) AS ASIN,
SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_30d,
SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_7d
FROM amazon_search_term_reports_sp
WHERE searchTerm LIKE 'b0%%' AND LENGTH(searchTerm) = 10
AND market = '{market}'
AND date BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 30 DAY) AND CURRENT_DATE- INTERVAL 1 DAY
GROUP BY
campaignId,
adGroupId,
ASIN
),
b AS (
SELECT
campaignId,
adGroupId,
ASIN,
ACOS_30d,
ACOS_7d
FROM
a
WHERE ASIN NOT IN (
SELECT DISTINCT asin FROM amazon_product_info
WHERE market = '{market}'
)
),
c AS (
SELECT
asp.asin,
b.ASIN AS targeting_asin,
b.ACOS_30d,
b.ACOS_7d
FROM
amazon_sp_productads_list asp
LEFT JOIN b ON asp.campaignId = b.campaignId AND asp.adGroupId = b.adGroupId
WHERE market = '{market}'
HAVING
targeting_asin IS NOT NULL
)
SELECT
        amazon_product_info_extended.parent_asins as asin,
        c.targeting_asin,
        c.ACOS_30d,
        c.ACOS_7d
FROM
        c
        LEFT JOIN amazon_product_info_extended ON c.asin = amazon_product_info_extended.asin
WHERE
        amazon_product_info_extended.market = '{self.depository}'
        AND amazon_product_info_extended.parent_asins IN %(column1_values1)s
GROUP BY
parent_asins,
targeting_asin
HAVING
        parent_asins IS NOT NULL AND parent_asins <> ''
        AND ACOS_30d IS NOT NULL
                                            """
            elif asin == 2:
                variable = f'{market.lower()}asin'
                query = f"""
WITH a AS(
SELECT
adGroupId,
campaignId,
UPPER(searchTerm) AS ASIN,
SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_30d,
SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN cost ELSE 0 END) / SUM(CASE WHEN date BETWEEN DATE_SUB(CURRENT_DATE - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) THEN sales14d ELSE 0 END) AS ACOS_7d
FROM amazon_search_term_reports_sp
WHERE searchTerm LIKE 'b0%%' AND LENGTH(searchTerm) = 10
AND market = '{market}'
AND date BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 30 DAY) AND CURRENT_DATE- INTERVAL 1 DAY
GROUP BY
campaignId,
adGroupId,
ASIN
),
b AS (
SELECT
campaignId,
adGroupId,
ASIN,
ACOS_30d,
ACOS_7d
FROM
a
WHERE ASIN NOT IN (
SELECT DISTINCT asin FROM amazon_product_info
WHERE market = '{market}'
)
),
c AS (
SELECT
asp.asin,
b.ASIN AS targeting_asin,
b.ACOS_30d,
b.ACOS_7d
FROM
amazon_sp_productads_list asp
LEFT JOIN b ON asp.campaignId = b.campaignId AND asp.adGroupId = b.adGroupId
WHERE market = '{market}'
HAVING
targeting_asin IS NOT NULL
)
SELECT
        prod_as_product_base.sspu as asin,
        c.targeting_asin,
        c.ACOS_30d,
        c.ACOS_7d
FROM
        c
        LEFT JOIN prod_as_product_base ON c.asin = prod_as_product_base.{variable}
WHERE
 prod_as_product_base.sspu IN %(column1_values1)s
GROUP BY
sspu,
targeting_asin
HAVING
        sspu IS NOT NULL AND sspu <> ''
        AND ACOS_30d IS NOT NULL
                                                        """
            df = pd.read_sql(query, con=conn, params={'column1_values1': asin_info})
            output_filename = f'{self.brand}_{market}_0808.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            # return df
            return output_filename

        except Exception as error:
            print("1-1.1Error while inserting data:", error)


#Sd0808Strategy('LAPASA','IT').get_0808_sd_ad('IT',['M93'],2)
