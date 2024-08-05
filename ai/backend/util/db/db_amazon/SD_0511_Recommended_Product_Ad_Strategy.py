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


class SdRecommendedProductAdStrategy:

    def __init__(self, brand):
        self.brand = brand
        self.db_info = self.load_db_info(brand)
        self.conn = self.connect(self.db_info)
        self.depository = self.load_depository(brand)

    def load_db_info(self, brand):
        # 从 JSON 文件加载数据库信息
        db_info_path = os.path.join(get_config_path(), 'db_info.json')
        with open(db_info_path, 'r') as f:
            db_info_json = json.load(f)

        if brand in db_info_json:
            return db_info_json[brand]
        else:
            raise ValueError(f"Unknown brand '{brand}'")

    def load_depository(self, brand):
        # 从 JSON 文件加载数据库信息
        db_info_path = os.path.join(get_config_path(), 'Brand.yml')
        with open(db_info_path, 'r') as f:
            db_info_json = yaml.safe_load(f)
        return db_info_json.get(brand).get('depository')


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

    def get_0511_sd_recommended_product_ad(self, market, startdate, enddate):

        try:
            conn = self.conn
            query = f"""
WITH sp_data AS (
        SELECT
                advertisedSku,
                SUM( sales1d ) AS total_sales_sp,
                SUM( cost ) AS total_cost_sp,
                market
        FROM
                amazon_advertised_product_reports_sp
        WHERE
                DATE BETWEEN '{startdate}'
                AND '{enddate}'
                AND market = '{market}'
        GROUP BY
                advertisedSku,
                market
        ),

-- 子查询用于获取amazon_advertised_product_reports_sd表的数据
        sd_data AS (
        SELECT
                promotedSku AS advertisedSku,
                SUM( sales ) AS total_sales_sd,
                SUM( cost ) AS total_cost_sd,
                market
        FROM
                amazon_advertised_product_reports_sd
        WHERE
                DATE BETWEEN '{startdate}'
                AND '{enddate}'
                AND market = '{market}'
        GROUP BY
                promotedSku,
                market
        ),

-- 使用LEFT JOIN和RIGHT JOIN来模拟FULL OUTER JOIN
market_data as (SELECT
    COALESCE(sp.advertisedSku, sd.advertisedSku) AS sku,
    COALESCE(sp.market, sd.market) AS market,
    (COALESCE(sp.total_sales_sp, 0) + COALESCE(sd.total_sales_sd, 0)) AS total_sales,
    (COALESCE(sp.total_cost_sp, 0) + COALESCE(sd.total_cost_sd, 0)) AS total_cost,
    (COALESCE(sp.total_cost_sp, 0) + COALESCE(sd.total_cost_sd, 0)) /
    (COALESCE(sp.total_sales_sp, 0) + COALESCE(sd.total_sales_sd, 0)) AS avg_acos
FROM
    sp_data AS sp
LEFT JOIN
    sd_data AS sd
ON
    sp.advertisedSku = sd.advertisedSku AND sp.market = sd.market
WHERE
(COALESCE(sp.total_sales_sp, 0) + COALESCE(sd.total_sales_sd, 0))>0 AND     (COALESCE(sp.total_cost_sp, 0) + COALESCE(sd.total_cost_sd, 0)) /
    (COALESCE(sp.total_sales_sp, 0) + COALESCE(sd.total_sales_sd, 0)) < 0.2
UNION

SELECT
    COALESCE(sp.advertisedSku, sd.advertisedSku) AS sku,
    COALESCE(sp.market, sd.market) AS market,
    (COALESCE(sp.total_sales_sp, 0) + COALESCE(sd.total_sales_sd, 0)) AS total_sales,
    (COALESCE(sp.total_cost_sp, 0) + COALESCE(sd.total_cost_sd, 0)) AS total_cost,
    (COALESCE(sp.total_cost_sp, 0) + COALESCE(sd.total_cost_sd, 0)) /
    (COALESCE(sp.total_sales_sp, 0) + COALESCE(sd.total_sales_sd, 0)) AS avg_acos
FROM
    sp_data AS sp
RIGHT JOIN
    sd_data AS sd
ON
    sp.advertisedSku = sd.advertisedSku AND sp.market = sd.market
WHERE
    sp.advertisedSku IS NULL AND (COALESCE(sp.total_sales_sp, 0) + COALESCE(sd.total_sales_sd, 0))>0 AND     (COALESCE(sp.total_cost_sp, 0) + COALESCE(sd.total_cost_sd, 0)) /
    (COALESCE(sp.total_sales_sp, 0) + COALESCE(sd.total_sales_sd, 0)) < 0.2),
-- 筛选出开设推荐商品投放的sku信息
sku_sd as (SELECT
  sd.sku,
  sd.market ,
  a.campaignName
FROM
( SELECT campaignId, campaignName, market FROM amazon_targeting_reports_sd WHERE targetingText = 'similar-product' GROUP BY campaignName, market) a
  JOIN amazon_productads_list_sd sd ON a.campaignId = sd.campaignId
  AND a.market = sd.market WHERE a.market = '{market}'
GROUP BY
  sd.sku,
  sd.market,
  a.campaignId),

-- 找出sku与parentasin对应关系
sku_par as (SELECT
  api.sku,
  api.market,
  apie.parent_asins
FROM
  amazon_product_info api
  JOIN amazon_product_info_extended apie ON api.asin = apie.asin
  AND api.market = apie.market),

 -- 筛选得到未投放推荐商品的sku
 sku_info as  (SELECT
  md.sku,
  md.total_sales,
  md.total_cost,
  md.avg_acos,
  md.market
FROM
  market_data md
  LEFT JOIN sku_sd ss ON md.sku = ss.sku
  AND md.market = ss.market
  WHERE ss.campaignName is NULL)

SELECT
  si.sku,
  si.market as sku_market,
  sp.parent_asins,
  sp.market
FROM
  sku_par sp
  JOIN sku_info si ON sp.sku = si.sku
WHERE
  sp.market = '{self.depository}'
  AND sp.parent_asins != ''

UNION

SELECT
  a.sku,
  a.market AS sku_market,
  sp.parent_asins,
  sp.market
FROM
  (
  SELECT
    si.sku,
    si.market

  FROM
    sku_par sp
    JOIN sku_info si ON sp.sku = si.sku
  WHERE
    sp.market = '{self.depository}'
    AND sp.parent_asins = ''
  ) a
  JOIN sku_par sp ON a.sku = sp.sku
  AND a.market = sp.market
                        """

            df = pd.read_sql(query, con=conn)
            output_filename = f'{self.brand}_{market}_{startdate}_{enddate}_0511_sku.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            # return df
            return output_filename

        except Exception as error:
            print("1-1.1Error while inserting data:", error)


SdRecommendedProductAdStrategy('OutdoorMaster').get_0511_sd_recommended_product_ad('IT','2024-07-01','2024-07-14')
