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


class SkuAndCountryLinkedFourAdsCreation:

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

    def get_sku_and_country_sp_manual_creation(self, sku_info):

        try:
            conn = self.conn
            query = f"""
WITH sp_ci_data AS (
  SELECT
    campaignName,
    market,
    SUM( cost ) as sum_cost,
    SUM( sales1d ) as sum_sale,
    SUM( cost )/ SUM( sales1d ) AS acos
  FROM
    amazon_targeting_reports_sp
  WHERE
    matchType IN ( 'EXACT', 'BROAD', 'PHRASE' )
    AND DATE BETWEEN DATE_SUB( CURDATE(), INTERVAL 7 DAY )
    AND DATE (
    NOW())
  GROUP BY
    campaignName,
    market
  HAVING
    SUM( sales1d ) > 0
  ),
  sp_data AS (
  SELECT
    market,
    campaignName,
    advertisedSku,
    SUM( cost ) as sum_cost,
    SUM( sales1d ) as sum_sale,
    SUM( cost )/ SUM( sales1d ) AS acos
  FROM
    amazon_advertised_product_reports_sp
  WHERE
    advertisedSku IN %(column1_values1)s
  GROUP BY
    market,
    campaignName,
    advertisedSku
  )
  SELECT
  scd.campaignName,
  scd.market,
  sp_data.acos,
  sp_data.sum_cost,
  sp_data.sum_sale,
  sp_data.advertisedSku
FROM
  sp_ci_data scd
  JOIN sp_data  ON scd.campaignName = sp_data.campaignName
  AND scd.market = sp_data.market where sp_data.acos <0.24
                        """

            df = pd.read_sql(query, con=conn, params={'column1_values1': sku_info})
            output_filename = f'{self.brand}_sp_manual.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            # return df
            return output_filename

        except Exception as error:
            print("1-1.1Error while inserting data:", error)

    def get_sku_and_country_sp_asin_creation(self, sku_info):

        try:
            conn = self.conn
            query = """
WITH sp_asin_data AS (
  SELECT
  campaignName,
  market,
  SUM( cost ) AS sum_cost,
  SUM( sales1d ) AS sum_sale,
  SUM( cost )/ SUM( sales1d ) AS acos
FROM
  amazon_targeting_reports_sp
WHERE
  ( keyword LIKE 'asin=%%' OR keyword LIKE 'category=%%' )
  AND DATE BETWEEN DATE_SUB( CURDATE(), INTERVAL 7 DAY )
  AND DATE (
  NOW())
GROUP BY
  campaignName,
  market
HAVING
  SUM( sales1d ) > 0
  ),
  sp_data AS (
  SELECT
    market,
    campaignName,
    advertisedSku,
    SUM( cost ) as sum_cost,
    SUM( sales1d ) as sum_sale,
    SUM( cost )/ SUM( sales1d ) AS acos
  FROM
    amazon_advertised_product_reports_sp
  WHERE
    advertisedSku IN %(column1_values1)s
  GROUP BY
    market,
    campaignName,
    advertisedSku
  )
  SELECT
  sad.campaignName,
  sad.market,
  sp_data.acos,
  sp_data.sum_cost,
  sp_data.sum_sale,
  sp_data.advertisedSku
FROM
  sp_asin_data sad
  JOIN sp_data  ON sad.campaignName = sp_data.campaignName
  AND sad.market = sp_data.market where sp_data.acos <0.24;
                        """

            df = pd.read_sql(query, con=conn, params={'column1_values1': sku_info})
            output_filename = f'{self.brand}_sp_asin.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            # return df
            return output_filename

        except Exception as error:
            print("1-1.1Error while inserting data:", error)

    def get_sku_and_country_sp_auto_creation(self, sku_info):

        try:
            conn = self.conn
            query = """
WITH sp_auto_data AS (
  SELECT
    campaignName,
    market,
    SUM( cost ) AS sum_cost,
    SUM( sales1d ) AS sum_sale,
    SUM( cost )/ SUM( sales1d ) AS acos
  FROM
    amazon_targeting_reports_sp
  WHERE
    targeting IN ( 'substitutes', 'complements', 'loose-match', 'close-match' )
    AND DATE BETWEEN DATE_SUB( CURDATE(), INTERVAL 7 DAY )
    AND DATE (
    NOW())
  GROUP BY
    campaignName,
    market
  HAVING
    SUM( sales1d ) > 0
  ),
  sp_data AS (
  SELECT
    market,
    campaignName,
    advertisedSku
  FROM
    amazon_advertised_product_reports_sp
  WHERE
    advertisedSku IN %(column1_values1)s
  GROUP BY
    market,
    campaignName,
    advertisedSku
  ) SELECT
  sad.campaignName,
  sad.market,
  sad.acos,
  sad.sum_cost,
  sad.sum_sale,
  sp_data.advertisedSku
FROM
  sp_auto_data sad
  JOIN sp_data ON sad.campaignName = sp_data.campaignName
  AND sad.market = sp_data.market where sad.acos <0.24
                        """

            df = pd.read_sql(query, con=conn, params={'column1_values1': sku_info})
            output_filename = f'{self.brand}_sp_auto.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            # return df
            return output_filename

        except Exception as error:
            print("1-1.1Error while inserting data:", error)

    def get_sku_and_country_sd_creation(self, sku_info):

        try:
            conn = self.conn
            query = """
WITH sd1 AS (
 SELECT
    campaignName,
    market,
    promotedSku,
    SUM( cost ) AS sum_cost,
    SUM( sales ) AS sum_sale,
    SUM( cost )/ SUM( sales ) AS acos
  FROM
    amazon_advertised_product_reports_sd
  WHERE
    DATE BETWEEN DATE_SUB( CURDATE(), INTERVAL 7 DAY )
    AND DATE (
    NOW())
  GROUP BY
    campaignName,
    market,
    promotedSku
  HAVING
    SUM( sales ) > 0
  ),
  sd2 AS (
  SELECT
    market,
    campaignName,
    promotedSku
  FROM
    amazon_advertised_product_reports_sd
  WHERE
    promotedSku IN %(column1_values1)s
  GROUP BY
    market,
    campaignName,
    promotedSku
  ) SELECT
  sd1.campaignName,
  sd1.market,
  sd1.acos,
  sd1.sum_cost,
  sd1.sum_sale,
  sd2.promotedSku
FROM
  sd1
  JOIN sd2 ON sd1.campaignName = sd2.campaignName
  AND sd1.market = sd2.market AND sd1.promotedSku = sd2.promotedSku where sd1.acos <0.24
                        """

            df = pd.read_sql(query, con=conn, params={'column1_values1': sku_info})
            output_filename = f'{self.brand}_sd.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            # return df
            return output_filename

        except Exception as error:
            print("1-1.1Error while inserting data:", error)


# SkuAndCountryLinkedFourAdsCreation('OutdoorMaster').get_sku_and_country_sp_manual_creation(['E01989', '804902-EU', 'E00349','E01333','E00165','E00351','E01403','E00362'])
# SkuAndCountryLinkedFourAdsCreation('OutdoorMaster').get_sku_and_country_sp_asin_creation(['E01989', '804902-EU', 'E00349','E01333','E00165','E00351','E01403','E00362'])
# SkuAndCountryLinkedFourAdsCreation('OutdoorMaster').get_sku_and_country_sd_creation(['E01989', '804902-EU', 'E00349','E01333','E00165','E00351','E01403','E00362'])
