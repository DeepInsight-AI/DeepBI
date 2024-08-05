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


class SdAsinDerivativeStrategyQuery:

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

    def get_SD_ASIN_Derivative_Strategy_Query(self, market, date):

        try:
            conn = self.conn
            query = f"""
WITH sp_asin AS (SELECT
  campaignId,
  market,
  SUBSTRING(keyword,7,10) as targeting_asin,
  campaignName,
  sum( sales7d ) total_sales,
  sum( cost ) total_cost,
  sum( cost )/ sum( sales7d ) acos
FROM
  amazon_targeting_reports_sp
WHERE
  keyword LIKE 'asin=%'
  AND market = '{market}'
  AND ( DATE BETWEEN DATE_SUB( cast( '{date}' AS DATE ), INTERVAL 29 DAY ) AND '{date}' )
GROUP BY
  campaignId,
  market,
  keyword
HAVING
  sum( cost )/ sum( sales7d ) < 0.2),

-- 确定sp自动广告名单
sp_auto AS
(SELECT
  campaignId,
  market,
  campaignName
FROM
  amazon_targeting_reports_sp
WHERE
  targeting IN ( 'substitutes', 'complements', 'loose-match', 'close-match' )
  AND market = '{market}'
GROUP BY
  campaignId,
  market ),

--  sp自动广告搜索词中优质ASIN并联合sp-asin数据
asin_data AS (
  SELECT
    a.campaignId,
    a.market,
    UPPER(b.searchTerm)  AS targeting_asin,
    a.campaignName,
  sum( sales7d ) total_sales,
  sum( cost ) total_cost,
  sum( cost )/ sum( sales7d ) acos
  FROM
    sp_auto a
    JOIN amazon_search_term_reports_sp b ON a.campaignId = b.campaignId
    AND a.market = b.market
  WHERE
    b.DATE BETWEEN DATE_SUB( cast( '{date}' AS DATE ), INTERVAL 13 DAY )
    AND '{date}'
    AND LENGTH( searchTerm ) = 10
  GROUP BY
    a.campaignId,
    a.market,
    b.searchTerm
  HAVING
  sum( b.cost )/ sum( b.sales7d ) < 0.2
  UNION

  SELECT * FROM sp_asin),


-- 获取对应SP广告信息
campaign_data AS (SELECT
  market,
  campaignId,
  campaignName
FROM
  asin_data
GROUP BY
  market,
  campaignId),

    -- 获取SP广告活动中优质商品ASIN
campaign_asin_data as  (SELECT
  a.campaignId,
  a.market,
  b.advertisedAsin as asin
FROM
  campaign_data a
  JOIN amazon_advertised_product_reports_sp b ON a.campaignId = b.campaignId
  AND a.market = b.market
GROUP BY
  a.campaignId,
  a.market,
  b.advertisedAsin
  HAVING sum( cost )/ sum( sales7d )<0.2),

 -- 寻找asin在德国的parentasin
de_parentasin as  (SELECT
  a.campaignId,
  a.market AS asin_market,
  a.asin,
  b.parent_asins,
  b.market AS parentasin_market
FROM
  campaign_asin_data a
  JOIN amazon_product_info_extended b ON a.asin = b.asin
WHERE
  b.market = '{self.depository}'),

 -- 在德国找不到parentasin的asin
de_null as   (SELECT
  campaignId,
  asin_market,
  asin
FROM
  de_parentasin
WHERE
  parent_asins = ''),

 -- 在本国寻找parentasin
fyx_parentasin AS (
SELECT
  a.campaignId,
  a.asin_market,
  a.asin,
  b.parent_asins,
  b.market AS parentasin_market
FROM
  de_null a
  JOIN amazon_product_info_extended b ON a.asin = b.asin
  AND a.asin_market = b.market
  ),

  -- 找不到parentasin，即以自身为parentasin并联合之前数据
parentasin_data as  (SELECT
  campaignId,
  asin_market,
  asin,
  asin AS parent_asins,
  'X' AS parentasin_market
FROM
  fyx_parentasin a
WHERE
  parent_asins = ''

  UNION
SELECT * FROM fyx_parentasin WHERE parent_asins != ''

UNION
SELECT * FROM de_parentasin WHERE parent_asins != ''),

market_data as (SELECT
  a.campaignId,
  a.asin_market,
  a.parent_asins,
  a.parentasin_market,
  b.targeting_asin
FROM
  parentasin_data a
  JOIN asin_data b
WHERE
  a.campaignId = b.campaignId
  AND a.asin_market = b.market)


SELECT
  asin_market,
  parent_asins,
  parentasin_market,
  targeting_asin
FROM
  market_data
GROUP BY
  asin_market,
  parent_asins,
  targeting_asin
                        """

            df = pd.read_sql(query, con=conn)
            output_filename = f'{self.brand}_{market}_{date}_SD_ASIN.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            # return df
            return output_filename

        except Exception as error:
            print("1-1.1Error while inserting data:", error)


#SdAsinDerivativeStrategyQuery('OutdoorMaster').get_SD_ASIN_Derivative_Strategy_Query('ES','2024-07-17')
