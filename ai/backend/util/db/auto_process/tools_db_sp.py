import json
import os
from datetime import datetime

import pandas as pd
import pymysql
from ai.backend.util.db.configuration.path import get_config_path



def get_timestamp():
    # 获取当前时间
    current_time = datetime.now()
    timestamp = int(current_time.timestamp())
    date_string = current_time.strftime("%Y-%m-%d")
    # 组合日期和时间戳
    date_timestamp_string = f"{date_string}_{timestamp}"
    return date_timestamp_string

class DbSpTools:
    def __init__(self, brand,market):
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

    def get_sp_SkuAdgroupCamapign(self, market,startdate,enddate,start_acos,end_acos,adjuest):
        # 低于 平均ACOS值 30% 以上的  campaign 广告活动
        # 建议执行的操作：预算提升30%
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            query = """
with tempacos as (
SELECT case when SUM(cost)/SUM(sales14d)<0.2 then 0.2  else SUM(cost)/SUM(sales14d) end AS avgacos
FROM amazon_campaign_reports_sp where (date between '{}' and '{}') and market='{}' and campaignStatus='ENABLED'
)
,temp2 as (
select
market,campaignName,campaignId,campaignBudgetAmount AS budget_old,campaignBudgetAmount*(1+{}) as budget_new,
(select avgacos from tempacos) as standards_acos,SUM(cost)/SUM(sales14d) as acos,
'camapign_调整:'+'{}' as beizhu,'todo' as status
from amazon_campaign_reports_sp
where (date between '{}' and '{}') and market='{}' and campaignStatus='ENABLED'
group by market,campaignName,campaignId
having SUM(cost)/SUM(sales14d) >(select avgacos from tempacos)*(1+{}) and SUM(cost)/SUM(sales14d) <(select avgacos from tempacos)*(1+{}) and SUM(sales14d)>0
)
select
*
from temp2
             """.format(startdate,enddate,market,adjuest,adjuest,startdate,enddate,market,start_acos,end_acos)
            df = pd.read_sql(query, con=conn)
            return df
            for index, row in df.iterrows():
                print(f"Index: {index}")
                print(row)
                print()  # 添加空行分隔每一行

            print("get_sp_SkuAdgroupCamapign Data query successfully!")

        except Exception as error:
            print("get_sp_SkuAdgroupCamapign Error while query data:", error)

    def get_sp_adgroup_update(self, market, startdate, enddate, start_acos, end_acos, adjuest):
        # 查找需要操作的广告组
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            query = """
with tempacos as (
SELECT case when SUM(cost)/SUM(sales14d)<0.2 then 0.2  else SUM(cost)/SUM(sales14d) end AS avgacos
FROM amazon_ad_group_reports_sp where (date between '{}' and '{}') and market='{}' and adStatus='ENABLED'
)
,temp2 as (
select
 market,adGroupName,adGroupId,
(select avgacos from tempacos) as standards_acos,SUM(cost)/SUM(sales14d) as acos,{} as adjuset,
'adgroup_调整:' as beizhu,'todo' as status
from amazon_ad_group_reports_sp
where (date between '{}' and '{}') and market='{}' and adStatus='ENABLED'
group by market,adGroupName,adGroupId
having SUM(cost)/SUM(sales14d) >(select avgacos from tempacos)*(1+{}) and SUM(cost)/SUM(sales14d) <(select avgacos from tempacos)*(1+{}) and SUM(sales14d)>0
)
select
*
from temp2
             """.format(startdate, enddate, market, adjuest, startdate, enddate, market, start_acos,
                        end_acos)
            pd.set_option('display.max_columns', None)
            pd.set_option('display.max_rows', None)
            df = pd.read_sql(query, con=conn)
            return df
            for index, row in df.iterrows():
                print(f"Index: {index}")
                print(row)
                print()

            print("get_sp_adgroup_update Data query successfully!")

        except Exception as error:
            print("get_sp_adgroup_update Error while query data:", error)

    def select_sd_campaign_name(self, market,product):
        try:
            conn = self.conn

            query = """SELECT campaignName FROM amazon_campaign_reports_sd
            WHERE market = '{}'
            AND LOWER(campaignName) LIKE LOWER('%{}%')""".format(market, product)
            df = pd.read_sql(query, con=conn)
            if df.empty:
                print("No campaign name")
                return ["faile"]
            else:
                print("campaignName is already exist")
                return ["success"]
        except Exception as e:
            print(f"Error occurred when select_sd_campaign_name: {e}")

    def select_sp_campaign_name(self, market,product):
        try:
            conn = self.conn

            query = """SELECT campaignName FROM amazon_campaign_reports_sp
            WHERE market = '{}'
            AND LOWER(campaignName) LIKE LOWER('%{}%')""".format(market, product)
            df = pd.read_sql(query, con=conn)
            if df.empty:
                print("No campaign name")
                return ["faile"]
            else:
                print("campaignName is already exist")
                return ["success"]
        except Exception as e:
            print(f"Error occurred when select_sd_campaign_name: {e}")

    def select_sd_product_sku(self, market,product):
        try:
            conn = self.conn
            if market in ('US', 'JP', 'UK'):
                sku = f"{market.lower()}sku"
            else:
                sku = "frsku"
            query = """
            SELECT {}
FROM prod_as_product_base
WHERE sspu = '{}'
and base_market = 'US'
GROUP BY {}
            """.format(sku, product, sku)
            df = pd.read_sql(query, con=conn)
            if df.empty:
                print("No product sku")
            else:
                print("select sd product sku success")
                return df[sku].tolist()
        except Exception as e:
            print(f"Error occurred when select_sd_product_sku: {e}")

    def select_sp_product_asin(self, market1,market2,asin):
        try:
            conn = self.conn
            asin1 = f"{market1.lower()}asin"
            asin2 = f"{market2.lower()}asin"
            query = """
           SELECT {}
    FROM prod_as_product_base
    WHERE {} = '{}'
            """.format(asin1, asin2, asin)
            df = pd.read_sql(query, con=conn)
            isales = df.loc[0, asin1]
            return isales
        except Exception as e:
            print(f"Error occurred when select_sd_product_sku: {e}")

    def select_sp_product_sku(self, market1,market2,advertisedSku):
        try:
            conn = self.conn
            market1_sku = "frsku"
            market2_sku = f"{market2.lower()}sku"
            query = """
            SELECT {} FROM prod_as_product_base
WHERE base_market = 'US'
and nsspu = (
SELECT nsspu FROM prod_as_product_base
WHERE  base_market = 'US'
and {} = '{}'
GROUP BY nsspu
)
GROUP BY {}
            """.format(market1_sku, market2_sku, advertisedSku,market1_sku)
            df = pd.read_sql(query, con=conn)
            if df.empty:
                print("No product sku")
            else:
                print("select sp product sku success")
                return df[market1_sku].tolist()
        except Exception as e:
            print(f"Error occurred when select_sp_product_sku: {e}")

    def select_product_sku(self, market1,market2,advertisedSku):
        try:
            conn = self.conn
            market1_sku = f"{market1.lower()}sku"
            market2_sku = f"{market2.lower()}sku"
            query = """
            SELECT {} FROM prod_as_product_base
WHERE base_market = 'US'
and nsspu in (
SELECT nsspu FROM prod_as_product_base
WHERE  base_market = 'US'
and {} in %(column1_values1)s
GROUP BY nsspu
)
GROUP BY {}
            """.format(market1_sku, market2_sku,market1_sku)
            df = pd.read_sql(query, con=conn, params={'column1_values1': advertisedSku})
            if df.empty:
                print("No product sku")
            else:
                print("select sp product sku success")
                return df[market1_sku].tolist()
        except Exception as e:
            print(f"Error occurred when select_product_sku: {e}")

    def select_product_sku_by_asin(self, market1,market2,advertisedSku,depository):
        try:
            conn = self.conn
            query1 = f"""
    SELECT
		amazon_product_info_extended.parent_asins AS nsspu
	FROM
		amazon_product_info
		LEFT JOIN amazon_product_info_extended ON amazon_product_info_extended.asin = amazon_product_info.asin
	WHERE
		amazon_product_info.market = '{market2}'
		AND amazon_product_info_extended.market = '{depository}'
	AND amazon_product_info.sku IN %(column1_values1)s
            """
            df1 = pd.read_sql(query1, con=conn, params={'column1_values1': advertisedSku})
            if df1.empty or (df1['nsspu'].str.strip() == '').all():
                print("No product")
                return advertisedSku
            column1_values2 = df1['nsspu'].tolist()
            query2 = f"""
SELECT DISTINCT
	amazon_product_info.sku
FROM
	amazon_product_info
	LEFT JOIN amazon_product_info_extended ON amazon_product_info_extended.asin = amazon_product_info.asin
WHERE
	amazon_product_info.market = '{depository}'
	AND amazon_product_info_extended.market = '{depository}'
	AND amazon_product_info_extended.parent_asins IN %(column1_values2)s
            """
            df = pd.read_sql(query2, con=conn, params={'column1_values2': column1_values2})
            if df.empty:
                print("No product sku")
            else:
                print("select product sku success")
                return df['sku'].tolist()
        except Exception as e:
            print(f"Error occurred when select_product_sku_by_asin: {e}")

    def select_product_sku_by_parent_asin(self, parent_asins, depository, market):
        try:
            conn = self.conn
            query = f"""
            SELECT DISTINCT
	amazon_product_info.sku
FROM
	amazon_product_info
	LEFT JOIN amazon_product_info_extended ON amazon_product_info_extended.asin = amazon_product_info.asin
WHERE
	amazon_product_info.market = '{depository}'
	AND amazon_product_info_extended.market = '{depository}'
	AND amazon_product_info_extended.parent_asins = '{parent_asins}'
            """
            df = pd.read_sql(query, con=conn)
            if df.empty:
                query1 = f"""
                            SELECT DISTINCT sku
                            FROM amazon_product_info
                            WHERE asin = '{parent_asins}'
                            AND market = '{market}'
                            """
                df1 = pd.read_sql(query1, con=conn)
                return df1['sku'].tolist()
            else:
                print("select product sku success")
                return df['sku'].tolist()
        except Exception as e:
            print(f"Error occurred when select_product_sku_by_asin: {e}")
    def select_sp_sspu_name(self,market,sspu):
        try:
            conn = self.conn
            sspu1 = sspu.lower()
            query = """
SELECT DISTINCT campaign_name,campaignId FROM amazon_campaigns_list_sp
WHERE market = '{}'
AND state != 'ARCHIVED'
AND (campaign_name LIKE '%{}%' OR campaign_name LIKE '%{}%')
                    """.format(market,sspu,sspu1)
            df = pd.read_sql(query, con=conn)
            if df.empty:
                print("No campaignName")
                return None, None
            else:
                print("select campaignName success")
                return df['campaign_name'].tolist(), df['campaignId'].tolist()
        except Exception as e:
            print(f"Error occurred when select_product_sku_by_asin: {e}")

    def select_sd_sspu_name(self,market,sspu):
        try:
            conn = self.conn
            sspu1 = sspu.lower()
            query = """
SELECT DISTINCT campaignName,campaignId FROM amazon_campaigns_list_sd
WHERE market = '{}'
AND state != 'archived'
AND (campaignName LIKE '%{}%' OR campaignName LIKE '%{}%')
                    """.format(market,sspu,sspu1)
            df = pd.read_sql(query, con=conn)
            if df.empty:
                print("No campaignName")
                return None, None
            else:
                print("select campaignName success")
                return df['campaignName'].tolist(), df['campaignId'].tolist()
        except Exception as e:
            print(f"Error occurred when select_sd_sspu_name: {e}")

    def select_sp_sspu_name_overstock(self,market,sspu):
        try:
            conn = self.conn
            sspu1 = sspu.lower()
            query = """
SELECT DISTINCT campaign_name, campaignId
FROM amazon_campaigns_list_sp
WHERE
    market = '{}' AND
    campaign_name LIKE '%_overstock' AND
    (
        campaign_name LIKE '%{}%' OR
        campaign_name LIKE '%{}%'
    )
                    """.format(market,sspu,sspu1)
            df = pd.read_sql(query, con=conn)
            if df.empty:
                print("No campaignName")
                return None, None
            else:
                print("select campaignName success")
                return df['campaign_name'].tolist(), df['campaignId'].tolist()
        except Exception as e:
            print(f"Error occurred when select_product_sku_by_asin: {e}")

    def select_sd_sspu_name_overstock(self,market,sspu):
        try:
            conn = self.conn
            sspu1 = sspu.lower()
            query = """
SELECT DISTINCT campaignName, campaignId
FROM amazon_campaigns_list_sd
WHERE
    market = '{}' AND
    campaignName LIKE '%_overstock' AND
    (
        campaignName LIKE '%{}%' OR
        campaignName LIKE '%{}%'
    )
                    """.format(market,sspu,sspu1)
            df = pd.read_sql(query, con=conn)
            if df.empty:
                print("No campaignName")
                return None, None
            else:
                print("select campaignName success")
                return df['campaignName'].tolist(), df['campaignId'].tolist()
        except Exception as e:
            print(f"Error occurred when select_sd_sspu_name: {e}")

    def select_sp_campaign(self,market):
        try:
            conn = self.conn
            query = """
SELECT DISTINCT campaignId FROM amazon_campaigns_list_sp
WHERE market = '{}'
AND state = 'ENABLED'
                    """.format(market)
            df = pd.read_sql(query, con=conn)
            if df.empty:
                print("No campaignId")
                return None
            else:
                print("select campaignId success")
                return df['campaignId'].tolist()
        except Exception as e:
            print(f"Error occurred when select_product_sku_by_asin: {e}")

    def select_sp_campaignid_search_term(self,market,curtime,campaignid):
        try:
            conn = self.conn
            query = """
WITH Campaign_Stats AS (
    SELECT
        acr.campaignId,
        acr.campaignName,
        acr.campaignBudgetAmount AS Budget,
        acr.market,
        SUM(CASE WHEN acr.date = DATE_SUB('{}', INTERVAL 2 DAY) THEN acr.cost ELSE 0 END) AS cost_yesterday,
        SUM(CASE WHEN acr.date = DATE_SUB('{}', INTERVAL 2 DAY) THEN acr.clicks ELSE 0 END) AS clicks_yesterday,
        SUM(CASE WHEN acr.date = DATE_SUB('{}', INTERVAL 2 DAY) THEN acr.sales14d ELSE 0 END) AS sales_yesterday,
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
    JOIN amazon_campaigns_list_sp acl ON acr.campaignId = acl.campaignId
    WHERE
        acr.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY)
        AND ('{}' - INTERVAL 1 DAY)
        AND acr.campaignId IN (SELECT campaignId FROM amazon_campaign_reports_sp WHERE campaignStatus = 'ENABLED' AND date = '{}' - INTERVAL 1 DAY )
        AND acr.market = '{}'
    GROUP BY
        acr.campaignName
),
b AS (
    SELECT
        SUM(reports.cost) / SUM(reports.sales14d) AS country_avg_ACOS_1m,
        reports.market
    FROM
        amazon_campaign_reports_sp AS reports
    INNER JOIN amazon_campaigns_list_sp AS campaigns ON reports.campaignId = campaigns.campaignId
    WHERE
        reports.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY)
        AND ('{}' - INTERVAL 1 DAY)
        AND campaigns.campaignId IN (SELECT campaignId FROM amazon_campaign_reports_sp WHERE campaignStatus = 'ENABLED' AND date = '{}' - INTERVAL 1 DAY)
        AND reports.market = '{}'
    GROUP BY
        reports.market
),
TargetCampaignIds AS (
    SELECT DISTINCT campaignId, adGroupId
    FROM amazon_sp_productads_list AS T1
    WHERE EXISTS (
        SELECT 1
        FROM amazon_sp_productads_list AS T2
        WHERE T2.campaignId = {}
          AND T2.market = '{}'
          AND T2.asin = T1.asin
      )
      AND EXISTS (
        SELECT 1
        FROM amazon_keywords_list_sp AS T4
        WHERE T4.campaignId = T1.campaignId
          AND T4.market = '{}'
          AND T4.keywordText != '(_targeting_auto_)'
          AND T4.extendedData_servingStatus ='TARGETING_CLAUSE_STATUS_LIVE'
      )
    GROUP BY T1.campaignId, T1.adGroupId
    HAVING COUNT(DISTINCT CASE WHEN T1.campaignId = {} THEN T1.asin ELSE NULL END) * 1.0 / COUNT(DISTINCT T1.asin) <= 0.5
),
CampaignStatsResult AS (
  SELECT
    tci.adGroupId,
    cs.*,
    b.country_avg_ACOS_1m
FROM
    Campaign_Stats cs
JOIN b ON cs.market = b.market
JOIN TargetCampaignIds tci ON cs.campaignId = tci.campaignId
WHERE
  cs.campaignId IN (SELECT campaignId FROM TargetCampaignIds)
  AND (SELECT COUNT(DISTINCT campaignId) FROM TargetCampaignIds) = 1
UNION ALL
SELECT
    tci.adGroupId,
    cs.*,
    b.country_avg_ACOS_1m
FROM
    Campaign_Stats cs
JOIN b ON cs.market = b.market
JOIN TargetCampaignIds tci ON cs.campaignId = tci.campaignId
WHERE (SELECT COUNT(DISTINCT campaignId) FROM TargetCampaignIds) > 1 AND (cs.ACOS_30d <= 0.36 OR cs.ACOS_30d IS NULL)
)
SELECT
    *
FROM
    CampaignStatsResult
ORDER BY
    total_sales14d_30d DESC
LIMIT 1
                    """.format(curtime,curtime,curtime,curtime,curtime,curtime,curtime,curtime,curtime,curtime,
                               curtime,curtime,curtime,curtime,curtime,curtime,curtime,curtime,curtime,curtime,
                               curtime,curtime,curtime,curtime,curtime,curtime,curtime,curtime,market,curtime,curtime,curtime,market,campaignid,market,market,campaignid)
            df = pd.read_sql(query, con=conn)
            if df.empty:
                print("No campaignId")
                return None,None,None
            else:
                print("select campaignId success")
                return df.loc[0,'campaignId'],df.loc[0,'campaignName'],df.loc[0,'adGroupId']
        except Exception as e:
            print(f"Error occurred when select_product_sku_by_asin: {e}")

    def select_sp_campaignid_search_term_jiutong(self, market, curtime, campaignid):
        try:
            conn = self.conn
            query = """
WITH Campaign_Stats AS (
    SELECT
        acr.campaignId,
        acr.campaignName,
        acr.campaignBudgetAmount AS Budget,
        acr.market,
        SUM(CASE WHEN acr.date = DATE_SUB('{}', INTERVAL 2 DAY) THEN acr.cost ELSE 0 END) AS cost_yesterday,
        SUM(CASE WHEN acr.date = DATE_SUB('{}', INTERVAL 2 DAY) THEN acr.clicks ELSE 0 END) AS clicks_yesterday,
        SUM(CASE WHEN acr.date = DATE_SUB('{}', INTERVAL 2 DAY) THEN acr.sales14d ELSE 0 END) AS sales_yesterday,
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
    JOIN amazon_campaigns_list_sp acl ON acr.campaignId = acl.campaignId
    WHERE
        acr.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY)
        AND ('{}' - INTERVAL 1 DAY)
        AND acr.campaignId IN (SELECT campaignId FROM amazon_campaign_reports_sp WHERE campaignStatus = 'ENABLED' AND date = '{}' - INTERVAL 1 DAY )
        AND acr.market = '{}'
    GROUP BY
        acr.campaignName
),
b AS (
    SELECT
        SUM(reports.cost) / SUM(reports.sales14d) AS country_avg_ACOS_1m,
        reports.market
    FROM
        amazon_campaign_reports_sp AS reports
    INNER JOIN amazon_campaigns_list_sp AS campaigns ON reports.campaignId = campaigns.campaignId
    WHERE
        reports.date BETWEEN DATE_SUB('{}', INTERVAL 30 DAY)
        AND ('{}' - INTERVAL 1 DAY)
        AND campaigns.campaignId IN (SELECT campaignId FROM amazon_campaign_reports_sp WHERE campaignStatus = 'ENABLED' AND date = '{}' - INTERVAL 1 DAY)
        AND reports.market = '{}'
    GROUP BY
        reports.market
),
TargetCampaignIds AS (
    SELECT DISTINCT campaignId, adGroupId
    FROM amazon_sp_productads_list AS T1
    WHERE EXISTS (
        SELECT 1
        FROM amazon_sp_productads_list AS T2
        WHERE T2.campaignId = {}
          AND T2.market = '{}'
          AND T2.asin = T1.asin
      )
      AND EXISTS (
        SELECT 1
        FROM amazon_keywords_list_sp AS T4
        WHERE T4.campaignId = T1.campaignId
          AND T4.market = '{}'
          AND T4.keywordText != '(_targeting_auto_)'
          AND T4.extendedData_servingStatus ='TARGETING_CLAUSE_STATUS_LIVE'
      )
    GROUP BY T1.campaignId, T1.adGroupId
    HAVING COUNT(DISTINCT CASE WHEN T1.campaignId = {} THEN T1.asin ELSE NULL END) * 1.0 / COUNT(DISTINCT T1.asin) <= 0.5
),
CampaignStatsResult AS (
  SELECT
    tci.adGroupId, --  从TargetCampaignIds中选择adGroupId, 放置在campaignId之前
    cs.*,
    b.country_avg_ACOS_1m
  FROM
    Campaign_Stats cs
  JOIN b ON cs.market = b.market
  JOIN TargetCampaignIds tci ON cs.campaignId = tci.campaignId
  WHERE
    cs.campaignId IN (SELECT campaignId FROM TargetCampaignIds)
    AND cs.campaignName LIKE '%Deep%'
)
SELECT
    *
FROM
    CampaignStatsResult
ORDER BY
    total_sales14d_30d DESC
LIMIT 1
                    """.format(curtime, curtime, curtime, curtime, curtime, curtime, curtime, curtime, curtime,
                               curtime,
                               curtime, curtime, curtime, curtime, curtime, curtime, curtime, curtime, curtime,
                               curtime,
                               curtime, curtime, curtime, curtime, curtime, curtime, curtime, curtime, market,
                               curtime, curtime, curtime, market, campaignid, market, market, campaignid)
            df = pd.read_sql(query, con=conn)
            if df.empty:
                print("No campaignId")
                return None, None, None
            else:
                print("select campaignId success")
                return df.loc[0, 'campaignId'], df.loc[0, 'campaignName'], df.loc[0, 'adGroupId']
        except Exception as e:
            print(f"Error occurred when select_product_sku_by_asin: {e}")

    def select_sp_asin_campaignid_search_term(self, market, curtime, campaignid):
        try:
            conn = self.conn
            query = f"""
WITH Campaign_Stats AS (
    SELECT
        acr.campaignId,
        acr.campaignName,
        acr.campaignBudgetAmount AS Budget,
        acr.market,
        SUM(CASE WHEN acr.date = DATE_SUB('{curtime}', INTERVAL 2 DAY) THEN acr.cost ELSE 0 END) AS cost_yesterday,
        SUM(CASE WHEN acr.date = DATE_SUB('{curtime}', INTERVAL 2 DAY) THEN acr.clicks ELSE 0 END) AS clicks_yesterday,
        SUM(CASE WHEN acr.date = DATE_SUB('{curtime}', INTERVAL 2 DAY) THEN acr.sales14d ELSE 0 END) AS sales_yesterday,
        SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{curtime}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{curtime}', INTERVAL 1 DAY) THEN acr.cost ELSE 0 END) AS total_cost_7d,
        SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{curtime}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{curtime}', INTERVAL 1 DAY) THEN acr.sales14d ELSE 0 END) AS total_sales14d_7d,
        SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{curtime}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{curtime}', INTERVAL 1 DAY) THEN acr.cost ELSE 0 END) AS total_cost_30d,
        SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{curtime}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{curtime}', INTERVAL 1 DAY) THEN acr.sales14d ELSE 0 END) AS total_sales14d_30d,
        SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{curtime}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{curtime}', INTERVAL 1 DAY) THEN acr.clicks ELSE 0 END) AS total_clicks_30d,
        SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{curtime}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{curtime}', INTERVAL 1 DAY) THEN acr.clicks ELSE 0 END) AS total_clicks_7d,
        SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{curtime}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{curtime}', INTERVAL 1 DAY) THEN acr.cost ELSE 0 END) / NULLIF(SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{curtime}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{curtime}', INTERVAL 1 DAY) THEN acr.sales14d ELSE 0 END), 0) AS ACOS_30d,
        SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{curtime}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{curtime}', INTERVAL 1 DAY) THEN acr.cost ELSE 0 END) / NULLIF(SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{curtime}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{curtime}', INTERVAL 1 DAY) THEN acr.sales14d ELSE 0 END), 0) AS ACOS_7d,
        SUM(CASE WHEN acr.date = '{curtime}' - INTERVAL 2 DAY THEN acr.cost ELSE 0 END) / NULLIF(SUM(CASE WHEN acr.date = '{curtime}' - INTERVAL 2 DAY THEN acr.sales14d ELSE 0 END), 0)  AS ACOS_yesterday
    FROM
        amazon_campaign_reports_sp acr
    JOIN amazon_campaigns_list_sp acl ON acr.campaignId = acl.campaignId
    WHERE
        acr.date BETWEEN DATE_SUB('{curtime}', INTERVAL 30 DAY)
        AND ('{curtime}' - INTERVAL 1 DAY)
        AND acr.campaignId IN (SELECT campaignId FROM amazon_campaign_reports_sp WHERE campaignStatus = 'ENABLED' AND date = '{curtime}' - INTERVAL 1 DAY )
        AND acr.market = '{market}'
    GROUP BY
        acr.campaignName
),
b AS (
    SELECT
        SUM(reports.cost) / SUM(reports.sales14d) AS country_avg_ACOS_1m,
        reports.market
    FROM
        amazon_campaign_reports_sp AS reports
    INNER JOIN amazon_campaigns_list_sp AS campaigns ON reports.campaignId = campaigns.campaignId
    WHERE
        reports.date BETWEEN DATE_SUB('{curtime}', INTERVAL 30 DAY)
        AND ('{curtime}' - INTERVAL 1 DAY)
        AND campaigns.campaignId IN (SELECT campaignId FROM amazon_campaign_reports_sp WHERE campaignStatus = 'ENABLED' AND date = '{curtime}' - INTERVAL 1 DAY)
        AND reports.market = '{market}'
    GROUP BY
        reports.market
),
TargetCampaignIds AS (
SELECT DISTINCT T1.adGroupId, T3.campaignId, T3.targetingType, T3.campaign_name, T3.state AS campaignStatus
FROM amazon_sp_productads_list AS T1
INNER JOIN amazon_campaigns_list_sp AS T3 ON T1.campaignId = T3.campaignId AND T3.market = '{market}'
WHERE T3.targetingType = 'MANUAL' AND T3.state = 'ENABLED'
  AND EXISTS (
    SELECT 1
    FROM amazon_sp_productads_list AS T2
    WHERE T2.campaignId = '{campaignid}'
      AND T2.market = '{market}'
      AND T2.asin = T1.asin
  )
  AND (T3.campaign_name LIKE '%0514%' OR T3.campaign_name LIKE '%ASIN%')
GROUP BY T1.adGroupId,T3.campaign_name, T3.state
HAVING COUNT(DISTINCT CASE WHEN T1.campaignId = '{campaignid}' THEN T1.asin ELSE NULL END) * 1.0 / COUNT(DISTINCT T1.asin) <= 0.5
),
CampaignStatsResult AS (
  SELECT
    cs.*,
    b.country_avg_ACOS_1m
FROM
    Campaign_Stats cs
JOIN b ON cs.market = b.market
WHERE
  cs.campaignId IN (SELECT campaignId FROM TargetCampaignIds)
  AND (SELECT COUNT(DISTINCT campaignId) FROM TargetCampaignIds) = 1
UNION ALL
SELECT
    cs.*,
    b.country_avg_ACOS_1m
FROM
    Campaign_Stats cs
JOIN b ON cs.market = b.market
JOIN TargetCampaignIds tci ON cs.campaignId = tci.campaignId
WHERE (SELECT COUNT(DISTINCT campaignId) FROM TargetCampaignIds) > 1 AND (cs.ACOS_30d <= 0.36 OR cs.ACOS_30d IS NULL)
)
SELECT
    tci.adGroupId,
    cs.*
FROM
    CampaignStatsResult cs
LEFT JOIN TargetCampaignIds tci ON cs.campaignId = tci.campaignId
ORDER BY
    total_sales14d_30d DESC
LIMIT 1;
                    """
            df = pd.read_sql(query, con=conn)
            if df.empty:
                print("No campaignId")
                return None, None, None
            else:
                print("select campaignId success")
                return df.loc[0, 'campaignId'], df.loc[0, 'campaignName'], df.loc[0, 'adGroupId']
        except Exception as e:
            print(f"Error occurred when select_product_sku_by_asin: {e}")

    def select_sp_asin_campaignid_search_term_jiutong(self, market, curtime, campaignid):
        try:
            conn = self.conn
            query = f"""
WITH Campaign_Stats AS (
    SELECT
        acr.campaignId,
        acr.campaignName,
        acr.campaignBudgetAmount AS Budget,
        acr.market,
        SUM(CASE WHEN acr.date = DATE_SUB('{curtime}', INTERVAL 2 DAY) THEN acr.cost ELSE 0 END) AS cost_yesterday,
        SUM(CASE WHEN acr.date = DATE_SUB('{curtime}', INTERVAL 2 DAY) THEN acr.clicks ELSE 0 END) AS clicks_yesterday,
        SUM(CASE WHEN acr.date = DATE_SUB('{curtime}', INTERVAL 2 DAY) THEN acr.sales14d ELSE 0 END) AS sales_yesterday,
        SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{curtime}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{curtime}', INTERVAL 1 DAY) THEN acr.cost ELSE 0 END) AS total_cost_7d,
        SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{curtime}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{curtime}', INTERVAL 1 DAY) THEN acr.sales14d ELSE 0 END) AS total_sales14d_7d,
        SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{curtime}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{curtime}', INTERVAL 1 DAY) THEN acr.cost ELSE 0 END) AS total_cost_30d,
        SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{curtime}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{curtime}', INTERVAL 1 DAY) THEN acr.sales14d ELSE 0 END) AS total_sales14d_30d,
        SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{curtime}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{curtime}', INTERVAL 1 DAY) THEN acr.clicks ELSE 0 END) AS total_clicks_30d,
        SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{curtime}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{curtime}', INTERVAL 1 DAY) THEN acr.clicks ELSE 0 END) AS total_clicks_7d,
        SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{curtime}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{curtime}', INTERVAL 1 DAY) THEN acr.cost ELSE 0 END) / NULLIF(SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{curtime}' - INTERVAL 1 DAY, INTERVAL 29 DAY) AND DATE_SUB('{curtime}', INTERVAL 1 DAY) THEN acr.sales14d ELSE 0 END), 0) AS ACOS_30d,
        SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{curtime}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{curtime}', INTERVAL 1 DAY) THEN acr.cost ELSE 0 END) / NULLIF(SUM(CASE WHEN acr.date BETWEEN DATE_SUB('{curtime}' - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB('{curtime}', INTERVAL 1 DAY) THEN acr.sales14d ELSE 0 END), 0) AS ACOS_7d,
        SUM(CASE WHEN acr.date = '{curtime}' - INTERVAL 2 DAY THEN acr.cost ELSE 0 END) / NULLIF(SUM(CASE WHEN acr.date = '{curtime}' - INTERVAL 2 DAY THEN acr.sales14d ELSE 0 END), 0)  AS ACOS_yesterday
    FROM
        amazon_campaign_reports_sp acr
    JOIN amazon_campaigns_list_sp acl ON acr.campaignId = acl.campaignId
    WHERE
        acr.date BETWEEN DATE_SUB('{curtime}', INTERVAL 30 DAY)
        AND ('{curtime}' - INTERVAL 1 DAY)
        AND acr.campaignId IN (SELECT campaignId FROM amazon_campaign_reports_sp WHERE campaignStatus = 'ENABLED' AND date = '{curtime}' - INTERVAL 1 DAY )
        AND acr.market = '{market}'
    GROUP BY
        acr.campaignName
),
b AS (
    SELECT
        SUM(reports.cost) / SUM(reports.sales14d) AS country_avg_ACOS_1m,
        reports.market
    FROM
        amazon_campaign_reports_sp AS reports
    INNER JOIN amazon_campaigns_list_sp AS campaigns ON reports.campaignId = campaigns.campaignId
    WHERE
        reports.date BETWEEN DATE_SUB('{curtime}', INTERVAL 30 DAY)
        AND ('{curtime}' - INTERVAL 1 DAY)
        AND campaigns.campaignId IN (SELECT campaignId FROM amazon_campaign_reports_sp WHERE campaignStatus = 'ENABLED' AND date = '{curtime}' - INTERVAL 1 DAY)
        AND reports.market = '{market}'
    GROUP BY
        reports.market
),
TargetCampaignIds AS (
SELECT DISTINCT T1.adGroupId, T3.campaignId, T3.targetingType, T3.campaign_name, T3.state AS campaignStatus
FROM amazon_sp_productads_list AS T1
INNER JOIN amazon_campaigns_list_sp AS T3 ON T1.campaignId = T3.campaignId AND T3.market = '{market}'
WHERE T3.targetingType = 'MANUAL' AND T3.state = 'ENABLED'
  AND EXISTS (
    SELECT 1
    FROM amazon_sp_productads_list AS T2
    WHERE T2.campaignId = '{campaignid}'
      AND T2.market = '{market}'
      AND T2.asin = T1.asin
  )
  AND (T3.campaign_name LIKE '%0514%')
GROUP BY T1.adGroupId,T3.campaign_name, T3.state
HAVING COUNT(DISTINCT CASE WHEN T1.campaignId = '{campaignid}' THEN T1.asin ELSE NULL END) * 1.0 / COUNT(DISTINCT T1.asin) <= 0.5
),
CampaignStatsResult AS (
  SELECT
    cs.*,
    b.country_avg_ACOS_1m
FROM
    Campaign_Stats cs
JOIN b ON cs.market = b.market
WHERE
  cs.campaignId IN (SELECT campaignId FROM TargetCampaignIds)
  AND (SELECT COUNT(DISTINCT campaignId) FROM TargetCampaignIds) = 1
UNION ALL
SELECT
    cs.*,
    b.country_avg_ACOS_1m
FROM
    Campaign_Stats cs
JOIN b ON cs.market = b.market
JOIN TargetCampaignIds tci ON cs.campaignId = tci.campaignId
WHERE (SELECT COUNT(DISTINCT campaignId) FROM TargetCampaignIds) > 1 AND (cs.ACOS_30d <= 0.36 OR cs.ACOS_30d IS NULL)
)
SELECT
    tci.adGroupId,
    cs.*
FROM
    CampaignStatsResult cs
LEFT JOIN TargetCampaignIds tci ON cs.campaignId = tci.campaignId
ORDER BY
    total_sales14d_30d DESC
LIMIT 1;
                    """
            df = pd.read_sql(query, con=conn)
            if df.empty:
                print("No campaignId")
                return None, None, None
            else:
                print("select campaignId success")
                return df.loc[0, 'campaignId'], df.loc[0, 'campaignName'], df.loc[0, 'adGroupId']
        except Exception as e:
            print(f"Error occurred when select_product_sku_by_asin: {e}")

    def select_sp_campaign_search_term(self,market,sspu):
        try:
            conn = self.conn
            query = f"""
SELECT
    b.sspu,
    a.searchTerm,
    a.keyword,
    a.keywordBid,
    a.adGroupName,
    a.adGroupId,
    a.matchType,
    a.campaignName,
    a.campaignId,
    SUM(CASE WHEN a.date BETWEEN DATE_SUB(CURDATE()- INTERVAL 1 DAY - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB(CURDATE()- INTERVAL 1 DAY, INTERVAL 1 DAY) THEN a.clicks ELSE 0 END) AS total_clicks_30d,
    SUM(CASE WHEN a.date BETWEEN DATE_SUB(CURDATE()- INTERVAL 1 DAY - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURDATE()- INTERVAL 1 DAY, INTERVAL 1 DAY) THEN a.clicks ELSE 0 END) AS total_clicks_7d,
    SUM(CASE WHEN a.date = CURDATE()- INTERVAL 1 DAY - INTERVAL 2 DAY THEN a.clicks ELSE 0 END) AS total_clicks_yesterday,
    SUM(CASE WHEN a.date BETWEEN DATE_SUB(CURDATE()- INTERVAL 1 DAY - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB(CURDATE()- INTERVAL 1 DAY, INTERVAL 1 DAY) THEN a.sales14d ELSE 0 END) AS total_sales14d_30d,
    SUM(CASE WHEN a.date BETWEEN DATE_SUB(CURDATE()- INTERVAL 1 DAY - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURDATE()- INTERVAL 1 DAY, INTERVAL 1 DAY) THEN a.sales14d ELSE 0 END) AS total_sales14d_7d,
    SUM(CASE WHEN a.date = CURDATE()- INTERVAL 1 DAY - INTERVAL 2 DAY THEN a.sales14d ELSE 0 END) AS total_sales14d_yesterday,
    SUM(CASE WHEN a.date BETWEEN DATE_SUB(CURDATE()- INTERVAL 1 DAY - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB(CURDATE()- INTERVAL 1 DAY, INTERVAL 1 DAY) THEN a.cost ELSE 0 END) AS total_cost_30d,
    SUM(CASE WHEN a.date BETWEEN DATE_SUB(CURDATE()- INTERVAL 1 DAY - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURDATE()- INTERVAL 1 DAY, INTERVAL 1 DAY) THEN a.cost ELSE 0 END) AS total_cost_7d,
    SUM(CASE WHEN a.date = CURDATE()- INTERVAL 1 DAY - INTERVAL 2 DAY THEN a.cost ELSE 0 END) AS total_cost_yesterday,
    SUM(CASE WHEN a.date BETWEEN DATE_SUB(CURDATE()- INTERVAL 1 DAY - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB(CURDATE()- INTERVAL 1 DAY, INTERVAL 1 DAY) THEN a.cost ELSE 0 END) / SUM(CASE WHEN a.date BETWEEN DATE_SUB(CURDATE()- INTERVAL 1 DAY - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB(CURDATE()- INTERVAL 1 DAY, INTERVAL 1 DAY) THEN a.sales14d ELSE 0 END) AS ACOS_30d,
    SUM(CASE WHEN a.date BETWEEN DATE_SUB(CURDATE()- INTERVAL 1 DAY - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURDATE()- INTERVAL 1 DAY, INTERVAL 1 DAY) THEN a.cost ELSE 0 END) / SUM(CASE WHEN a.date BETWEEN DATE_SUB(CURDATE()- INTERVAL 1 DAY - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURDATE()- INTERVAL 1 DAY, INTERVAL 1 DAY) THEN a.sales14d ELSE 0 END) AS ACOS_7d,
    SUM(CASE WHEN a.date = CURDATE()- INTERVAL 1 DAY - INTERVAL 2 DAY THEN a.cost ELSE 0 END) / SUM(CASE WHEN a.date = CURDATE()- INTERVAL 1 DAY - INTERVAL 2 DAY THEN a.sales14d ELSE 0 END)  AS ACOS_yesterday,
    SUM(CASE WHEN a.date BETWEEN DATE_SUB(CURDATE()- INTERVAL 1 DAY - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB(CURDATE()- INTERVAL 1 DAY, INTERVAL 1 DAY) THEN a.purchases7d ELSE 0 END) AS ORDER_1m,
    SUM(CASE WHEN a.date BETWEEN DATE_SUB(CURDATE()- INTERVAL 1 DAY - INTERVAL 1 DAY, INTERVAL 6 day) AND DATE_SUB(CURDATE()- INTERVAL 1 DAY, INTERVAL 1 DAY) THEN a.purchases7d ELSE 0 END) AS ORDER_7d

FROM
    amazon_search_term_reports_sp a
JOIN
    amazon_campaigns_list_sp c ON a.campaignId = c.campaignId
JOIN
    (select sspu,campaignId
    from amazon_advertised_product_reports_sp t1
    join
        prod_as_product_base t2 ON t2.sku = t1.advertisedSku
    where market = '{market}'
    group by
        campaignId
    ) b ON c.campaignId = b.campaignId
WHERE
    a.date BETWEEN DATE_SUB(CURDATE()- INTERVAL 1 DAY, INTERVAL 30 DAY) AND CURDATE()- INTERVAL 1 DAY- INTERVAL 1 DAY
    AND a.market = '{market}'
    AND b.sspu = '{sspu}'
    AND c.state = 'ENABLED'
    AND c.targetingType LIKE '%AUT%'
    AND NOT (a.searchTerm LIKE 'b0%' AND LENGTH(a.searchTerm) = 10) -- 添加排除条件
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
                    """.format(market)
            df = pd.read_sql(query, con=conn)
            if df.empty:
                print("No search_term")
                return None
            else:
                # Define criteria for filtering
                # Criteria 1: Sales in the last 7 days > 0 and ACOS in the last 7 days <= 0.24
                criteria1 = (df['total_sales14d_7d'] > 0) & (df['ACOS_7d'] <= 0.24)

                # Criteria 2: Orders in the last 30 days >= 2 and average ACOS in the last 30 days <= 0.24
                criteria2 = (df['ORDER_1m'] >= 2) & (df['ACOS_30d'] <= 0.24)

                # Apply filters
                filtered_df = df[criteria1 | criteria2]

                if filtered_df.empty:
                    print("No search_term matching the criteria")
                    return None
                else:
                    print("select campaignId success")
                    return filtered_df['searchTerm'].tolist()

        except Exception as e:
            print(f"Error occurred when selecting campaign search term: {e}")

    def select_sp_campaign_search_term_by_parent_asin(self, market, parent_asin,depository):
        try:
            conn = self.conn
            query = f"""
SELECT
    b.parent_asins,
    a.searchTerm,
    a.keyword,
    a.keywordBid,
    a.adGroupName,
    a.adGroupId,
    a.matchType,
    a.campaignName,
    a.campaignId,
    SUM(CASE WHEN a.date BETWEEN DATE_SUB(CURDATE()- INTERVAL 1 DAY - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB(CURDATE()- INTERVAL 1 DAY, INTERVAL 1 DAY) THEN a.clicks ELSE 0 END) AS total_clicks_30d,
    SUM(CASE WHEN a.date BETWEEN DATE_SUB(CURDATE()- INTERVAL 1 DAY - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURDATE()- INTERVAL 1 DAY, INTERVAL 1 DAY) THEN a.clicks ELSE 0 END) AS total_clicks_7d,
    SUM(CASE WHEN a.date = CURDATE()- INTERVAL 1 DAY - INTERVAL 2 DAY THEN a.clicks ELSE 0 END) AS total_clicks_yesterday,
    SUM(CASE WHEN a.date BETWEEN DATE_SUB(CURDATE()- INTERVAL 1 DAY - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB(CURDATE()- INTERVAL 1 DAY, INTERVAL 1 DAY) THEN a.sales14d ELSE 0 END) AS total_sales14d_30d,
    SUM(CASE WHEN a.date BETWEEN DATE_SUB(CURDATE()- INTERVAL 1 DAY - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURDATE()- INTERVAL 1 DAY, INTERVAL 1 DAY) THEN a.sales14d ELSE 0 END) AS total_sales14d_7d,
    SUM(CASE WHEN a.date = CURDATE()- INTERVAL 1 DAY - INTERVAL 2 DAY THEN a.sales14d ELSE 0 END) AS total_sales14d_yesterday,
    SUM(CASE WHEN a.date BETWEEN DATE_SUB(CURDATE()- INTERVAL 1 DAY - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB(CURDATE()- INTERVAL 1 DAY, INTERVAL 1 DAY) THEN a.cost ELSE 0 END) AS total_cost_30d,
    SUM(CASE WHEN a.date BETWEEN DATE_SUB(CURDATE()- INTERVAL 1 DAY - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURDATE()- INTERVAL 1 DAY, INTERVAL 1 DAY) THEN a.cost ELSE 0 END) AS total_cost_7d,
    SUM(CASE WHEN a.date = CURDATE()- INTERVAL 1 DAY - INTERVAL 2 DAY THEN a.cost ELSE 0 END) AS total_cost_yesterday,
    SUM(CASE WHEN a.date BETWEEN DATE_SUB(CURDATE()- INTERVAL 1 DAY - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB(CURDATE()- INTERVAL 1 DAY, INTERVAL 1 DAY) THEN a.cost ELSE 0 END) / SUM(CASE WHEN a.date BETWEEN DATE_SUB(CURDATE()- INTERVAL 1 DAY - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB(CURDATE()- INTERVAL 1 DAY, INTERVAL 1 DAY) THEN a.sales14d ELSE 0 END) AS ACOS_30d,
    SUM(CASE WHEN a.date BETWEEN DATE_SUB(CURDATE()- INTERVAL 1 DAY - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURDATE()- INTERVAL 1 DAY, INTERVAL 1 DAY) THEN a.cost ELSE 0 END) / SUM(CASE WHEN a.date BETWEEN DATE_SUB(CURDATE()- INTERVAL 1 DAY - INTERVAL 1 DAY, INTERVAL 6 DAY) AND DATE_SUB(CURDATE()- INTERVAL 1 DAY, INTERVAL 1 DAY) THEN a.sales14d ELSE 0 END) AS ACOS_7d,
    SUM(CASE WHEN a.date = CURDATE()- INTERVAL 1 DAY - INTERVAL 2 DAY THEN a.cost ELSE 0 END) / SUM(CASE WHEN a.date = CURDATE()- INTERVAL 1 DAY - INTERVAL 2 DAY THEN a.sales14d ELSE 0 END)  AS ACOS_yesterday,
    SUM(CASE WHEN a.date BETWEEN DATE_SUB(CURDATE()- INTERVAL 1 DAY - INTERVAL 1 DAY, INTERVAL 29 day) AND DATE_SUB(CURDATE()- INTERVAL 1 DAY, INTERVAL 1 DAY) THEN a.purchases7d ELSE 0 END) AS ORDER_1m,
    SUM(CASE WHEN a.date BETWEEN DATE_SUB(CURDATE()- INTERVAL 1 DAY - INTERVAL 1 DAY, INTERVAL 6 day) AND DATE_SUB(CURDATE()- INTERVAL 1 DAY, INTERVAL 1 DAY) THEN a.purchases7d ELSE 0 END) AS ORDER_7d

FROM
    amazon_search_term_reports_sp a
JOIN
    (	SELECT
	parent_asins,
	campaignId
FROM
	amazon_advertised_product_reports_sp t1
	JOIN amazon_product_info_extended t2 ON t2.asin = t1.advertisedAsin
WHERE
	t1.market = '{market}'
	AND t2.market = '{depository}'
GROUP BY
	campaignId
HAVING
	TRIM(parent_asins) <> ''
	AND parent_asins IS NOT NULL
    ) b ON a.campaignId = b.campaignId
WHERE
    a.date BETWEEN DATE_SUB(CURDATE()- INTERVAL 1 DAY, INTERVAL 30 DAY) AND CURDATE()- INTERVAL 1 DAY- INTERVAL 1 DAY
    AND a.market = '{market}'
    AND b.parent_asins = '{parent_asin}'
    AND NOT (a.searchTerm LIKE 'b0%' AND LENGTH(a.searchTerm) = 10) -- 添加排除条件
		AND a.matchType IN ('TARGETING_EXPRESSION_PREDEFINED')
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

                    """.format(market)
            df = pd.read_sql(query, con=conn)
            if df.empty:
                print("No search_term")
                return None
            else:
                # Define criteria for filtering
                # Criteria 1: Sales in the last 7 days > 0 and ACOS in the last 7 days <= 0.24
                criteria1 = (df['total_sales14d_7d'] > 0) & (df['ACOS_7d'] <= 0.24)

                # Criteria 2: Orders in the last 30 days >= 2 and average ACOS in the last 30 days <= 0.24
                criteria2 = (df['ORDER_1m'] >= 2) & (df['ACOS_30d'] <= 0.24)

                # Apply filters
                filtered_df = df[criteria1 | criteria2]

                if filtered_df.empty:
                    print("No search_term matching the criteria")
                    return None
                else:
                    print("select campaignId success")
                    return filtered_df['searchTerm'].tolist()

        except Exception as e:
            print(f"Error occurred when selecting campaign search term: {e}")

    def select_sp_delete_keyword(self, market):
        try:
            conn = self.conn
            query = f"""
SELECT
        market,
        campaignId,
        adGroupId,
        keywordId,
        keywordText,
        matchType,
        state,
        bid
FROM
        amazon_keywords_list_sp
WHERE
        market = '{market}'
        AND state = 'PAUSED'
        AND keywordText NOT IN ( '(_targeting_auto_)' )
        AND campaignId IN (
        SELECT DISTINCT
                campaignId
        FROM
                amazon_keywords_list_sp
        WHERE
                market = '{market}'
                AND state IN ( 'ENABLED', 'PAUSED' )
                AND keywordText NOT IN ( '(_targeting_auto_)' )
                AND extendedData_servingStatus NOT IN ( 'CAMPAIGN_PAUSED', 'AD_GROUP_PAUSED' )
        GROUP BY
                campaignId,
                adGroupId
        HAVING
                count( keywordId )> 800
        )
            """
            df = pd.read_sql(query, con=conn)
            if df.empty:
                print("No delete_keyword")
            else:
                print("select sp delete_keyword success")
                return df["keywordId"].tolist()
        except Exception as e:
            print(f"Error occurred when select_sp_delete_keyword: {e}")
# api = DbSpTools('OutdoorMaster')
# #res = api.select_sd_campaign_name("FR",'M06')
# # #res = api.select_sp_product_asin("IT",'FR','B0CHRYCWPG')
# # res = api.select_sp_campaign_name('FR',"DeepBI_0514_M35_ASIN")
# res = api.select_product_sku_by_asin('FR','DE',['800810-EU'])
# #res = api.select_sp_campaign_search_term('US','M100')
# print(res)
#L17
