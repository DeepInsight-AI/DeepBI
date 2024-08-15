import json
import os
import pymysql
import pandas as pd
from datetime import datetime
import warnings
from ai.backend.util.db.configuration.path import get_config_path

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

    def __init__(self, brand,market):
        self.brand = brand
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


    def get_new_sd_top90_sku(self, market1,market2, startdate, endate):


        try:
            conn = self.conn

            query1 = """
                        WITH RankedCost AS (
            SELECT
                promotedSku,
                SUM(sales) AS total_cost
            FROM
                amazon_advertised_product_reports_sd
            WHERE
                market = '{}'
                AND date BETWEEN '{}' AND '{}'
            GROUP BY
                promotedSku
            HAVING
                SUM(sales) > 0 -- 确保cost不为零
        ),
        TotalCost AS (
            SELECT
                SUM(sales) AS total_cost
            FROM
                amazon_advertised_product_reports_sd
            WHERE
                market = '{}'
                AND date BETWEEN '{}' AND '{}'
                AND promotedSku IN (SELECT promotedSku FROM RankedCost)
        ),
        CumulativeCost AS (
            SELECT
                promotedSku,
                total_cost,
                SUM(total_cost) OVER (ORDER BY total_cost DESC) AS cumulative_cost,
                RANK() OVER (ORDER BY total_cost DESC) AS ranking
            FROM
                RankedCost
        ),
        TargetCumulative AS (
            SELECT
                total_cost * 0.99 AS target_cumulative
            FROM
                TotalCost
        ),
        Threshold AS (
            SELECT
                MIN(ranking) AS min_ranking
            FROM
                CumulativeCost
            WHERE
                cumulative_cost > (SELECT target_cumulative FROM TargetCumulative)
        )
        SELECT
            promotedSku
        FROM
            CumulativeCost
        CROSS JOIN
            Threshold
        WHERE
            ranking <= Threshold.min_ranking
        ORDER BY
            total_cost DESC;
                    """.format(market2, startdate, endate, market2, startdate, endate)
            df1 = pd.read_sql(query1, con=conn)

            column1_values1 = df1['promotedSku'].tolist()

            if self.brand == 'LAPASA':
                sku1 = f"{market1.lower()}sku"
                sku2 = f"{market2.lower()}sku"
                query4 = """
                SELECT DISTINCT {}
    FROM prod_as_product_base
    WHERE {} IN %(column1_values1)s
                """.format(sku1, sku2)
                df4 = pd.read_sql(query4, con=conn, params={'column1_values1': column1_values1})
                column1_values4 = df4[sku1].tolist()
                print(column1_values4)
                query2 = """
                                    SELECT DISTINCT
                    fr.promotedSku
                FROM
                    amazon_advertised_product_reports_sd AS fr
                INNER JOIN
                    (
                        -- 这里是之前查询的结果
                        SELECT
                            promotedSku
                        FROM
                            amazon_advertised_product_reports_sd
                        WHERE
                            promotedSku IN %(column1_values1)s
                    ) AS de -- 这是你之前的查询结果
                ON
                    fr.promotedSku = de.promotedSku
                WHERE
                    fr.market = '{}'
                    AND fr.date BETWEEN '{}' AND '{}'
                    AND cost>0;
                            """.format(market1, startdate, endate)
                df2 = pd.read_sql(query2, con=conn, params={'column1_values1': column1_values4})

                df1_not_in_df2 = df1[~df1['promotedSku'].isin(df2['promotedSku'])]
                column1_values2 = df1_not_in_df2['promotedSku'].tolist()
            else:
                query2 = """
                                                    SELECT DISTINCT
                                    fr.promotedSku
                                FROM
                                    amazon_advertised_product_reports_sd AS fr
                                INNER JOIN
                                    (
                                        -- 这里是之前查询的结果
                                        SELECT
                                            promotedSku
                                        FROM
                                            amazon_advertised_product_reports_sd
                                        WHERE
                                            promotedSku IN %(column1_values1)s
                                    ) AS de -- 这是你之前的查询结果
                                ON
                                    fr.promotedSku = de.promotedSku
                                WHERE
                                    fr.market = '{}'
                                    AND fr.date BETWEEN '{}' AND '{}'
                                    AND cost>0;
                                            """.format(market1, startdate, endate)
                df2 = pd.read_sql(query2, con=conn, params={'column1_values1': column1_values1})

                df1_not_in_df2 = df1[~df1['promotedSku'].isin(df2['promotedSku'])]
                column1_values2 = df1_not_in_df2['promotedSku'].tolist()



            query3 = """
                    SELECT
	market,
	sum( sales ),
	sum( cost ),
	sum( cost ) / sum( sales ) AS avg_acos,
	campaignId,
	campaignName,
	adGroupId,
	adGroupName,
	promotedSku,
	CASE
	    WHEN campaignName LIKE 'DeepBI_0509_%%' THEN CONCAT('DeepBI_0507_', SUBSTRING(campaignName, 13))
        WHEN campaignName LIKE 'DeepBI_0507_%%' THEN campaignName
        ELSE CONCAT('DeepBI_0507_', campaignName)
    END AS new_campaignName,
    CASE
        WHEN adGroupName LIKE 'DeepBI_0507_%%' THEN adGroupName
        ELSE CONCAT('DeepBI_0507_', adGroupName)
    END AS new_adGroupName
FROM
	amazon_advertised_product_reports_sd
WHERE
	( date BETWEEN '{}' AND '{}' )
	AND market = '{}'
	AND cost > 0
	AND promotedSku IN %(column1_values2)s
	AND campaignName NOT LIKE '%%0511%%' AND campaignName NOT LIKE '%%0614%%' AND campaignName NOT LIKE '%%0615%%' AND campaignName NOT LIKE '%%0510%%'
GROUP BY
	campaignId,
	campaignName,
	adGroupId,
	adGroupName,
	promotedSku
ORDER BY
	sum( sales ) DESC
                        """.format( startdate, endate, market2)

            df3 = pd.read_sql(query3, con=conn, params={'column1_values2': column1_values2})
            output_filename = f'{market1}_{market2}_{startdate}_{endate}_sd_sku_new.csv'
            df3.to_csv(output_filename, index=False, encoding='utf-8-sig')
            # return df
            return output_filename

        except Exception as error:
            print("1-1.1Error while inserting data:", error)

# market1 = 'SE'
# market2 = 'DE'
# startdate = '2024-06-27'
# enddate = '2024-07-04'
#
# # 实例化AmazonMysqlRagUitl类
# util = AmazonMysqlRagUitl('OutdoorMaster')
#
# # 调用方法进行测试
# result = util.get_new_sd_top90_sku(market1, market2, startdate, enddate)
#
# # 打印结果
# print(result)
