import pymysql
import pandas as pd
from datetime import datetime
import warnings

# 忽略特定类型的警告
warnings.filterwarnings("ignore", category=UserWarning)

db_info = {'host': '****',
           'user': '****',
           'passwd': '****',
           'port': 3306,
           'db': '****',
           'charset': 'utf8mb4',
           'use_unicode': True, }

def get_timestamp():
    # 获取当前时间
    current_time = datetime.now()
    timestamp = int(current_time.timestamp())
    date_string = current_time.strftime("%Y-%m-%d")
    # 组合日期和时间戳
    date_timestamp_string = f"{date_string}_{timestamp}"
    return date_timestamp_string

class AmazonMysqlRagUitl:

    def __init__(self, db_info):
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

    df1 = None
    df2 = None
    df3 = None
    def get_sp_seles_top70_sku(self, market, startdate, enddate):

        """关键词优化分析：-1-1.4  找出 在 2024.04.01 至 2024.04.14 这段时间内，  美国SP广告中 低于 平均ACOS值（替换为第一问结论） 10%  ，并且 没有在广告定向投放中出现的 用户搜索关键词。 将这些关键词信息生成csv文件，里面记录这些关键词的以下信息，CPC，SKU/ASIN， ACOS, Clicks."""
        try:
            conn = self.conn


            query1 = """
                WITH RankedCost AS (
    SELECT
        advertisedSku,
        SUM(sales14d) AS total_cost
    FROM
        amazon_advertised_product_reports_sp
    WHERE
        market = '{}'
        AND date BETWEEN '{}' AND '{}'
    GROUP BY
        advertisedSku
    HAVING
        SUM(sales14d) > 0 -- 确保cost不为零
),
TotalCost AS (
    SELECT
        SUM(sales14d) AS total_cost
    FROM
        amazon_advertised_product_reports_sp
    WHERE
        market = '{}'
        AND date BETWEEN '{}' AND '{}'
        AND advertisedSku IN (SELECT advertisedSku FROM RankedCost)
),
CumulativeCost AS (
    SELECT
        advertisedSku,
        total_cost,
        SUM(total_cost) OVER (ORDER BY total_cost DESC) AS cumulative_cost,
        RANK() OVER (ORDER BY total_cost DESC) AS ranking
    FROM
        RankedCost
),
TargetCumulative AS (
    SELECT
        total_cost * 0.7 AS target_cumulative
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
    advertisedSku
FROM
    CumulativeCost
CROSS JOIN
    Threshold
WHERE
    ranking <= Threshold.min_ranking
ORDER BY
    total_cost DESC;
                """.format(market, startdate, enddate,market, startdate, enddate)
            global df1
            df1 = pd.read_sql(query1, con=conn)
            # return df
            output_filename = get_timestamp() + '_top70_sku_1_1_1.csv'
            df1.to_csv(output_filename, index=False, encoding='utf-8-sig')
            # return df
            return "查询已完成，请查看文件： " + output_filename

        except Exception as error:
            print("1-1.1Error while inserting data:", error)

    def get_repeat_sp_seles_top70_sku(self, market1,market2, startdate, endate):
        """关键词优化分析：-1-1.4  找出 在 2024.04.01 至 2024.04.14 这段时间内，  美国SP广告中 低于 平均ACOS值（替换为第一问结论） 10%  ，并且 没有在广告定向投放中出现的 用户搜索关键词。 将这些关键词信息生成csv文件，里面记录这些关键词的以下信息，CPC，SKU/ASIN， ACOS, Clicks."""

        try:
            conn = self.conn

            column1_values = df1['advertisedSku'].tolist()
            #sku = df1.loc[0, 'advertisedSku']
            #print(sku)
            query = """
                    SELECT DISTINCT
    fr.advertisedSku
FROM
    amazon_advertised_product_reports_sp AS fr
INNER JOIN
    (
        -- 这里是之前查询的结果
        SELECT
            advertisedSku
        FROM
            amazon_advertised_product_reports_sp
        WHERE
            advertisedSku IN %(column1_values)s  -- 使用 %(column1_values)s 作为参数
    ) AS de -- 这是你之前的查询结果
ON
    fr.advertisedSku = de.advertisedSku
WHERE
    fr.market = '{}'
    AND fr.date BETWEEN '{}' AND '{}'
    AND cost>0
                    """.format( market1, startdate, endate)
            global df2
            df2 = pd.read_sql(query, con=conn, params={'column1_values': column1_values})
            # return df
            output_filename = get_timestamp() + '_repeat_top70_sku_1_1_2.csv'
            df2.to_csv(output_filename, index=False, encoding='utf-8-sig')
            # return df
            return "查询已完成，请查看文件： " + output_filename

        except Exception as error:

            print("1-1.2Error while inserting data:", error)

    def get_repeat_sp_seles_top70_sku_info_market2(self, market1, market2, startdate, endate):
        """关键词优化分析：-1-1.1 在 2024.04.01 至 2024.04.14 这段时间内，美国SP广告的 平均ACOS ，总广告消耗，去重后用户搜索关键词 数量 和 总点击量 分别是多少？"""
        try:
            conn = self.conn

            column1_values = df2['advertisedSku'].tolist()
            # return df
            query = """
                        SELECT
	SUM( sales14d ) AS sales_before,
	SUM( cost ) AS cost_before,
	(
	SUM( cost ) / SUM( sales14d )) AS ACOS_before
FROM
	amazon_advertised_product_reports_sp
WHERE
	market = '{}'
	AND date BETWEEN '{}'
	AND '{}'
	AND advertisedSku IN(
	 -- 这里是之前查询的结果
                    SELECT
                        advertisedSku
                    FROM
                        amazon_advertised_product_reports_sp
                    WHERE
                        advertisedSku IN %(column1_values)s  -- 使用 %(column1_values)s 作为参数
	)
                        """.format(market2, startdate, endate)
            df = pd.read_sql(query, con=conn, params={'column1_values': column1_values})
            acos = df.loc[0,'ACOS_before']
            total_cost = df.loc[0, 'cost_before']
            sales = df.loc[0, 'sales_before']
            result_str = "总销售额(sales):{} - 总广告消耗(Cost):{} -平均ACOS:{} ".format(sales,total_cost,acos)
            return result_str

            print("1.1.3 Data inserted successfully!")

        except Exception as error:
            print("1.1.3 Error while inserting data:", error)

    repeat_sp_seles_sku_info_market1_acos = None
    def get_repeat_sp_seles_top70_sku_info_market1(self, market1, market2, startdate, endate):
        """关键词优化分析：-1-1.1 在 2024.04.01 至 2024.04.14 这段时间内，美国SP广告的 平均ACOS ，总广告消耗，去重后用户搜索关键词 数量 和 总点击量 分别是多少？"""
        try:
            conn = self.conn

            column1_values = df2['advertisedSku'].tolist()
            # return df
            query = """
                        SELECT
    SUM( sales14d ) AS sales_before,
    SUM( cost ) AS cost_before,
    (
    SUM( cost ) / SUM( sales14d )) AS ACOS_before
FROM
    amazon_advertised_product_reports_sp
WHERE
    market = '{}'
    AND date BETWEEN '{}'
    AND '{}'
    AND advertisedSku IN(
     -- 这里是之前查询的结果
                    SELECT
                        advertisedSku
                    FROM
                        amazon_advertised_product_reports_sp
                    WHERE
                        advertisedSku IN %(column1_values)s  -- 使用 %(column1_values)s 作为参数
    )
                        """.format(market1, startdate, endate)
            df = pd.read_sql(query, con=conn, params={'column1_values': column1_values})
            global repeat_sp_seles_sku_info_market1_acos
            repeat_sp_seles_sku_info_market1_acos = df.loc[0, 'ACOS_before']
            total_cost = df.loc[0, 'cost_before']
            sales = df.loc[0, 'sales_before']
            result_str = "总销售额(sales):{} - 总广告消耗(Cost):{} -平均ACOS:{} ".format(sales, total_cost, repeat_sp_seles_sku_info_market1_acos)
            return result_str

            print("1.1.4 Data inserted successfully!")

        except Exception as error:
            print("1.1.4 Error while inserting data:", error)

    market1_acos = None
    def predict_repeat_market1_sp_seles_sku_info1(self, market1, market2, startdate, endate):
        """关键词优化分析：-1-1.4  找出 在 2024.04.01 至 2024.04.14 这段时间内，  美国SP广告中 低于 平均ACOS值（替换为第一问结论） 10%  ，并且 没有在广告定向投放中出现的 用户搜索关键词。 将这些关键词信息生成csv文件，里面记录这些关键词的以下信息，CPC，SKU/ASIN， ACOS, Clicks."""
        try:
            conn = self.conn

            query1 = """
            SELECT
	(SUM( cost ) / SUM( sales14d )) AS ACOS_before
FROM
	amazon_advertised_product_reports_sp
WHERE
	market = '{}'
	AND date BETWEEN '{}'
	AND '{}'
            """.format(market1, startdate, endate)

            df3 = pd.read_sql(query1, con=conn)
            global market1_acos
            market1_acos = df3.loc[0, 'ACOS_before']

            column1_values = df2['advertisedSku'].tolist()
            # sku = df1.loc[0, 'advertisedSku']
            # print(sku)
            query = """
                              SELECT
    sum( clicks_old ) AS total_clicks_old,
    sum( cost_old ) AS total_cost_old,
    sum( cost_old )/ sum( clicks_old ) AS total_cpc_old,
    SUM( sales14d_old ) AS total_sales_old,
    sum( clicks_new ) AS total_clicks_new,
    SUM( cost_new ) AS total_cost_new,
    SUM( cost_new )/ sum( clicks_new ) AS total_cpc_new,
    SUM( sales_new ) AS total_sales_new
FROM
    (
    SELECT
        Id,
        sum( clicks ) AS clicks_old,
        SUM( cost ) AS cost_old,
        SUM( sales14d ) AS sales14d_old,
        sum( clicks )* 1.15 AS clicks_new,
        SUM( cost )* 1.3225 AS cost_new,
        SUM( sales14d )* 1.3225 AS sales_new
    FROM
        amazon_advertised_product_reports_sp
    WHERE
        market = '{}'
        AND date BETWEEN '{}'
        AND '{}'
        AND advertisedSku  IN %(column1_values)s
    GROUP BY
        Id
    HAVING
        SUM( cost ) / SUM( sales14d ) < ( {} * 0.7 ) AND SUM( clicks ) >= 3 UNION ALL
    SELECT
        Id,
        sum( clicks ) AS clicks_old,
        SUM( cost ) AS cost_old,
        SUM( sales14d ) AS sales14d_old,
        sum( clicks ) AS clicks_new,
        SUM( cost ) AS cost_new,
        SUM( sales14d ) AS sales_new
    FROM
        amazon_advertised_product_reports_sp
    WHERE
        market = '{}'
        AND date BETWEEN '{}'
        AND '{}'
        AND advertisedSku IN %(column1_values)s
    GROUP BY
        Id
    HAVING
        SUM( cost ) / SUM( sales14d ) < ( {} * 0.7 )
    AND SUM( clicks ) < 3
    ) AS subquery;
                                """.format(market1, startdate, endate, market1_acos,
                                           market1, startdate, endate, market1_acos)
            df = pd.read_sql(query, con=conn, params={'column1_values': column1_values})

            total_cost_old = df.loc[0, 'total_cost_old']
            total_sales_old = df.loc[0, 'total_sales_old']
            total_cost_new = df.loc[0, 'total_cost_new']
            total_sales_new = df.loc[0, 'total_sales_new']

            if total_cost_old is None:
                total_cost_old = 0
                total_sales_old = 0
                total_cost_new = 0
                total_sales_new = 0

            return [{"total_cost_old": total_cost_old}, {"total_sales_old": total_sales_old},
                    {"total_cost_new": total_cost_new}, {"total_sales_new": total_sales_new}]
        except Exception as error:

            print("1-2.1Error while inserting data:", error)

    def predict_repeat_market1_sp_seles_sku_info2(self, market1, market2, startdate, endate):
        """关键词优化分析：-1-1.4  找出 在 2024.04.01 至 2024.04.14 这段时间内，  美国SP广告中 低于 平均ACOS值（替换为第一问结论） 10%  ，并且 没有在广告定向投放中出现的 用户搜索关键词。 将这些关键词信息生成csv文件，里面记录这些关键词的以下信息，CPC，SKU/ASIN， ACOS, Clicks."""
        try:
            conn = self.conn
            column1_values = df2['advertisedSku'].tolist()
            # sku = df1.loc[0, 'advertisedSku']
            # print(sku)
            query = """
                              SELECT
    sum( clicks_old ) AS total_clicks_old,
    sum( cost_old ) AS total_cost_old,
    sum( cost_old )/ sum( clicks_old ) AS total_cpc_old,
    SUM( sales14d_old ) AS total_sales_old,
    sum( clicks_new ) AS total_clicks_new,
    SUM( cost_new ) AS total_cost_new,
    SUM( cost_new )/ sum( clicks_new ) AS total_cpc_new,
    SUM( sales_new ) AS total_sales_new
FROM
    (
    SELECT
        Id,
        sum( clicks ) AS clicks_old,
        SUM( cost ) AS cost_old,
        SUM( sales14d ) AS sales14d_old,
        sum( clicks )* 1.08 AS clicks_new,
        SUM( cost )* 1.08 * 1.08 AS cost_new,
        SUM( sales14d )* 1.08 * 1.08 AS sales_new
    FROM
        amazon_advertised_product_reports_sp
    WHERE
        market = '{}'
        AND date BETWEEN '{}'
        AND '{}'
        AND advertisedSku  IN %(column1_values)s
    GROUP BY
        Id
    HAVING
         ({} * 0.7) < SUM(cost) / SUM(sales14d) AND SUM(cost) / SUM(sales14d) < ({} * 0.8)   AND  SUM(clicks) >= 3 UNION ALL
    SELECT
        Id,
        sum( clicks ) AS clicks_old,
        SUM( cost ) AS cost_old,
        SUM( sales14d ) AS sales14d_old,
        sum( clicks ) AS clicks_new,
        SUM( cost ) AS cost_new,
        SUM( sales14d ) AS sales_new
    FROM
        amazon_advertised_product_reports_sp
    WHERE
        market = '{}'
        AND date BETWEEN '{}'
        AND '{}'
        AND advertisedSku IN %(column1_values)s
    GROUP BY
        Id
    HAVING
        ({} * 0.7) < SUM(cost) / SUM(sales14d) AND SUM(cost) / SUM(sales14d) < ({} * 0.8)   AND  SUM(clicks) < 3
    ) AS subquery;
                                """.format(market1, startdate, endate, market1_acos,
                                           market1_acos, market1, startdate, endate,
                                           market1_acos,
                                           market1_acos)
            df = pd.read_sql(query, con=conn, params={'column1_values': column1_values})

            total_cost_old = df.loc[0, 'total_cost_old']
            total_sales_old = df.loc[0, 'total_sales_old']
            total_cost_new = df.loc[0, 'total_cost_new']
            total_sales_new = df.loc[0, 'total_sales_new']

            if total_cost_old is None:
                total_cost_old = 0
                total_sales_old = 0
                total_cost_new = 0
                total_sales_new = 0

            return [{"total_cost_old": total_cost_old}, {"total_sales_old": total_sales_old},
                    {"total_cost_new": total_cost_new}, {"total_sales_new": total_sales_new}]
        except Exception as error:

            print("1-2.2Error while inserting data:", error)

    def predict_repeat_market1_sp_seles_sku_info3(self, market1, market2, startdate, endate):
        """关键词优化分析：-1-1.4  找出 在 2024.04.01 至 2024.04.14 这段时间内，  美国SP广告中 低于 平均ACOS值（替换为第一问结论） 10%  ，并且 没有在广告定向投放中出现的 用户搜索关键词。 将这些关键词信息生成csv文件，里面记录这些关键词的以下信息，CPC，SKU/ASIN， ACOS, Clicks."""
        try:
            conn = self.conn
            column1_values = df2['advertisedSku'].tolist()
            # sku = df1.loc[0, 'advertisedSku']
            # print(sku)
            query = """
                              SELECT
    sum( clicks_old ) AS total_clicks_old,
    sum( cost_old ) AS total_cost_old,
    sum( cost_old )/ sum( clicks_old ) AS total_cpc_old,
    SUM( sales14d_old ) AS total_sales_old,
    sum( clicks_new ) AS total_clicks_new,
    SUM( cost_new ) AS total_cost_new,
    SUM( cost_new )/ sum( clicks_new ) AS total_cpc_new,
    SUM( sales_new ) AS total_sales_new
FROM
    (
    SELECT
        Id,
        sum( clicks ) AS clicks_old,
        SUM( cost ) AS cost_old,
        SUM( sales14d ) AS sales14d_old,
        sum( clicks )* 1.04 AS clicks_new,
        SUM( cost )* 1.04 * 1.04 AS cost_new,
        SUM( sales14d )* 1.04 * 1.04 AS sales_new
    FROM
        amazon_advertised_product_reports_sp
    WHERE
        market = '{}'
        AND date BETWEEN '{}'
        AND '{}'
        AND advertisedSku  IN %(column1_values)s
    GROUP BY
        Id
    HAVING
         ({} * 0.8) < SUM(cost) / SUM(sales14d) AND SUM(cost) / SUM(sales14d) < ({} * 0.9)   AND  SUM(clicks) >= 3 UNION ALL
    SELECT
        Id,
        sum( clicks ) AS clicks_old,
        SUM( cost ) AS cost_old,
        SUM( sales14d ) AS sales14d_old,
        sum( clicks ) AS clicks_new,
        SUM( cost ) AS cost_new,
        SUM( sales14d ) AS sales_new
    FROM
        amazon_advertised_product_reports_sp
    WHERE
        market = '{}'
        AND date BETWEEN '{}'
        AND '{}'
        AND advertisedSku IN %(column1_values)s
    GROUP BY
        Id
    HAVING
        ({} * 0.8) < SUM(cost) / SUM(sales14d) AND SUM(cost) / SUM(sales14d) < ({} * 0.9)   AND  SUM(clicks) < 3
    ) AS subquery;
                                """.format(market1, startdate, endate, market1_acos,
                                           market1_acos, market1, startdate, endate,
                                           market1_acos,
                                           market1_acos)
            df = pd.read_sql(query, con=conn, params={'column1_values': column1_values})

            total_cost_old = df.loc[0, 'total_cost_old']
            total_sales_old = df.loc[0, 'total_sales_old']
            total_cost_new = df.loc[0, 'total_cost_new']
            total_sales_new = df.loc[0, 'total_sales_new']

            if total_cost_old is None:
                total_cost_old = 0
                total_sales_old = 0
                total_cost_new = 0
                total_sales_new = 0

            return [{"total_cost_old": total_cost_old}, {"total_sales_old": total_sales_old},
                    {"total_cost_new": total_cost_new}, {"total_sales_new": total_sales_new}]
        except Exception as error:

            print("1-2.3Error while inserting data:", error)

    def predict_repeat_market1_sp_seles_sku_info4(self, market1, market2, startdate, endate):
        """关键词优化分析：-1-1.4  找出 在 2024.04.01 至 2024.04.14 这段时间内，  美国SP广告中 低于 平均ACOS值（替换为第一问结论） 10%  ，并且 没有在广告定向投放中出现的 用户搜索关键词。 将这些关键词信息生成csv文件，里面记录这些关键词的以下信息，CPC，SKU/ASIN， ACOS, Clicks."""
        try:
            conn = self.conn
            column1_values = df2['advertisedSku'].tolist()
            # sku = df1.loc[0, 'advertisedSku']
            # print(sku)
            query = """
                              SELECT
    sum( clicks_old ) AS total_clicks_old,
    sum( cost_old ) AS total_cost_old,
    sum( cost_old )/ sum( clicks_old ) AS total_cpc_old,
    SUM( sales14d_old ) AS total_sales_old,
    sum( clicks_new ) AS total_clicks_new,
    SUM( cost_new ) AS total_cost_new,
    SUM( cost_new )/ sum( clicks_new ) AS total_cpc_new,
    SUM( sales_new ) AS total_sales_new
FROM
    (
    SELECT
        Id,
        sum( clicks ) AS clicks_old,
        SUM( cost ) AS cost_old,
        SUM( sales14d ) AS sales14d_old,
        sum( clicks )* 0.85 AS clicks_new,
        SUM( cost )* 0.85 * 0.85 AS cost_new,
        SUM( sales14d )* 0.85 * 0.85 AS sales_new
    FROM
        amazon_advertised_product_reports_sp
    WHERE
        market = '{}'
        AND date BETWEEN '{}'
        AND '{}'
        AND advertisedSku  IN %(column1_values)s
    GROUP BY
        Id
    HAVING
         ({} * 1.2) < SUM(cost) / SUM(sales14d) AND SUM(cost) / SUM(sales14d) < ({} * 1.3)   AND  SUM(clicks) >= 3 UNION ALL
    SELECT
        Id,
        sum( clicks ) AS clicks_old,
        SUM( cost ) AS cost_old,
        SUM( sales14d ) AS sales14d_old,
        sum( clicks ) AS clicks_new,
        SUM( cost ) AS cost_new,
        SUM( sales14d ) AS sales_new
    FROM
        amazon_advertised_product_reports_sp
    WHERE
        market = '{}'
        AND date BETWEEN '{}'
        AND '{}'
        AND advertisedSku IN %(column1_values)s
    GROUP BY
        Id
    HAVING
        ({} * 1.2) < SUM(cost) / SUM(sales14d) AND SUM(cost) / SUM(sales14d) < ({} * 1.3)   AND  SUM(clicks) < 3
    ) AS subquery;
                                """.format(market1, startdate, endate, market1_acos,
                                           market1_acos, market1, startdate, endate,
                                           market1_acos,
                                           market1_acos)
            df = pd.read_sql(query, con=conn, params={'column1_values': column1_values})

            total_cost_old = df.loc[0, 'total_cost_old']
            total_sales_old = df.loc[0, 'total_sales_old']
            total_cost_new = df.loc[0, 'total_cost_new']
            total_sales_new = df.loc[0, 'total_sales_new']

            if total_cost_old is None:
                total_cost_old = 0
                total_sales_old = 0
                total_cost_new = 0
                total_sales_new = 0

            return [{"total_cost_old": total_cost_old}, {"total_sales_old": total_sales_old},
                    {"total_cost_new": total_cost_new}, {"total_sales_new": total_sales_new}]
        except Exception as error:

            print("1-2.4Error while inserting data:", error)

    def predict_repeat_market1_sp_seles_sku_info5(self, market1, market2, startdate, endate):
        """关键词优化分析：-1-1.4  找出 在 2024.04.01 至 2024.04.14 这段时间内，  美国SP广告中 低于 平均ACOS值（替换为第一问结论） 10%  ，并且 没有在广告定向投放中出现的 用户搜索关键词。 将这些关键词信息生成csv文件，里面记录这些关键词的以下信息，CPC，SKU/ASIN， ACOS, Clicks."""
        try:
            conn = self.conn
            column1_values = df2['advertisedSku'].tolist()
            # sku = df1.loc[0, 'advertisedSku']
            # print(sku)
            query = """
                              SELECT
    sum( clicks_old ) AS total_clicks_old,
    sum( cost_old ) AS total_cost_old,
    sum( cost_old )/ sum( clicks_old ) AS total_cpc_old,
    SUM( sales14d_old ) AS total_sales_old,
    sum( clicks_new ) AS total_clicks_new,
    SUM( cost_new ) AS total_cost_new,
    SUM( cost_new )/ sum( clicks_new ) AS total_cpc_new,
    SUM( sales_new ) AS total_sales_new
FROM
    (
    SELECT
        Id,
        sum( clicks ) AS clicks_old,
        SUM( cost ) AS cost_old,
        SUM( sales14d ) AS sales14d_old,
        sum( clicks )* 0.92 AS clicks_new,
        SUM( cost )* 0.92 * 0.92 AS cost_new,
        SUM( sales14d )* 0.92 * 0.92 AS sales_new
    FROM
        amazon_advertised_product_reports_sp
    WHERE
        market = '{}'
        AND date BETWEEN '{}'
        AND '{}'
        AND advertisedSku  IN %(column1_values)s
    GROUP BY
        Id
    HAVING
         ({} * 1.1) < SUM(cost) / SUM(sales14d) AND SUM(cost) / SUM(sales14d) < ({} * 1.2)   AND  SUM(clicks) >= 3 UNION ALL
    SELECT
        Id,
        sum( clicks ) AS clicks_old,
        SUM( cost ) AS cost_old,
        SUM( sales14d ) AS sales14d_old,
        sum( clicks ) AS clicks_new,
        SUM( cost ) AS cost_new,
        SUM( sales14d ) AS sales_new
    FROM
        amazon_advertised_product_reports_sp
    WHERE
        market = '{}'
        AND date BETWEEN '{}'
        AND '{}'
        AND advertisedSku IN %(column1_values)s
    GROUP BY
        Id
    HAVING
        ({} * 1.1) < SUM(cost) / SUM(sales14d) AND SUM(cost) / SUM(sales14d) < ({} * 1.2)   AND  SUM(clicks) < 3
    ) AS subquery;
                                """.format(market1, startdate, endate, market1_acos,
                                           market1_acos, market1, startdate, endate,
                                           market1_acos,
                                           market1_acos)
            df = pd.read_sql(query, con=conn, params={'column1_values': column1_values})

            total_cost_old = df.loc[0, 'total_cost_old']
            total_sales_old = df.loc[0, 'total_sales_old']
            total_cost_new = df.loc[0, 'total_cost_new']
            total_sales_new = df.loc[0, 'total_sales_new']

            if total_cost_old is None:
                total_cost_old = 0
                total_sales_old = 0
                total_cost_new = 0
                total_sales_new = 0

            return [{"total_cost_old": total_cost_old}, {"total_sales_old": total_sales_old},
                    {"total_cost_new": total_cost_new}, {"total_sales_new": total_sales_new}]
        except Exception as error:

            print("1-2.5Error while inserting data:", error)

    def get_repeat_sp_seles_sku_info_market1_1(self, market1, market2, startdate, endate):
        """关键词优化分析：-1-1.1 在 2024.04.01 至 2024.04.14 这段时间内，美国SP广告的 平均ACOS ，总广告消耗，去重后用户搜索关键词 数量 和 总点击量 分别是多少？"""
        try:
            conn = self.conn

            column1_values = df2['advertisedSku'].tolist()
            # return df
            query = """
                            SELECT
        SUM( sales_before ) AS sales_before,
        SUM( cost_before ) AS cost_before
    FROM
    (
    SELECT
        ID,
        SUM(cost) AS cost_before,
        SUM(sales14d) AS sales_before
    FROM
        amazon_advertised_product_reports_sp
    WHERE
        market = '{}'
        AND date BETWEEN '{}' AND '{}'
        AND advertisedSku  IN %(column1_values)s
    GROUP BY
        ID
    HAVING
         ({} * 0.9) < SUM(cost) / SUM(sales14d) AND SUM(cost) / SUM(sales14d) < ({} * 1.1)
         ) AS subquery;
                            """.format(market1, startdate, endate,market1_acos,market1_acos)
            df = pd.read_sql(query, con=conn, params={'column1_values': column1_values})

            total_cost_old = df.loc[0, 'cost_before']
            total_sales_old = df.loc[0, 'sales_before']

            if total_cost_old is None:
                total_cost_old = 0
                total_sales_old = 0

            return [{"total_cost_old": total_cost_old}, {"total_sales_old": total_sales_old}]
            print("3.2.1 Data inserted successfully!")

        except Exception as error:
            print("1.2.6 Error while inserting data:", error)

    def get_repeat_sp_seles_sku_info_market1_2(self, market1, market2, startdate, endate):
        """关键词优化分析：-1-1.1 在 2024.04.01 至 2024.04.14 这段时间内，美国SP广告的 平均ACOS ，总广告消耗，去重后用户搜索关键词 数量 和 总点击量 分别是多少？"""
        try:
            conn = self.conn

            column1_values = df2['advertisedSku'].tolist()
            # return df
            query = """
                            SELECT
        SUM( sales_before ) AS sales_before,
        SUM( cost_before ) AS cost_before
    FROM
    (
    SELECT
        ID,
        SUM(cost) AS cost_before,
        SUM(sales14d) AS sales_before
    FROM
        amazon_advertised_product_reports_sp
    WHERE
        market = '{}'
        AND date BETWEEN '{}' AND '{}'
        AND advertisedSku  IN %(column1_values)s
    GROUP BY
        ID
    HAVING
         SUM(sales14d) = 0
         ) AS subquery;
                            """.format(market1, startdate, endate)
            df = pd.read_sql(query, con=conn, params={'column1_values': column1_values})

            total_cost_old = df.loc[0, 'cost_before']
            total_sales_old = df.loc[0, 'sales_before']

            if total_cost_old is None:
                total_cost_old = 0
                total_sales_old = 0

            return [{"total_cost_old": total_cost_old}, {"total_sales_old": total_sales_old}]
            print("3.2.2 Data inserted successfully!")

        except Exception as error:
            print("1.2.7 Error while inserting data:", error)

    def get_repeat_sp_seles_sku_info_market1_3(self, market1, market2, startdate, endate):
        """关键词优化分析：-1-1.1 在 2024.04.01 至 2024.04.14 这段时间内，美国SP广告的 平均ACOS ，总广告消耗，去重后用户搜索关键词 数量 和 总点击量 分别是多少？"""
        try:
            conn = self.conn

            column1_values = df2['advertisedSku'].tolist()
            # return df
            query = """
                            SELECT
        SUM( sales_before ) AS sales_before,
        SUM( cost_before ) AS cost_before
    FROM
    (
    SELECT
        ID,
        SUM(cost) AS cost_before,
        SUM(sales14d) AS sales_before
    FROM
        amazon_advertised_product_reports_sp
    WHERE
        market = '{}'
        AND date BETWEEN '{}' AND '{}'
        AND advertisedSku  IN %(column1_values)s
    GROUP BY
        ID
    HAVING
         ({} * 1.3) < SUM(cost) / SUM(sales14d)
         ) AS subquery;
                            """.format(market1, startdate, endate,market1_acos)
            df = pd.read_sql(query, con=conn, params={'column1_values': column1_values})

            total_cost_old = df.loc[0, 'cost_before']
            total_sales_old = df.loc[0, 'sales_before']

            if total_cost_old is None:
                total_cost_old = 0
                total_sales_old = 0

            return [{"total_cost_old": total_cost_old}, {"total_sales_old": total_sales_old}]
            print("3.2.3 Data inserted successfully!")

        except Exception as error:
            print("1.2.8 Error while inserting data:", error)

    def get_unrepeat_sp_seles_top70_sku(self, market1, market2, startdate, endate):
        """关键词优化分析：-1-1.4  找出 在 2024.04.01 至 2024.04.14 这段时间内，  美国SP广告中 低于 平均ACOS值（替换为第一问结论） 10%  ，并且 没有在广告定向投放中出现的 用户搜索关键词。 将这些关键词信息生成csv文件，里面记录这些关键词的以下信息，CPC，SKU/ASIN， ACOS, Clicks."""
        try:
            conn = self.conn

            df1_not_in_df2 = df1[~df1['advertisedSku'].isin(df2['advertisedSku'])]
            # return df
            output_filename = get_timestamp() + '_unrepeat_top70_sku_2_1_1.csv'
            df1_not_in_df2.to_csv(output_filename, index=False, encoding='utf-8-sig')
            # return df
            return "查询已完成，请查看文件： " + output_filename

        except Exception as error:

            print("2-1.1Error while inserting data:", error)

    def get_unrepeat_sp_seles_top70_sku_info_market2(self, market1, market2, startdate, endate):
        """关键词优化分析：-1-1.1 在 2024.04.01 至 2024.04.14 这段时间内，美国SP广告的 平均ACOS ，总广告消耗，去重后用户搜索关键词 数量 和 总点击量 分别是多少？"""
        try:
            conn = self.conn
            df1_not_in_df2 = df1[~df1['advertisedSku'].isin(df2['advertisedSku'])]
            column1_values = df1_not_in_df2['advertisedSku'].tolist()
            # return df
            query = """
                        SELECT
	SUM( sales14d ) AS sales_before,
	SUM( cost ) AS cost_before,
	(
	SUM( cost ) / SUM( sales14d )) AS ACOS_before
FROM
	amazon_advertised_product_reports_sp
WHERE
	market = '{}'
	AND date BETWEEN '{}'
	AND '{}'
	AND advertisedSku IN(
	 -- 这里是之前查询的结果
                    SELECT
                        advertisedSku
                    FROM
                        amazon_advertised_product_reports_sp
                    WHERE
                        advertisedSku IN %(column1_values)s  -- 使用 %(column1_values)s 作为参数
	)
                        """.format(market2, startdate, endate)
            df = pd.read_sql(query, con=conn, params={'column1_values': column1_values})
            acos = df.loc[0,'ACOS_before']
            total_cost = df.loc[0, 'cost_before']
            sales = df.loc[0, 'sales_before']
            result_str = "总销售额(sales):{} - 总广告消耗(Cost):{} -平均ACOS:{} ".format(sales,total_cost,acos)
            return result_str

            print("2.1.2 Data inserted successfully!")

        except Exception as error:
            print("2.1.2 Error while inserting data:", error)

    def predict_unrepeat_sp_seles_top70_sku_info_market1(self, market1, market2, startdate, endate):
        """关键词优化分析：-1-1.1 在 2024.04.01 至 2024.04.14 这段时间内，美国SP广告的 平均ACOS ，总广告消耗，去重后用户搜索关键词 数量 和 总点击量 分别是多少？"""
        try:
            conn = self.conn
            column1_values = df2['advertisedSku'].tolist()
            # return df
            query1 = """
                                    SELECT
            	SUM( sales14d ) AS sales_before,
            	SUM( cost ) AS cost_before,
            	(
            	SUM( cost ) / SUM( sales14d )) AS ACOS_before
            FROM
            	amazon_advertised_product_reports_sp
            WHERE
            	market = '{}'
            	AND date BETWEEN '{}'
            	AND '{}'
            	AND advertisedSku IN(
            	 -- 这里是之前查询的结果
                                SELECT
                                    advertisedSku
                                FROM
                                    amazon_advertised_product_reports_sp
                                WHERE
                                    advertisedSku IN %(column1_values)s  -- 使用 %(column1_values)s 作为参数
            	)
                                    """.format(market2, startdate, endate)
            df6 = pd.read_sql(query1, con=conn, params={'column1_values': column1_values})
            market2_repeat_total_cost = df6.loc[0, 'cost_before']
            market2_repeat_sales = df6.loc[0, 'sales_before']
            market2_repeat_acos = df6.loc[0, 'ACOS_before']

            query2 = """
                                    SELECT
                SUM( sales14d ) AS sales_before,
                SUM( cost ) AS cost_before,
                (
                SUM( cost ) / SUM( sales14d )) AS ACOS_before
            FROM
                amazon_advertised_product_reports_sp
            WHERE
                market = '{}'
                AND date BETWEEN '{}'
                AND '{}'
                AND advertisedSku IN(
                 -- 这里是之前查询的结果
                                SELECT
                                    advertisedSku
                                FROM
                                    amazon_advertised_product_reports_sp
                                WHERE
                                    advertisedSku IN %(column1_values)s  -- 使用 %(column1_values)s 作为参数
                )
                                    """.format(market1, startdate, endate)
            df4 = pd.read_sql(query2, con=conn, params={'column1_values': column1_values})
            market1_repeat_total_cost = df4.loc[0, 'cost_before']
            market1_repeat_sales = df4.loc[0, 'sales_before']

            df1_not_in_df2 = df1[~df1['advertisedSku'].isin(df2['advertisedSku'])]
            column1_values = df1_not_in_df2['advertisedSku'].tolist()
            # return df
            query3 = """
                        SELECT
	SUM( sales14d ) AS sales_before,
	SUM( cost ) AS cost_before,
	(
	SUM( cost ) / SUM( sales14d )) AS ACOS_before
FROM
	amazon_advertised_product_reports_sp
WHERE
	market = '{}'
	AND date BETWEEN '{}'
	AND '{}'
	AND advertisedSku IN(
	 -- 这里是之前查询的结果
                    SELECT
                        advertisedSku
                    FROM
                        amazon_advertised_product_reports_sp
                    WHERE
                        advertisedSku IN %(column1_values)s  -- 使用 %(column1_values)s 作为参数
	)
                        """.format(market2, startdate, endate)
            df5 = pd.read_sql(query3, con=conn, params={'column1_values': column1_values})
            market2_unrepeat_total_cost = df5.loc[0, 'cost_before']
            market2_unrepeat_sales = df5.loc[0, 'sales_before']

            if  (market2_unrepeat_total_cost*(market1_repeat_total_cost/market2_repeat_total_cost))/(market2_unrepeat_sales*(market1_repeat_sales/market2_repeat_sales)) > market2_repeat_acos:
                market1_unrepeat_total_cost = market2_unrepeat_total_cost*(market1_repeat_sales/market2_repeat_sales)*2
                market1_unrepeat_sales = market2_unrepeat_sales*(market1_repeat_sales/market2_repeat_sales)*2
            else:
                market1_unrepeat_total_cost = market2_unrepeat_total_cost * (market1_repeat_total_cost / market2_repeat_total_cost) * 2
                market1_unrepeat_sales = market2_unrepeat_sales * (market1_repeat_sales / market2_repeat_sales) * 2

            return [{"total_cost_old": 0}, {"total_sales_old": 0},
                    {"total_cost_new": market1_unrepeat_total_cost}, {"total_sales_new": market1_unrepeat_sales}]


            print("2.1.3 Data inserted successfully!")

        except Exception as error:
            print("2.1.3 Error while inserting data:", error)

    def get_unrepeat_market1_sp_seles_sku(self, market1, market2, startdate, endate):
        """关键词优化分析：-1-1.4  找出 在 2024.04.01 至 2024.04.14 这段时间内，  美国SP广告中 低于 平均ACOS值（替换为第一问结论） 10%  ，并且 没有在广告定向投放中出现的 用户搜索关键词。 将这些关键词信息生成csv文件，里面记录这些关键词的以下信息，CPC，SKU/ASIN， ACOS, Clicks."""
        try:
            conn = self.conn
            column1_values = df2['advertisedSku'].tolist()

            # sku = df1.loc[0, 'advertisedSku']
            # print(sku)
            query = """
                               SELECT DISTINCT
    advertisedSku
FROM
                        amazon_advertised_product_reports_sp
                    WHERE
    market = '{}'
    AND date BETWEEN '{}'
    AND '{}'
    AND advertisedSku NOT IN %(column1_values)s
                                """.format(market1, startdate, endate)
            global df3
            df3 = pd.read_sql(query, con=conn, params={'column1_values': column1_values})

            # return df
            output_filename = get_timestamp() + '_unrepeat_sku_3_1_1.csv'
            df3.to_csv(output_filename, index=False, encoding='utf-8-sig')
            # return df
            return "查询已完成，请查看文件： " + output_filename

        except Exception as error:
            column1_values = df2['advertisedSku'].tolist()
            print(column1_values)
            print("3-1.1Error while inserting data:", error)

    unrepeat_sp_seles_sku_info_market1_acos = None
    def get_unrepeat_sp_seles_sku_info_market1(self, market1, market2, startdate, endate):
        """关键词优化分析：-1-1.1 在 2024.04.01 至 2024.04.14 这段时间内，美国SP广告的 平均ACOS ，总广告消耗，去重后用户搜索关键词 数量 和 总点击量 分别是多少？"""
        try:
            conn = self.conn

            column1_values = df3['advertisedSku'].tolist()
            # return df
            query = """
                            SELECT
        SUM( sales14d ) AS sales_before,
        SUM( cost ) AS cost_before,
        (
        SUM( cost ) / SUM( sales14d )) AS ACOS_before
    FROM
        amazon_advertised_product_reports_sp
    WHERE
        market = '{}'
        AND date BETWEEN '{}'
        AND '{}'
        AND advertisedSku IN(
         -- 这里是之前查询的结果
                        SELECT
                            advertisedSku
                        FROM
                            amazon_advertised_product_reports_sp
                        WHERE
                            advertisedSku IN %(column1_values)s  -- 使用 %(column1_values)s 作为参数
        )
                            """.format(market1, startdate, endate)
            df = pd.read_sql(query, con=conn, params={'column1_values': column1_values})
            global unrepeat_sp_seles_sku_info_market1_acos
            unrepeat_sp_seles_sku_info_market1_acos = df.loc[0, 'ACOS_before']
            total_cost = df.loc[0, 'cost_before']
            sales = df.loc[0, 'sales_before']
            result_str = "总销售额(sales):{} - 总广告消耗(Cost):{} -平均ACOS:{} ".format(sales, total_cost,
                                                                                         unrepeat_sp_seles_sku_info_market1_acos)
            return result_str

            print("3.1.2 Data inserted successfully!")

        except Exception as error:
            print("3.1.2 Error while inserting data:", error)

    def predict_unrepeat_market1_sp_seles_sku_info1(self, market1, market2, startdate, endate):
        """关键词优化分析：-1-1.4  找出 在 2024.04.01 至 2024.04.14 这段时间内，  美国SP广告中 低于 平均ACOS值（替换为第一问结论） 10%  ，并且 没有在广告定向投放中出现的 用户搜索关键词。 将这些关键词信息生成csv文件，里面记录这些关键词的以下信息，CPC，SKU/ASIN， ACOS, Clicks."""
        try:
            conn = self.conn
            column1_values = df3['advertisedSku'].tolist()
            # sku = df1.loc[0, 'advertisedSku']
            # print(sku)
            query = """
                              SELECT
    sum( clicks_old ) AS total_clicks_old,
	sum( cost_old ) AS total_cost_old,
	sum( cost_old )/ sum( clicks_old ) AS total_cpc_old,
	SUM( sales14d_old ) AS total_sales_old,
	sum( clicks_new ) AS total_clicks_new,
	SUM( cost_new ) AS total_cost_new,
	SUM( cost_new )/ sum( clicks_new ) AS total_cpc_new,
	SUM( sales_new ) AS total_sales_new
FROM
	(
	SELECT
		Id,
		sum( clicks ) AS clicks_old,
		SUM( cost ) AS cost_old,
		SUM( sales14d ) AS sales14d_old,
		sum( clicks )* 1.15 AS clicks_new,
		SUM( cost )* 1.3225 AS cost_new,
		SUM( sales14d )* 1.3225 AS sales_new
	FROM
		amazon_advertised_product_reports_sp
	WHERE
		market = '{}'
		AND date BETWEEN '{}'
		AND '{}'
		AND advertisedSku  IN %(column1_values)s
	GROUP BY
		Id
	HAVING
		SUM( cost ) / SUM( sales14d ) < ( {} * 0.7 ) AND SUM( clicks ) >= 3 UNION ALL
	SELECT
		Id,
		sum( clicks ) AS clicks_old,
		SUM( cost ) AS cost_old,
		SUM( sales14d ) AS sales14d_old,
		sum( clicks ) AS clicks_new,
		SUM( cost ) AS cost_new,
		SUM( sales14d ) AS sales_new
	FROM
		amazon_advertised_product_reports_sp
	WHERE
		market = '{}'
		AND date BETWEEN '{}'
		AND '{}'
		AND advertisedSku IN %(column1_values)s
	GROUP BY
		Id
	HAVING
		SUM( cost ) / SUM( sales14d ) < ( {} * 0.7 )
	AND SUM( clicks ) < 3
	) AS subquery;
                                """.format(market1, startdate, endate,market1_acos,market1, startdate, endate,market1_acos)
            df = pd.read_sql(query, con=conn, params={'column1_values': column1_values})

            total_cost_old = df.loc[0, 'total_cost_old']
            total_sales_old = df.loc[0, 'total_sales_old']
            total_cost_new = df.loc[0, 'total_cost_new']
            total_sales_new = df.loc[0, 'total_sales_new']

            if total_cost_old is None:
                total_cost_old = 0
                total_sales_old = 0
                total_cost_new = 0
                total_sales_new = 0

            return [{"total_cost_old": total_cost_old}, {"total_sales_old": total_sales_old},{"total_cost_new": total_cost_new}, {"total_sales_new": total_sales_new}]
        except Exception as error:

            print("3-1.3Error while inserting data:", error)

    def predict_unrepeat_market1_sp_seles_sku_info2(self, market1, market2, startdate, endate):
        """关键词优化分析：-1-1.4  找出 在 2024.04.01 至 2024.04.14 这段时间内，  美国SP广告中 低于 平均ACOS值（替换为第一问结论） 10%  ，并且 没有在广告定向投放中出现的 用户搜索关键词。 将这些关键词信息生成csv文件，里面记录这些关键词的以下信息，CPC，SKU/ASIN， ACOS, Clicks."""
        try:
            conn = self.conn
            column1_values = df3['advertisedSku'].tolist()
            # sku = df1.loc[0, 'advertisedSku']
            # print(sku)
            query = """
                              SELECT
    sum( clicks_old ) AS total_clicks_old,
	sum( cost_old ) AS total_cost_old,
	sum( cost_old )/ sum( clicks_old ) AS total_cpc_old,
	SUM( sales14d_old ) AS total_sales_old,
	sum( clicks_new ) AS total_clicks_new,
	SUM( cost_new ) AS total_cost_new,
	SUM( cost_new )/ sum( clicks_new ) AS total_cpc_new,
	SUM( sales_new ) AS total_sales_new
FROM
	(
	SELECT
		Id,
		sum( clicks ) AS clicks_old,
		SUM( cost ) AS cost_old,
		SUM( sales14d ) AS sales14d_old,
		sum( clicks )* 1.08 AS clicks_new,
		SUM( cost )* 1.08 * 1.08 AS cost_new,
		SUM( sales14d )* 1.08 * 1.08 AS sales_new
	FROM
		amazon_advertised_product_reports_sp
	WHERE
		market = '{}'
		AND date BETWEEN '{}'
		AND '{}'
		AND advertisedSku  IN %(column1_values)s
	GROUP BY
		Id
	HAVING
		 ({} * 0.7) < SUM(cost) / SUM(sales14d) AND SUM(cost) / SUM(sales14d) < ({} * 0.8)   AND  SUM(clicks) >= 3 UNION ALL
	SELECT
		Id,
		sum( clicks ) AS clicks_old,
		SUM( cost ) AS cost_old,
		SUM( sales14d ) AS sales14d_old,
		sum( clicks ) AS clicks_new,
		SUM( cost ) AS cost_new,
		SUM( sales14d ) AS sales_new
	FROM
		amazon_advertised_product_reports_sp
	WHERE
		market = '{}'
		AND date BETWEEN '{}'
		AND '{}'
		AND advertisedSku IN %(column1_values)s
	GROUP BY
		Id
	HAVING
		({} * 0.7) < SUM(cost) / SUM(sales14d) AND SUM(cost) / SUM(sales14d) < ({} * 0.8)   AND  SUM(clicks) < 3
	) AS subquery;
                                """.format(market1, startdate, endate,market1_acos,market1_acos,market1, startdate, endate,market1_acos,market1_acos)
            df = pd.read_sql(query, con=conn, params={'column1_values': column1_values})

            total_cost_old = df.loc[0, 'total_cost_old']
            total_sales_old = df.loc[0, 'total_sales_old']
            total_cost_new = df.loc[0, 'total_cost_new']
            total_sales_new = df.loc[0, 'total_sales_new']

            if total_cost_old is None:
                total_cost_old = 0
                total_sales_old = 0
                total_cost_new = 0
                total_sales_new = 0

            return [{"total_cost_old": total_cost_old}, {"total_sales_old": total_sales_old},{"total_cost_new": total_cost_new}, {"total_sales_new": total_sales_new}]
        except Exception as error:

            print("3-1.4Error while inserting data:", error)

    def predict_unrepeat_market1_sp_seles_sku_info3(self, market1, market2, startdate, endate):
        """关键词优化分析：-1-1.4  找出 在 2024.04.01 至 2024.04.14 这段时间内，  美国SP广告中 低于 平均ACOS值（替换为第一问结论） 10%  ，并且 没有在广告定向投放中出现的 用户搜索关键词。 将这些关键词信息生成csv文件，里面记录这些关键词的以下信息，CPC，SKU/ASIN， ACOS, Clicks."""
        try:
            conn = self.conn
            column1_values = df3['advertisedSku'].tolist()
            # sku = df1.loc[0, 'advertisedSku']
            # print(sku)
            query = """
                              SELECT
    sum( clicks_old ) AS total_clicks_old,
	sum( cost_old ) AS total_cost_old,
	sum( cost_old )/ sum( clicks_old ) AS total_cpc_old,
	SUM( sales14d_old ) AS total_sales_old,
	sum( clicks_new ) AS total_clicks_new,
	SUM( cost_new ) AS total_cost_new,
	SUM( cost_new )/ sum( clicks_new ) AS total_cpc_new,
	SUM( sales_new ) AS total_sales_new
FROM
	(
	SELECT
		Id,
		sum( clicks ) AS clicks_old,
		SUM( cost ) AS cost_old,
		SUM( sales14d ) AS sales14d_old,
		sum( clicks )* 1.04 AS clicks_new,
		SUM( cost )* 1.04 * 1.04 AS cost_new,
		SUM( sales14d )* 1.04 * 1.04 AS sales_new
	FROM
		amazon_advertised_product_reports_sp
	WHERE
		market = '{}'
		AND date BETWEEN '{}'
		AND '{}'
		AND advertisedSku  IN %(column1_values)s
	GROUP BY
		Id
	HAVING
		 ({} * 0.8) < SUM(cost) / SUM(sales14d) AND SUM(cost) / SUM(sales14d) < ({} * 0.9)   AND  SUM(clicks) >= 3 UNION ALL
	SELECT
		Id,
		sum( clicks ) AS clicks_old,
		SUM( cost ) AS cost_old,
		SUM( sales14d ) AS sales14d_old,
		sum( clicks ) AS clicks_new,
		SUM( cost ) AS cost_new,
		SUM( sales14d ) AS sales_new
	FROM
		amazon_advertised_product_reports_sp
	WHERE
		market = '{}'
		AND date BETWEEN '{}'
		AND '{}'
		AND advertisedSku IN %(column1_values)s
	GROUP BY
		Id
	HAVING
		({} * 0.8) < SUM(cost) / SUM(sales14d) AND SUM(cost) / SUM(sales14d) < ({} * 0.9)   AND  SUM(clicks) < 3
	) AS subquery;
                                """.format(market1, startdate, endate,market1_acos,market1_acos,market1, startdate, endate,market1_acos,market1_acos)
            df = pd.read_sql(query, con=conn, params={'column1_values': column1_values})

            total_cost_old = df.loc[0, 'total_cost_old']
            total_sales_old = df.loc[0, 'total_sales_old']
            total_cost_new = df.loc[0, 'total_cost_new']
            total_sales_new = df.loc[0, 'total_sales_new']

            if total_cost_old is None:
                total_cost_old = 0
                total_sales_old = 0
                total_cost_new = 0
                total_sales_new = 0

            return [{"total_cost_old": total_cost_old}, {"total_sales_old": total_sales_old},{"total_cost_new": total_cost_new}, {"total_sales_new": total_sales_new}]
        except Exception as error:

            print("3-1.5Error while inserting data:", error)

    def predict_unrepeat_market1_sp_seles_sku_info4(self, market1, market2, startdate, endate):
        """关键词优化分析：-1-1.4  找出 在 2024.04.01 至 2024.04.14 这段时间内，  美国SP广告中 低于 平均ACOS值（替换为第一问结论） 10%  ，并且 没有在广告定向投放中出现的 用户搜索关键词。 将这些关键词信息生成csv文件，里面记录这些关键词的以下信息，CPC，SKU/ASIN， ACOS, Clicks."""
        try:
            conn = self.conn
            column1_values = df3['advertisedSku'].tolist()
            # sku = df1.loc[0, 'advertisedSku']
            # print(sku)
            query = """
                              SELECT
    sum( clicks_old ) AS total_clicks_old,
	sum( cost_old ) AS total_cost_old,
	sum( cost_old )/ sum( clicks_old ) AS total_cpc_old,
	SUM( sales14d_old ) AS total_sales_old,
	sum( clicks_new ) AS total_clicks_new,
	SUM( cost_new ) AS total_cost_new,
	SUM( cost_new )/ sum( clicks_new ) AS total_cpc_new,
	SUM( sales_new ) AS total_sales_new
FROM
	(
	SELECT
		Id,
		sum( clicks ) AS clicks_old,
		SUM( cost ) AS cost_old,
		SUM( sales14d ) AS sales14d_old,
		sum( clicks )* 0.85 AS clicks_new,
		SUM( cost )* 0.85 * 0.85 AS cost_new,
		SUM( sales14d )* 0.85 * 0.85 AS sales_new
	FROM
		amazon_advertised_product_reports_sp
	WHERE
		market = '{}'
		AND date BETWEEN '{}'
		AND '{}'
		AND advertisedSku  IN %(column1_values)s
	GROUP BY
		Id
	HAVING
		 ({} * 1.2) < SUM(cost) / SUM(sales14d) AND SUM(cost) / SUM(sales14d) < ({} * 1.3)   AND  SUM(clicks) >= 3 UNION ALL
	SELECT
		Id,
		sum( clicks ) AS clicks_old,
		SUM( cost ) AS cost_old,
		SUM( sales14d ) AS sales14d_old,
		sum( clicks ) AS clicks_new,
		SUM( cost ) AS cost_new,
		SUM( sales14d ) AS sales_new
	FROM
		amazon_advertised_product_reports_sp
	WHERE
		market = '{}'
		AND date BETWEEN '{}'
		AND '{}'
		AND advertisedSku IN %(column1_values)s
	GROUP BY
		Id
	HAVING
		({} * 1.2) < SUM(cost) / SUM(sales14d) AND SUM(cost) / SUM(sales14d) < ({} * 1.3)   AND  SUM(clicks) < 3
	) AS subquery;
                                """.format(market1, startdate, endate,market1_acos,market1_acos,market1, startdate, endate,market1_acos,market1_acos)
            df = pd.read_sql(query, con=conn, params={'column1_values': column1_values})

            total_cost_old = df.loc[0, 'total_cost_old']
            total_sales_old = df.loc[0, 'total_sales_old']
            total_cost_new = df.loc[0, 'total_cost_new']
            total_sales_new = df.loc[0, 'total_sales_new']

            if total_cost_old is None:
                total_cost_old = 0
                total_sales_old = 0
                total_cost_new = 0
                total_sales_new = 0

            return [{"total_cost_old": total_cost_old}, {"total_sales_old": total_sales_old},{"total_cost_new": total_cost_new}, {"total_sales_new": total_sales_new}]
        except Exception as error:

            print("3-1.6Error while inserting data:", error)

    def predict_unrepeat_market1_sp_seles_sku_info5(self, market1, market2, startdate, endate):
        """关键词优化分析：-1-1.4  找出 在 2024.04.01 至 2024.04.14 这段时间内，  美国SP广告中 低于 平均ACOS值（替换为第一问结论） 10%  ，并且 没有在广告定向投放中出现的 用户搜索关键词。 将这些关键词信息生成csv文件，里面记录这些关键词的以下信息，CPC，SKU/ASIN， ACOS, Clicks."""
        try:
            conn = self.conn
            column1_values = df3['advertisedSku'].tolist()
            # sku = df1.loc[0, 'advertisedSku']
            # print(sku)
            query = """
                              SELECT
    sum( clicks_old ) AS total_clicks_old,
	sum( cost_old ) AS total_cost_old,
	sum( cost_old )/ sum( clicks_old ) AS total_cpc_old,
	SUM( sales14d_old ) AS total_sales_old,
	sum( clicks_new ) AS total_clicks_new,
	SUM( cost_new ) AS total_cost_new,
	SUM( cost_new )/ sum( clicks_new ) AS total_cpc_new,
	SUM( sales_new ) AS total_sales_new
FROM
	(
	SELECT
		Id,
		sum( clicks ) AS clicks_old,
		SUM( cost ) AS cost_old,
		SUM( sales14d ) AS sales14d_old,
		sum( clicks )* 0.92 AS clicks_new,
		SUM( cost )* 0.92 * 0.92 AS cost_new,
		SUM( sales14d )* 0.92 * 0.92 AS sales_new
	FROM
		amazon_advertised_product_reports_sp
	WHERE
		market = '{}'
		AND date BETWEEN '{}'
		AND '{}'
		AND advertisedSku  IN %(column1_values)s
	GROUP BY
		Id
	HAVING
		 ({} * 1.1) < SUM(cost) / SUM(sales14d) AND SUM(cost) / SUM(sales14d) < ({} * 1.2)   AND  SUM(clicks) >= 3 UNION ALL
	SELECT
		Id,
		sum( clicks ) AS clicks_old,
		SUM( cost ) AS cost_old,
		SUM( sales14d ) AS sales14d_old,
		sum( clicks ) AS clicks_new,
		SUM( cost ) AS cost_new,
		SUM( sales14d ) AS sales_new
	FROM
		amazon_advertised_product_reports_sp
	WHERE
		market = '{}'
		AND date BETWEEN '{}'
		AND '{}'
		AND advertisedSku IN %(column1_values)s
	GROUP BY
		Id
	HAVING
		({} * 1.1) < SUM(cost) / SUM(sales14d) AND SUM(cost) / SUM(sales14d) < ({} * 1.2)   AND  SUM(clicks) < 3
	) AS subquery;
                                """.format(market1, startdate, endate,market1_acos,market1_acos,market1, startdate, endate,market1_acos,market1_acos)
            df = pd.read_sql(query, con=conn, params={'column1_values': column1_values})

            total_cost_old = df.loc[0, 'total_cost_old']
            total_sales_old = df.loc[0, 'total_sales_old']
            total_cost_new = df.loc[0, 'total_cost_new']
            total_sales_new = df.loc[0, 'total_sales_new']

            if total_cost_old is None:
                total_cost_old = 0
                total_sales_old = 0
                total_cost_new = 0
                total_sales_new = 0

            return [{"total_cost_old": total_cost_old}, {"total_sales_old": total_sales_old},{"total_cost_new": total_cost_new}, {"total_sales_new": total_sales_new}]
        except Exception as error:

            print("3-1.7Error while inserting data:", error)

    def get_unrepeat_sp_seles_sku_info_market1_1(self, market1, market2, startdate, endate):
        """关键词优化分析：-1-1.1 在 2024.04.01 至 2024.04.14 这段时间内，美国SP广告的 平均ACOS ，总广告消耗，去重后用户搜索关键词 数量 和 总点击量 分别是多少？"""
        try:
            conn = self.conn

            column1_values = df3['advertisedSku'].tolist()
            # return df
            query = """
                            SELECT
        SUM( sales_before ) AS sales_before,
        SUM( cost_before ) AS cost_before
    FROM
    (
    SELECT
        ID,
        SUM(cost) AS cost_before,
        SUM(sales14d) AS sales_before
    FROM
        amazon_advertised_product_reports_sp
    WHERE
        market = '{}'
        AND date BETWEEN '{}' AND '{}'
        AND advertisedSku  IN %(column1_values)s
    GROUP BY
        ID
    HAVING
         ({} * 0.9) < SUM(cost) / SUM(sales14d) AND SUM(cost) / SUM(sales14d) < ({} * 1.1)
         ) AS subquery;
                            """.format(market1, startdate, endate,market1_acos,market1_acos)
            df = pd.read_sql(query, con=conn, params={'column1_values': column1_values})

            total_cost_old = df.loc[0, 'cost_before']
            total_sales_old = df.loc[0, 'sales_before']

            if total_cost_old is None:
                total_cost_old = 0
                total_sales_old = 0


            return [{"total_cost_old": total_cost_old}, {"total_sales_old": total_sales_old}]
            print("3.2.1 Data inserted successfully!")

        except Exception as error:
            print("3.2.1 Error while inserting data:", error)

    def get_unrepeat_sp_seles_sku_info_market1_2(self, market1, market2, startdate, endate):
        """关键词优化分析：-1-1.1 在 2024.04.01 至 2024.04.14 这段时间内，美国SP广告的 平均ACOS ，总广告消耗，去重后用户搜索关键词 数量 和 总点击量 分别是多少？"""
        try:
            conn = self.conn

            column1_values = df3['advertisedSku'].tolist()
            # return df
            query = """
                            SELECT
        SUM( sales_before ) AS sales_before,
        SUM( cost_before ) AS cost_before
    FROM
    (
    SELECT
        ID,
        SUM(cost) AS cost_before,
        SUM(sales14d) AS sales_before
    FROM
        amazon_advertised_product_reports_sp
    WHERE
        market = '{}'
        AND date BETWEEN '{}' AND '{}'
        AND advertisedSku  IN %(column1_values)s
    GROUP BY
        ID
    HAVING
         SUM(sales14d) = 0
         ) AS subquery;
                            """.format(market1, startdate, endate)
            df = pd.read_sql(query, con=conn, params={'column1_values': column1_values})

            total_cost_old = df.loc[0, 'cost_before']
            total_sales_old = df.loc[0, 'sales_before']

            if total_cost_old is None:
                total_cost_old = 0
                total_sales_old = 0


            return [{"total_cost_old": total_cost_old}, {"total_sales_old": total_sales_old}]
            print("3.2.2 Data inserted successfully!")

        except Exception as error:
            print("3.2.2 Error while inserting data:", error)

    def get_unrepeat_sp_seles_sku_info_market1_3(self, market1, market2, startdate, endate):
        """关键词优化分析：-1-1.1 在 2024.04.01 至 2024.04.14 这段时间内，美国SP广告的 平均ACOS ，总广告消耗，去重后用户搜索关键词 数量 和 总点击量 分别是多少？"""
        try:
            conn = self.conn

            column1_values = df3['advertisedSku'].tolist()
            # return df
            query = """
                            SELECT
        SUM( sales_before ) AS sales_before,
        SUM( cost_before ) AS cost_before
    FROM
    (
    SELECT
        ID,
        SUM(cost) AS cost_before,
        SUM(sales14d) AS sales_before
    FROM
        amazon_advertised_product_reports_sp
    WHERE
        market = '{}'
        AND date BETWEEN '{}' AND '{}'
        AND advertisedSku  IN %(column1_values)s
    GROUP BY
        ID
    HAVING
         ({} * 1.3) < SUM(cost) / SUM(sales14d)
         ) AS subquery;
                            """.format(market1, startdate, endate,market1_acos)
            df = pd.read_sql(query, con=conn, params={'column1_values': column1_values})

            total_cost_old = df.loc[0, 'cost_before']
            total_sales_old = df.loc[0, 'sales_before']


            if total_cost_old is None:
                total_cost_old = 0
                total_sales_old = 0

            return [{"total_cost_old": total_cost_old}, {"total_sales_old": total_sales_old}]
            print("3.2.3 Data inserted successfully!")

        except Exception as error:
            print("3.2.3 Error while inserting data:", error)
