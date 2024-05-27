import pymysql
import pandas as pd
from datetime import datetime
import warnings

# 忽略特定类型的警告
warnings.filterwarnings("ignore", category=UserWarning)

db_info = {'host': '192.168.5.114', 'user': 'test_deepdata', 'passwd': 'test123!@#', 'port': 3308,
               'db': 'amazon_ads',
               'charset': 'utf8mb4', 'use_unicode': True, }

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


    def get_new_sp_top70_sku(self, market1,market2, startdate, endate):


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
            advertisedSku
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

            column1_values1 = df1['advertisedSku'].tolist()


            query2 = """
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
                        advertisedSku IN %(column1_values1)s
                ) AS de -- 这是你之前的查询结果
            ON
                fr.advertisedSku = de.advertisedSku
            WHERE
                fr.market = '{}'
                AND fr.date BETWEEN '{}' AND '{}'
                AND cost>0;
                        """.format(market1, startdate, endate)
            df2 = pd.read_sql(query2, con=conn, params={'column1_values1': column1_values1})

            df1_not_in_df2 = df1[~df1['advertisedSku'].isin(df2['advertisedSku'])]
            column1_values2 = df1_not_in_df2['advertisedSku'].tolist()



            query3 = """
                    SELECT
            market,
            sum(sales1d),
            sum(cost),
            sum(cost) / sum(sales1d) as avg_acos,
            campaignId,
            campaignName,
            adGroupId,
            adGroupName,
            advertisedSku,
            CONCAT('DeepBI_0502_', campaignName) as new_campaignName,
            CONCAT('DeepBI_0502_', campaignName) as new_adGroupName

            FROM
            amazon_advertised_product_reports_sp
            WHERE
            date BETWEEN '{}' and '{}'
            and market = '{}'
            and sales1d > 0
            and advertisedSku in %(column1_values2)s
            and campaignId = '194689133640064'
            GROUP BY
            campaignId,
            campaignName,
            adGroupId,
            adGroupName,
            advertisedSku
            ORDER BY sum(sales1d) desc
                        """.format( startdate, endate, market2)

            df3 = pd.read_sql(query3, con=conn, params={'column1_values2': column1_values2})
            #df3['campaignId'] = df3['campaignId'].astype(str)
            print(df3.dtypes)
            pd.options.display.float_format = '{:.0f}'.format
            output_filename = f'{market1}_{market2}_{startdate}_{enddate}_sp_sku_new.csv'
            df3.to_csv(output_filename, index=False, encoding='utf-8-sig')
            # return df
            return "查询已完成，请查看文件： " + output_filename

        except Exception as error:
            print("1-1.1Error while inserting data:", error)

market1 = 'SE'
market2 = 'FR'
startdate = '2024-04-21'
enddate = '2024-05-22'

# 实例化AmazonMysqlRagUitl类
util = AmazonMysqlRagUitl(db_info)

# 调用方法进行测试
result = util.get_new_sp_top70_sku(market1, market2, startdate, enddate)

# 打印结果
print(result)
