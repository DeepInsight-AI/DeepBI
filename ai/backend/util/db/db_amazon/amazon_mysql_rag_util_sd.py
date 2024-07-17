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

class AmazonMysqlSDRagUitl:

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


    def get_sd_product_111(self, market, startdate , enddate):
        """商品优化分析：-1-1.1 在 2024.04.01 至 2024.04.14 这段时间内，美国SD广告的 平均ACOS ，总广告消耗，sku广告数量 和 总点击量 分别是多少？"""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            query = """
            SELECT
            SUM(cost)/SUM(sales) as ACOS,
            SUM(cost) as total_cost, SUM(sales) as total_sales, COUNT(DISTINCT promotedSku) as sku_ad_count, SUM(clicks) as total_clicks
            FROM amazon_advertised_product_reports_sd
            WHERE market = '{}' AND date BETWEEN '{}' AND '{}'
            """.format(market, startdate, enddate)
            df = pd.read_sql(query, con=conn)
            # return df
            acos=df.loc[0,'ACOS']
            total_cost = df.loc[0, 'total_cost']
            sku_ad_count = df.loc[0, 'sku_ad_count']
            total_clicks  = df.loc[0, 'total_clicks']
            result_str = "平均ACOS:{} - 总广告消耗(Cost):{}  不同SKU的广告数量:{}个 - 总点击量(Clicks):{}次".format(acos,total_cost,sku_ad_count,total_clicks)
            return result_str

            print("1.1.1 Data inserted successfully!")

        except Exception as error:
            print("1.1.1 Error while inserting data:", error)


    def get_sd_product_112(self, market, startdate, enddate):
        """关键词优化分析：-1-1.2 在 2024.04.01 至 2024.04.14 这段时间内，  美国SP广告中 低于 平均ACOS值（替换为第一问结论） 10% 的 去重后用户搜索关键词 数量 和 总点击量 是多少"""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            # ACOS求值放到sql中
            # 增加判断逻辑 如果ACOS>0.2 则以原ACOS为准，否则以0.2为基准
            query1 = """
                        SELECT
                        SUM(cost)/SUM(sales7d) as ACOS,
                        SUM(cost) AS total_cost,
                        COUNT(DISTINCT searchTerm) AS unique_search_terms,
                        SUM(clicks) AS total_clicks
                        FROM amazon_search_term_reports_sp WHERE market = '{}' AND date BETWEEN '{}' AND '{}'
                        """.format(market, startdate, enddate)
            df1 = pd.read_sql(query1, con=conn)
            # return df
            acos = df1.loc[0, 'ACOS']
            avgacos = acos if acos>0.2 else 0.2
            # print(avgacos)

            query = """
            SELECT
    promotedSku,
    promotedAsin,
    SUM(cost) AS cost,
    SUM(sales) AS sales,
    SUM(clicks) AS clicks,
    adGroupName,
    adGroupId,
    campaignId,
    campaignName,
    (SUM(cost) / SUM(sales)) AS ACOS
FROM
    amazon_advertised_product_reports_sd
WHERE
    date BETWEEN '{}' AND '{}'
    AND market = '{}'
GROUP BY
    promotedSku,
    promotedAsin,
    adGroupName,
    adGroupId,
    campaignId,
    campaignName
HAVING
    (SUM(cost) / SUM(sales)) < (0.2 * 0.9)
            """.format( startdate, enddate ,market)
            df = pd.read_sql(query, con=conn)
            print(df)
            # 保存到CSV文件中
            output_filename = get_timestamp() + '_sd_product_1_2.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            s1 = f"优质广告数量：{df[['promotedSku', 'promotedAsin','adGroupName','campaignId','campaignName']].drop_duplicates().shape[0]}"
            acos_pro='针对SD广告筛选，ACOS统一取值0.2进行计算'

            print("1.1.2 Data inserted successfully!")
            print("[ACOS:{}][{}]查询已完成，请查看文件： ".format(acos_pro, s1) + output_filename)
            return "[ACOS:{}][{}]查询已完成，请查看文件： ".format(acos_pro, s1) + output_filename

        except Exception as error:
            print("1-1.2Error while inserting data:", error)


    def get_sd_product_113(self, market, startdate, enddate):
        """关键词优化分析：-1-1.3  找出在 2024.04.01 至 2024.04.14 这段时间内，  美国SD广告中 低于 20% 30% 以上的 sku广告。将这些信息生成csv文件，里面记录这些广告的以下信息，CPC，SKU/ASIN， ACOS,  Clicks，adgroupid."""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            # ACOS求值放到sql中
            # 增加判断逻辑 如果ACOS>0.2 则以原ACOS为准，否则以0.2为基准
            query1 = """
                                SELECT
                                SUM(cost)/SUM(sales7d) as ACOS,
                                SUM(cost) AS total_cost,
                                COUNT(DISTINCT searchTerm) AS unique_search_terms,
                                SUM(clicks) AS total_clicks
                                FROM amazon_search_term_reports_sp WHERE market = '{}' AND date BETWEEN '{}' AND '{}'
                                """.format(market, startdate, enddate)
            df1 = pd.read_sql(query1, con=conn)
            # return df
            acos = df1.loc[0, 'ACOS']
            avgacos = acos if acos > 0.2 else 0.2
            # print(avgacos)

            query = """
                    SELECT
            promotedSku,
            promotedAsin,
            SUM(cost) AS cost,
            SUM(sales) AS sales,
            SUM(clicks) AS clicks,
            adGroupName,
            adGroupId,
            campaignId,
    campaignName,
            (SUM(cost) / SUM(sales)) AS ACOS
        FROM
            amazon_advertised_product_reports_sd
        WHERE
            date BETWEEN '{}' AND '{}'
            AND market = '{}'
        GROUP BY
            promotedSku,
            promotedAsin,
            adGroupName,
            adGroupId,
            campaignId,
    campaignName
        HAVING
            (SUM(cost) / SUM(sales)) < (0.2 * 0.7)
                    """.format(startdate, enddate, market)
            df = pd.read_sql(query, con=conn)
            print(df)
            # 保存到CSV文件中
            output_filename = get_timestamp() + '_sd_product_1_3.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            s1 = f"优质广告数量：{df[['promotedSku', 'promotedAsin', 'adGroupName','campaignId','campaignName']].drop_duplicates().shape[0]}"
            acos_pro = '针对SD广告筛选，ACOS统一取值0.2进行计算'

            print("1.1.3 Data inserted successfully!")
            print("[ACOS:{}][{}]查询已完成，请查看文件： ".format(acos_pro, s1) + output_filename)
            return "[ACOS:{}][{}]查询已完成，请查看文件： ".format(acos_pro, s1) + output_filename

        except Exception as error:
            print("1-1.3Error while inserting data:", error)


    def get_sd_product_114(self, market, startdate, enddate):
        """关键词优化分析：-1-1.4  找出在 2024.04.01 至 2024.04.14 这段时间内，  美国SD广告中 低于 20% 20% - 30% 的 sku广告。将这些信息生成csv文件，里面记录这些广告的以下信息，CPC，SKU/ASIN， ACOS, Clicks，adgroupid"""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            # ACOS求值放到sql中
            # 增加判断逻辑 如果ACOS>0.2 则以原ACOS为准，否则以0.2为基准
            query1 = """
                                SELECT
                                SUM(cost)/SUM(sales7d) as ACOS,
                                SUM(cost) AS total_cost,
                                COUNT(DISTINCT searchTerm) AS unique_search_terms,
                                SUM(clicks) AS total_clicks
                                FROM amazon_search_term_reports_sp WHERE market = '{}' AND date BETWEEN '{}' AND '{}'
                                """.format(market, startdate, enddate)
            df1 = pd.read_sql(query1, con=conn)
            # return df
            acos = df1.loc[0, 'ACOS']
            avgacos = acos if acos > 0.2 else 0.2
            # print(avgacos)

            query = """
                    SELECT
            promotedSku,
            promotedAsin,
            SUM(cost) AS cost,
            SUM(sales) AS sales,
            SUM(clicks) AS clicks,
            adGroupName,
            adGroupId,
            campaignId,
    campaignName,
            (SUM(cost) / SUM(sales)) AS ACOS
        FROM
            amazon_advertised_product_reports_sd
        WHERE
            date BETWEEN '{}' AND '{}'
            AND market = '{}'
        GROUP BY
            promotedSku,
            promotedAsin,
            adGroupName,
            adGroupId,
            campaignId,
    campaignName
        HAVING
            (SUM(cost) / SUM(sales)) > (0.2 * 0.7) and (SUM(cost) / SUM(sales)) < (0.2 * 0.8)
                    """.format(startdate, enddate, market)
            df = pd.read_sql(query, con=conn)
            print(df)
            # 保存到CSV文件中
            output_filename = get_timestamp() + '_sd_product_1_4.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            s1 = f"优质广告数量：{df[['promotedSku', 'promotedAsin', 'adGroupName','campaignId','campaignName']].drop_duplicates().shape[0]}"
            acos_pro = '针对SD广告筛选，ACOS统一取值0.2进行计算'

            print("1.1.4 Data inserted successfully!")
            print("[ACOS:{}][{}]查询已完成，请查看文件： ".format(acos_pro, s1) + output_filename)
            return "[ACOS:{}][{}]查询已完成，请查看文件： ".format(acos_pro, s1) + output_filename

        except Exception as error:
            print("1-1.4Error while inserting data:", error)

    def get_sd_product_115(self, market, startdate, enddate):
        """关键词优化分析：-1-1.5  找出在 2024.04.01 至 2024.04.14 这段时间内，  美国SD广告中 低于 20% 10% - 20% 的 sku广告。将这些信息生成csv文件，里面记录这些广告的以下信息，CPC，SKU/ASIN， ACOS, Clicks，adgroupid."""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            # ACOS求值放到sql中
            # 增加判断逻辑 如果ACOS>0.2 则以原ACOS为准，否则以0.2为基准
            query1 = """
                                SELECT
                                SUM(cost)/SUM(sales7d) as ACOS,
                                SUM(cost) AS total_cost,
                                COUNT(DISTINCT searchTerm) AS unique_search_terms,
                                SUM(clicks) AS total_clicks
                                FROM amazon_search_term_reports_sp WHERE market = '{}' AND date BETWEEN '{}' AND '{}'
                                """.format(market, startdate, enddate)
            df1 = pd.read_sql(query1, con=conn)
            # return df
            acos = df1.loc[0, 'ACOS']
            avgacos = acos if acos > 0.2 else 0.2
            # print(avgacos)

            query = """
                    SELECT
            promotedSku,
            promotedAsin,
            SUM(cost) AS cost,
            SUM(sales) AS sales,
            SUM(clicks) AS clicks,
            adGroupName,
            adGroupId,
            campaignId,
    campaignName,
            (SUM(cost) / SUM(sales)) AS ACOS
        FROM
            amazon_advertised_product_reports_sd
        WHERE
            date BETWEEN '{}' AND '{}'
            AND market = '{}'
        GROUP BY
            promotedSku,
            promotedAsin,
            adGroupName,
            adGroupId,
            campaignId,
    campaignName
        HAVING
             (SUM(cost) / SUM(sales)) > (0.2 * 0.8) and  (SUM(cost) / SUM(sales)) < (0.2 * 0.9);
                    """.format(startdate, enddate, market)
            df = pd.read_sql(query, con=conn)
            print(df)
            # 保存到CSV文件中
            output_filename = get_timestamp() + '_sd_product_1_5.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            s1 = f"优质广告数量：{df[['promotedSku', 'promotedAsin', 'adGroupName','campaignId','campaignName']].drop_duplicates().shape[0]}"
            acos_pro = '针对SD广告筛选，ACOS统一取值0.2进行计算'

            print("1.1.5 Data inserted successfully!")
            print("[ACOS:{}][{}]查询已完成，请查看文件： ".format(acos_pro, s1) + output_filename)
            return "[ACOS:{}][{}]查询已完成，请查看文件： ".format(acos_pro, s1) + output_filename

        except Exception as error:
            print("1-1.5Error while inserting data:", error)



    def get_sd_product_121(self, market, startdate, enddate):
        """关键词优化分析：-1-2.1  在 2024.04.01 至 2024.04.14 这段时间内，美国SD广告的 平均ACOS ，总广告消耗，sku广告数量 和 总点击量 分别是多少？"""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            query = """
            SELECT
            SUM(cost)/SUM(sales) as ACOS,
            SUM(cost) as total_cost, SUM(sales) as total_sales, COUNT(DISTINCT promotedSku) as sku_ad_count, SUM(clicks) as total_clicks
            FROM amazon_advertised_product_reports_sd
            WHERE market = '{}' AND date BETWEEN '{}' AND '{}'
            """.format(market, startdate, enddate)
            df = pd.read_sql(query, con=conn)
            # return df
            acos = df.loc[0, 'ACOS']
            total_cost = df.loc[0, 'total_cost']
            sku_ad_count = df.loc[0, 'sku_ad_count']
            total_clicks = df.loc[0, 'total_clicks']
            result_str = "平均ACOS:{} - 总广告消耗(Cost):{}  不同SKU的广告数量:{}个 - 总点击量(Clicks):{}次".format(acos, total_cost,
                                                                                                sku_ad_count,
                                                                                                total_clicks)
            return result_str

            print("1.2.1 Data inserted successfully!")

        except Exception as error:
            print("1.2.1 Error while inserting data:", error)


    def get_sd_product_122(self, market, startdate, enddate):
        """关键词优化分析：-1-2.2  在 2024.04.01 至 2024.04.14 这段时间内，  美国SP广告中 低于 平均ACOS值（替换为第一问结论） 10% 的 去重后定向投放关键词 数量 和 总点击量 是多少"""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            # ACOS求值放到sql中
            # 增加判断逻辑 如果ACOS>0.2 则以原ACOS为准，否则以0.2为基准
            query1 = """
                                SELECT
                                SUM(cost)/SUM(sales7d) as ACOS,
                                SUM(cost) AS total_cost,
                                COUNT(DISTINCT searchTerm) AS unique_search_terms,
                                SUM(clicks) AS total_clicks
                                FROM amazon_search_term_reports_sp WHERE market = '{}' AND date BETWEEN '{}' AND '{}'
                                """.format(market, startdate, enddate)
            df1 = pd.read_sql(query1, con=conn)
            # return df
            acos = df1.loc[0, 'ACOS']
            avgacos = acos if acos > 0.2 else 0.2
            # print(avgacos)

            query = """
                    SELECT
            promotedSku,
            promotedAsin,
            SUM(cost) AS cost,
            SUM(sales) AS sales,
            SUM(clicks) AS clicks,
            adGroupName,
            adGroupId,campaignId,campaignName,
            (SUM(cost) / SUM(sales)) AS ACOS
        FROM
            amazon_advertised_product_reports_sd
        WHERE
            date BETWEEN '{}' AND '{}'
            AND market = '{}'
        GROUP BY
            promotedSku,
            promotedAsin,
            adGroupName,
            adGroupId,campaignId,campaignName
        HAVING
            (SUM(cost) / SUM(sales)) > (0.2 * 1.1)
                    """.format(startdate, enddate, market)
            df = pd.read_sql(query, con=conn)
            print(df)
            # 保存到CSV文件中
            output_filename = get_timestamp() + '_sd_product_2_2.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            s1 = f"优质广告数量：{df[['promotedSku', 'promotedAsin', 'adGroupName','campaignId','campaignName']].drop_duplicates().shape[0]}"
            acos_pro = '针对SD广告筛选，ACOS统一取值0.2进行计算'

            print("1.2.2 Data inserted successfully!")
            print("[ACOS:{}][{}]查询已完成，请查看文件： ".format(acos_pro, s1) + output_filename)
            return "[ACOS:{}][{}]查询已完成，请查看文件： ".format(acos_pro, s1) + output_filename

        except Exception as error:
            print("1-2.2Error while inserting data:", error)

    def get_sd_product_123(self, market, startdate, enddate):
        """关键词优化分析：-1-2.3  找出在 2024.04.01 至 2024.04.14 这段时间内，  美国SD广告中 高于 20% 30% 以上的 sku广告。将这些信息生成csv文件，里面记录这些广告的以下信息，CPC，SKU/ASIN， ACOS,  Clicks，adgroupid."""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            # ACOS求值放到sql中
            # 增加判断逻辑 如果ACOS>0.2 则以原ACOS为准，否则以0.2为基准
            query1 = """
                                SELECT
                                SUM(cost)/SUM(sales7d) as ACOS,
                                SUM(cost) AS total_cost,
                                COUNT(DISTINCT searchTerm) AS unique_search_terms,
                                SUM(clicks) AS total_clicks
                                FROM amazon_search_term_reports_sp WHERE market = '{}' AND date BETWEEN '{}' AND '{}'
                                """.format(market, startdate, enddate)
            df1 = pd.read_sql(query1, con=conn)
            # return df
            acos = df1.loc[0, 'ACOS']
            avgacos = acos if acos > 0.2 else 0.2
            # print(avgacos)

            query = """
                    SELECT
            promotedSku,
            promotedAsin,
            SUM(cost) AS cost,
            SUM(sales) AS sales,
            SUM(clicks) AS clicks,
            adGroupName,
            adGroupId,campaignId,campaignName,
            (SUM(cost) / SUM(sales)) AS ACOS
        FROM
            amazon_advertised_product_reports_sd
        WHERE
            date BETWEEN '{}' AND '{}'
            AND market = '{}'
        GROUP BY
            promotedSku,
            promotedAsin,
            adGroupName,
            adGroupId,campaignId,campaignName
        HAVING
            (SUM(cost) / SUM(sales)) > (0.2 * 1.3);
                    """.format(startdate, enddate, market)
            df = pd.read_sql(query, con=conn)
            print(df)
            # 保存到CSV文件中
            output_filename = get_timestamp() + '_sd_product_2_3.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            s1 = f"优质广告数量：{df[['promotedSku', 'promotedAsin', 'adGroupName','campaignId','campaignName']].drop_duplicates().shape[0]}"
            acos_pro = '针对SD广告筛选，ACOS统一取值0.2进行计算'

            print("1.2.3 Data inserted successfully!")
            print("[ACOS:{}][{}]查询已完成，请查看文件： ".format(acos_pro, s1) + output_filename)
            return "[ACOS:{}][{}]查询已完成，请查看文件： ".format(acos_pro, s1) + output_filename

        except Exception as error:
            print("1-2.3Error while inserting data:", error)


    def get_sd_product_124(self, market, startdate, enddate):
        """关键词优化分析：-1-2.4  找出在 2024.04.01 至 2024.04.14 这段时间内，  美国SD广告中 高于 20% 20% - 30% 的 sku广告。将这些信息生成csv文件，里面记录这些广告的以下信息，CPC，SKU/ASIN， ACOS, Clicks，adgroupid."""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            # ACOS求值放到sql中
            # 增加判断逻辑 如果ACOS>0.2 则以原ACOS为准，否则以0.2为基准
            query1 = """
                                SELECT
                                SUM(cost)/SUM(sales7d) as ACOS,
                                SUM(cost) AS total_cost,
                                COUNT(DISTINCT searchTerm) AS unique_search_terms,
                                SUM(clicks) AS total_clicks
                                FROM amazon_search_term_reports_sp WHERE market = '{}' AND date BETWEEN '{}' AND '{}'
                                """.format(market, startdate, enddate)
            df1 = pd.read_sql(query1, con=conn)
            # return df
            acos = df1.loc[0, 'ACOS']
            avgacos = acos if acos > 0.2 else 0.2
            # print(avgacos)

            query = """
                    SELECT
            promotedSku,
            promotedAsin,
            SUM(cost) AS cost,
            SUM(sales) AS sales,
            SUM(clicks) AS clicks,
            adGroupName,
            adGroupId,campaignId,campaignName,
            (SUM(cost) / SUM(sales)) AS ACOS
        FROM
            amazon_advertised_product_reports_sd
        WHERE
            date BETWEEN '{}' AND '{}'
            AND market = '{}'
        GROUP BY
            promotedSku,
            promotedAsin,
            adGroupName,
            adGroupId,campaignId,campaignName
        HAVING
            (SUM(cost) / SUM(sales)) > (0.2 * 1.2) and  (SUM(cost) / SUM(sales)) < (0.2 * 1.3);
                    """.format(startdate, enddate, market)
            df = pd.read_sql(query, con=conn)
            print(df)
            # 保存到CSV文件中
            output_filename = get_timestamp() + '_sd_product_2_4.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            s1 = f"优质广告数量：{df[['promotedSku', 'promotedAsin', 'adGroupName','campaignId','campaignName']].drop_duplicates().shape[0]}"
            acos_pro = '针对SD广告筛选，ACOS统一取值0.2进行计算'

            print("1.2.4 Data inserted successfully!")
            print("[ACOS:{}][{}]查询已完成，请查看文件： ".format(acos_pro, s1) + output_filename)
            return "[ACOS:{}][{}]查询已完成，请查看文件： ".format(acos_pro, s1) + output_filename

        except Exception as error:
            print("1-2.4Error while inserting data:", error)


    def get_sd_product_125(self, market, startdate, enddate):
        """关键词优化分析：-1-2.5  找出在 2024.04.01 至 2024.04.14 这段时间内，  美国SD广告中 高于 20% 10% - 20% 的 sku广告。将这些信息生成csv文件，里面记录这些广告的以下信息，CPC，SKU/ASIN， ACOS, Clicks，adgroupid. """
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            # ACOS求值放到sql中
            # 增加判断逻辑 如果ACOS>0.2 则以原ACOS为准，否则以0.2为基准
            query1 = """
                                SELECT
                                SUM(cost)/SUM(sales7d) as ACOS,
                                SUM(cost) AS total_cost,
                                COUNT(DISTINCT searchTerm) AS unique_search_terms,
                                SUM(clicks) AS total_clicks
                                FROM amazon_search_term_reports_sp WHERE market = '{}' AND date BETWEEN '{}' AND '{}'
                                """.format(market, startdate, enddate)
            df1 = pd.read_sql(query1, con=conn)
            # return df
            acos = df1.loc[0, 'ACOS']
            avgacos = acos if acos > 0.2 else 0.2
            # print(avgacos)

            query = """
                    SELECT
            promotedSku,
            promotedAsin,
            SUM(cost) AS cost,
            SUM(sales) AS sales,
            SUM(clicks) AS clicks,
            adGroupName,
            adGroupId,campaignId,campaignName,
            (SUM(cost) / SUM(sales)) AS ACOS
        FROM
            amazon_advertised_product_reports_sd
        WHERE
            date BETWEEN '{}' AND '{}'
            AND market = '{}'
        GROUP BY
            promotedSku,
            promotedAsin,
            adGroupName,
            adGroupId,campaignId,campaignName
        HAVING
             (SUM(cost) / SUM(sales)) > (0.2 * 1.1) and  (SUM(cost) / SUM(sales)) < (0.2 * 1.2);
                    """.format(startdate, enddate, market)
            df = pd.read_sql(query, con=conn)
            print(df)
            # 保存到CSV文件中
            output_filename = get_timestamp() + '_sd_product_2_5.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            s1 = f"优质广告数量：{df[['promotedSku', 'promotedAsin', 'adGroupName','campaignId','campaignName']].drop_duplicates().shape[0]}"
            acos_pro = '针对SD广告筛选，ACOS统一取值0.2进行计算'

            print("1.2.5 Data inserted successfully!")
            print("[ACOS:{}][{}]查询已完成，请查看文件： ".format(acos_pro, s1) + output_filename)
            return "[ACOS:{}][{}]查询已完成，请查看文件： ".format(acos_pro, s1) + output_filename

        except Exception as error:
            print("1-2.5Error while inserting data:", error)





    def get_sd_advertise_211(self, market, startdate, enddate):
        """广告计划优化分析：-2-1.1  在 2024.04.01 至 2024.04.14 这段时间内，美国SD广告的 平均ACOS ，总广告消耗， campaign 广告活动数量 和 总点击量 分别是多少？"""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            query = """
            SELECT
            SUM(cost)/SUM(sales) as acos,
            SUM(cost) as total_cost, SUM(sales) as total_sales,
            COUNT(DISTINCT campaignId) as campaign_count,
            SUM(clicks) as total_clicks
            FROM amazon_campaign_reports_sd
            WHERE market = '{}' AND date BETWEEN '{}' AND '{}'
            """.format(market, startdate, enddate)
            df = pd.read_sql(query, con=conn)
            # return df
            acos = df.loc[0, 'acos']
            total_cost = df.loc[0, 'total_cost']
            sku_ad_count = df.loc[0, 'campaign_count']
            total_clicks = df.loc[0, 'total_clicks']
            result_str = "平均ACOS:{} - 总广告消耗(Cost):{}  不同广告活动（campaign）数量:{}个 - 总点击量(Clicks):{}次".format(acos, total_cost,
                                                                                                sku_ad_count,
                                                                                                total_clicks)
            return result_str

            print("2.1.1 Data inserted successfully!")

        except Exception as error:
            print("2.1.1 Error while inserting data:", error)


    def get_sd_advertise_212(self, market, startdate, enddate):
        """广告计划优化分析：-2-1.2 在 2024.04.01 至 2024.04.14 这段时间内，  美国SD广告中 低于 20% 10% 的  campaign 广告活动数量 和 总点击量 是多少"""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            # ACOS求值放到sql中
            # 增加判断逻辑 如果ACOS>0.2 则以原ACOS为准，否则以0.2为基准
            query1 = """
            SELECT
            SUM(cost)/SUM(sales7d) as ACOS,
            SUM(cost) AS total_cost,
            COUNT(DISTINCT searchTerm) AS unique_search_terms,
            SUM(clicks) AS total_clicks
            FROM amazon_search_term_reports_sp WHERE market = '{}' AND date BETWEEN '{}' AND '{}'
            """.format(market, startdate, enddate)
            df1 = pd.read_sql(query1, con=conn)
            # return df
            acos = df1.loc[0, 'ACOS']
            avgacos = acos if acos > 0.2 else 0.2
            # print(avgacos)

            query = """
             SELECT campaignId,campaignName,SUM(clicks) AS total_clicks,(SUM(cost) / SUM(sales)) AS ACOS,SUM(cost) as spend,(SUM(cost) / SUM(clicks)) AS CPC
FROM amazon_campaign_reports_sd
        WHERE
            date BETWEEN '{}' AND '{}'
            AND market = '{}'
        GROUP BY campaignName,campaignId,campaignId,campaignName
HAVING ACOS < (0.2 * 0.9)
                    """.format(startdate, enddate, market)
            df = pd.read_sql(query, con=conn)
            print(df)
            # 保存到CSV文件中
            output_filename = get_timestamp() + '_sd_advertise_1_2.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            s1 = f"优质广告数量：{df[['campaignName', 'campaignId']].drop_duplicates().shape[0]}"
            acos_pro = '针对SD广告筛选，ACOS统一取值0.2进行计算'

            print("2-1.2 Data inserted successfully!")
            print("[ACOS:{}][{}]查询已完成，请查看文件： ".format(acos_pro, s1) + output_filename)
            return "[ACOS:{}][{}]查询已完成，请查看文件： ".format(acos_pro, s1) + output_filename

        except Exception as error:
            print("2-1.2Error while inserting data:", error)

    def get_sd_advertise_213(self, market, startdate, enddate):
        """广告计划优化分析：-2-1.3  找出在 2024.04.01 至 2024.04.14 这段时间内，  美国SD广告中 低于 20% 30% 以上的  campaign 广告活动。将这些信息生成csv文件，里面记录这些广告的以下信息，CPC， ACOS,  Clicks，campaignid，spend."""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            # ACOS求值放到sql中
            # 增加判断逻辑 如果ACOS>0.2 则以原ACOS为准，否则以0.2为基准
            query1 = """
                                SELECT
                                SUM(cost)/SUM(sales7d) as ACOS,
                                SUM(cost) AS total_cost,
                                COUNT(DISTINCT searchTerm) AS unique_search_terms,
                                SUM(clicks) AS total_clicks
                                FROM amazon_search_term_reports_sp WHERE market = '{}' AND date BETWEEN '{}' AND '{}'
                                """.format(market, startdate, enddate)
            df1 = pd.read_sql(query1, con=conn)
            # return df
            acos = df1.loc[0, 'ACOS']
            avgacos = acos if acos > 0.2 else 0.2
            # print(avgacos)

            query = """
            SELECT SUM(clicks) AS total_clicks,(SUM(cost) / SUM(sales)) AS ACOS,campaignName,campaignId,SUM(cost) as spend,(SUM(cost) / SUM(clicks)) AS CPC
FROM amazon_campaign_reports_sd
        WHERE
            date BETWEEN '{}' AND '{}'
            AND market = '{}'
        GROUP BY campaignName,campaignId
HAVING ACOS < 0.14
                    """.format(startdate, enddate, market)
            df = pd.read_sql(query, con=conn)
            print(df)
            # 保存到CSV文件中
            output_filename = get_timestamp() + '_sd_advertise_1_3.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            s1 = f"优质广告数量：{df[['campaignName', 'campaignId']].drop_duplicates().shape[0]}"
            acos_pro = '针对SD广告筛选，ACOS统一取值0.2进行计算'

            print("2-1.3 Data inserted successfully!")
            print("[ACOS:{}][{}]查询已完成，请查看文件： ".format(acos_pro, s1) + output_filename)
            return "[ACOS:{}][{}]查询已完成，请查看文件： ".format(acos_pro, s1) + output_filename

        except Exception as error:
            print("2-1.3Error while inserting data:", error)


    def get_sd_advertise_214(self, market, startdate, enddate):
        """广告计划优化分析：-2-1.4  找出在 2024.04.01 至 2024.04.14 这段时间内，  美国SD广告中 低于 20% 20% - 30% 的 campaign 广告活动。将这些信息生成csv文件，里面记录这些广告的以下信息，CPC， ACOS,  Clicks，campaignid，spend."""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            # ACOS求值放到sql中
            # 增加判断逻辑 如果ACOS>0.2 则以原ACOS为准，否则以0.2为基准
            query1 = """
            SELECT
            SUM(cost)/SUM(sales7d) as ACOS,
            SUM(cost) AS total_cost,
            COUNT(DISTINCT searchTerm) AS unique_search_terms,
            SUM(clicks) AS total_clicks
            FROM amazon_search_term_reports_sp WHERE market = '{}' AND date BETWEEN '{}' AND '{}'
            """.format(market, startdate, enddate)
            df1 = pd.read_sql(query1, con=conn)
            # return df
            acos = df1.loc[0, 'ACOS']
            avgacos = acos if acos > 0.2 else 0.2
            # print(avgacos)

            query = """
                    SELECT SUM(clicks) AS total_clicks,(SUM(cost) / SUM(sales)) AS ACOS,campaignName,campaignId,SUM(cost) as spend,(SUM(cost) / SUM(clicks)) AS CPC
        FROM amazon_campaign_reports_sd
                WHERE
                    date BETWEEN '{}' AND '{}'
                    AND market = '{}'
                GROUP BY campaignName,campaignId
        HAVING ACOS < (0.2 * 0.8) and (0.2 * 0.7) <ACOS
                            """.format(startdate, enddate, market)
            df = pd.read_sql(query, con=conn)
            print(df)
            # 保存到CSV文件中
            output_filename = get_timestamp() + '_sd_advertise_1_4.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            s1 = f"优质广告数量：{df[['campaignName', 'campaignId']].drop_duplicates().shape[0]}"
            acos_pro = '针对SD广告筛选，ACOS统一取值0.2进行计算'

            print("2-1.4 Data inserted successfully!")
            print("[ACOS:{}][{}]查询已完成，请查看文件： ".format(acos_pro, s1) + output_filename)
            return "[ACOS:{}][{}]查询已完成，请查看文件： ".format(acos_pro, s1) + output_filename

        except Exception as error:
            print("2-1.4Error while inserting data:", error)


    def get_sd_advertise_215(self, market, startdate, enddate):
        """广告计划优化分析：-2-1.5  找出在 2024.04.01 至 2024.04.14 这段时间内，  美国SD广告中 低于 20% 10% - 20% 的 campaign 广告活动。将这些信息生成csv文件，里面记录这些广告的以下信息，CPC， ACOS,  Clicks，campaignid，spend."""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            # ACOS求值放到sql中
            # 增加判断逻辑 如果ACOS>0.2 则以原ACOS为准，否则以0.2为基准
            query1 = """
            SELECT
            SUM(cost)/SUM(sales7d) as ACOS,
            SUM(cost) AS total_cost,
            COUNT(DISTINCT searchTerm) AS unique_search_terms,
            SUM(clicks) AS total_clicks
            FROM amazon_search_term_reports_sp WHERE market = '{}' AND date BETWEEN '{}' AND '{}'
            """.format(market, startdate, enddate)
            df1 = pd.read_sql(query1, con=conn)
            # return df
            acos = df1.loc[0, 'ACOS']
            avgacos = acos if acos > 0.2 else 0.2
            # print(avgacos)

            query = """
                    SELECT SUM(clicks) AS total_clicks,(SUM(cost) / SUM(sales)) AS ACOS,campaignName,campaignId,SUM(cost) as spend,(SUM(cost) / SUM(clicks)) AS CPC
        FROM amazon_campaign_reports_sd
                WHERE
                    date BETWEEN '{}' AND '{}'
                    AND market = '{}'
                GROUP BY campaignName,campaignId
        HAVING ACOS < (0.2 * 0.9) and (0.2 * 0.8) <ACOS
                            """.format(startdate, enddate, market)
            df = pd.read_sql(query, con=conn)
            print(df)
            # 保存到CSV文件中
            output_filename = get_timestamp() + '_sd_advertise_1_5.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            s1 = f"优质广告数量：{df[['campaignName', 'campaignId']].drop_duplicates().shape[0]}"
            acos_pro = '针对SD广告筛选，ACOS统一取值0.2进行计算'

            print("2-1.5 Data inserted successfully!")
            print("[ACOS:{}][{}]查询已完成，请查看文件： ".format(acos_pro, s1) + output_filename)
            return "[ACOS:{}][{}]查询已完成，请查看文件： ".format(acos_pro, s1) + output_filename

        except Exception as error:
            print("2-1.5Error while inserting data:", error)




    def get_sd_advertise_221(self, market, startdate, enddate):
        """广告计划优化分析：-2-2.1  在 2024.04.01 至 2024.04.14 这段时间内，美国SD广告的 平均ACOS ，总广告消耗， campaign 广告活动数量 和 总点击量 分别是多少？"""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            query = """
            SELECT
            SUM(cost)/SUM(sales) as acos,
            SUM(cost) as total_cost, SUM(sales) as total_sales,
            COUNT(DISTINCT campaignId) as campaign_count,
            SUM(clicks) as total_clicks
            FROM amazon_campaign_reports_sd
            WHERE market = '{}' AND date BETWEEN '{}' AND '{}'
            """.format(market, startdate, enddate)
            df = pd.read_sql(query, con=conn)
            # return df
            acos = df.loc[0, 'acos']
            total_cost = df.loc[0, 'total_cost']
            sku_ad_count = df.loc[0, 'campaign_count']
            total_clicks = df.loc[0, 'total_clicks']
            result_str = "平均ACOS:{} - 总广告消耗(Cost):{}  不同广告活动（campaign）数量:{}个 - 总点击量(Clicks):{}次".format(acos, total_cost,
                                                                                                sku_ad_count,
                                                                                                total_clicks)
            return result_str

            print("2.2.1 Data inserted successfully!")

        except Exception as error:
            print("2.2.1 Error while inserting data:", error)


    def get_sd_advertise_222(self, market, startdate, enddate):
        """广告计划优化分析：-2-2.2 在 2024.04.01 至 2024.04.14 这段时间内，  美国SD广告中 高于 20% 10% 的  campaign 广告活动数量 和 总点击量 是多少"""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            # ACOS求值放到sql中
            # 增加判断逻辑 如果ACOS>0.2 则以原ACOS为准，否则以0.2为基准
            query1 = """
            SELECT
            SUM(cost)/SUM(sales7d) as ACOS,
            SUM(cost) AS total_cost,
            COUNT(DISTINCT searchTerm) AS unique_search_terms,
            SUM(clicks) AS total_clicks
            FROM amazon_search_term_reports_sp WHERE market = '{}' AND date BETWEEN '{}' AND '{}'
            """.format(market, startdate, enddate)
            df1 = pd.read_sql(query1, con=conn)
            # return df
            acos = df1.loc[0, 'ACOS']
            avgacos = acos if acos > 0.2 else 0.2
            # print(avgacos)

            query = """
             SELECT SUM(clicks) AS total_clicks,(SUM(cost) / SUM(sales)) AS ACOS,campaignName,campaignId,SUM(cost) as spend,(SUM(cost) / SUM(clicks)) AS CPC
FROM amazon_campaign_reports_sd
        WHERE
            date BETWEEN '{}' AND '{}'
            AND market = '{}'
        GROUP BY campaignName,campaignId
HAVING ACOS > 0.22
                    """.format(startdate, enddate, market)
            df = pd.read_sql(query, con=conn)
            print(df)
            # 保存到CSV文件中
            output_filename = get_timestamp() + '_sd_advertise_2_2.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            s1 = f"优质广告数量：{df[['campaignName', 'campaignId']].drop_duplicates().shape[0]}"
            acos_pro = '针对SD广告筛选，ACOS统一取值0.2进行计算'

            print("2-2.2 Data inserted successfully!")
            print("[ACOS:{}][{}]查询已完成，请查看文件： ".format(acos_pro, s1) + output_filename)
            return "[ACOS:{}][{}]查询已完成，请查看文件： ".format(acos_pro, s1) + output_filename

        except Exception as error:
            print("2-2.2Error while inserting data:", error)

    def get_sd_advertise_223(self, market, startdate, enddate):
        """广告计划优化分析：-2-2.3  找出在 2024.04.01 至 2024.04.14 这段时间内，  美国SD广告中 高于 20% 30% 以上的  campaign 广告活动。将这些信息生成csv文件，里面记录这些广告的以下信息，CPC， ACOS,  Clicks，campaignid，spend."""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            # ACOS求值放到sql中
            # 增加判断逻辑 如果ACOS>0.2 则以原ACOS为准，否则以0.2为基准
            query1 = """
                                SELECT
                                SUM(cost)/SUM(sales7d) as ACOS,
                                SUM(cost) AS total_cost,
                                COUNT(DISTINCT searchTerm) AS unique_search_terms,
                                SUM(clicks) AS total_clicks
                                FROM amazon_search_term_reports_sp WHERE market = '{}' AND date BETWEEN '{}' AND '{}'
                                """.format(market, startdate, enddate)
            df1 = pd.read_sql(query1, con=conn)
            # return df
            acos = df1.loc[0, 'ACOS']
            avgacos = acos if acos > 0.2 else 0.2
            # print(avgacos)

            query = """
            SELECT SUM(clicks) AS total_clicks,(SUM(cost) / SUM(sales)) AS ACOS,campaignName,campaignId,SUM(cost) as spend,(SUM(cost) / SUM(clicks)) AS CPC
FROM amazon_campaign_reports_sd
        WHERE
            date BETWEEN '{}' AND '{}'
            AND market = '{}'
        GROUP BY campaignName,campaignId
HAVING ACOS >0.26
                    """.format(startdate, enddate, market)
            df = pd.read_sql(query, con=conn)
            print(df)
            # 保存到CSV文件中
            output_filename = get_timestamp() + '_sd_advertise_2_3.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            s1 = f"优质广告数量：{df[['campaignName', 'campaignId']].drop_duplicates().shape[0]}"
            acos_pro = '针对SD广告筛选，ACOS统一取值0.2进行计算'

            print("2-2.3 Data inserted successfully!")
            print("[ACOS:{}][{}]查询已完成，请查看文件： ".format(acos_pro, s1) + output_filename)
            return "[ACOS:{}][{}]查询已完成，请查看文件： ".format(acos_pro, s1) + output_filename

        except Exception as error:
            print("2-2.3Error while inserting data:", error)


    def get_sd_advertise_224(self, market, startdate, enddate):
        """广告计划优化分析：-2-2.4  找出在 2024.04.01 至 2024.04.14 这段时间内，  美国SD广告中 高于 20% 20% - 30% 的 campaign 广告活动。将这些信息生成csv文件，里面记录这些广告的以下信息，CPC， ACOS,  Clicks，campaignid，spend."""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            # ACOS求值放到sql中
            # 增加判断逻辑 如果ACOS>0.2 则以原ACOS为准，否则以0.2为基准
            query1 = """
            SELECT
            SUM(cost)/SUM(sales7d) as ACOS,
            SUM(cost) AS total_cost,
            COUNT(DISTINCT searchTerm) AS unique_search_terms,
            SUM(clicks) AS total_clicks
            FROM amazon_search_term_reports_sp WHERE market = '{}' AND date BETWEEN '{}' AND '{}'
            """.format(market, startdate, enddate)
            df1 = pd.read_sql(query1, con=conn)
            # return df
            acos = df1.loc[0, 'ACOS']
            avgacos = acos if acos > 0.2 else 0.2
            # print(avgacos)

            query = """
                    SELECT SUM(clicks) AS total_clicks,(SUM(cost) / SUM(sales)) AS ACOS,campaignName,campaignId,SUM(cost) as spend,(SUM(cost) / SUM(clicks)) AS CPC
        FROM amazon_campaign_reports_sd
                WHERE
                    date BETWEEN '{}' AND '{}'
                    AND market = '{}'
                GROUP BY campaignName,campaignId
        HAVING ACOS < (0.2 * 1.3) and (0.2 * 1.2) <ACOS
                            """.format(startdate, enddate, market)
            df = pd.read_sql(query, con=conn)
            print(df)
            # 保存到CSV文件中
            output_filename = get_timestamp() + '_sd_advertise_2_4.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            s1 = f"优质广告数量：{df[['campaignName', 'campaignId']].drop_duplicates().shape[0]}"
            acos_pro = '针对SD广告筛选，ACOS统一取值0.2进行计算'

            print("2-2.4 Data inserted successfully!")
            print("[ACOS:{}][{}]查询已完成，请查看文件： ".format(acos_pro, s1) + output_filename)
            return "[ACOS:{}][{}]查询已完成，请查看文件： ".format(acos_pro, s1) + output_filename

        except Exception as error:
            print("2-2.4 Error while inserting data:", error)


    def get_sd_advertise_225(self, market, startdate, enddate):
        """广告计划优化分析：-2-2.5  找出在 2024.04.01 至 2024.04.14 这段时间内，  美国SD广告中 高于 20% 10% - 20% 的 campaign 广告活动。将这些信息生成csv文件，里面记录这些广告的以下信息，CPC， ACOS,  Clicks，campaignid，spend."""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            # ACOS求值放到sql中
            # 增加判断逻辑 如果ACOS>0.2 则以原ACOS为准，否则以0.2为基准
            query1 = """
            SELECT
            SUM(cost)/SUM(sales7d) as ACOS,
            SUM(cost) AS total_cost,
            COUNT(DISTINCT searchTerm) AS unique_search_terms,
            SUM(clicks) AS total_clicks
            FROM amazon_search_term_reports_sp WHERE market = '{}' AND date BETWEEN '{}' AND '{}'
            """.format(market, startdate, enddate)
            df1 = pd.read_sql(query1, con=conn)
            # return df
            acos = df1.loc[0, 'ACOS']
            avgacos = acos if acos > 0.2 else 0.2
            # print(avgacos)

            query = """
                    SELECT SUM(clicks) AS total_clicks,(SUM(cost) / SUM(sales)) AS ACOS,campaignName,campaignId,SUM(cost) as spend,(SUM(cost) / SUM(clicks)) AS CPC
        FROM amazon_campaign_reports_sd
                WHERE
                    date BETWEEN '{}' AND '{}'
                    AND market = '{}'
                GROUP BY campaignName,campaignId
        HAVING ACOS < (0.2 * 1.2) and (0.2 * 1.1) <ACOS
                            """.format(startdate, enddate, market)
            df = pd.read_sql(query, con=conn)
            print(df)
            # 保存到CSV文件中
            output_filename = get_timestamp() + '_sd_advertise_2_5.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            s1 = f"优质广告数量：{df[['campaignName', 'campaignId']].drop_duplicates().shape[0]}"
            acos_pro = '针对SD广告筛选，ACOS统一取值0.2进行计算'

            print("2-2.5 Data inserted successfully!")
            print("[ACOS:{}][{}]查询已完成，请查看文件： ".format(acos_pro, s1) + output_filename)
            return "[ACOS:{}][{}]查询已完成，请查看文件： ".format(acos_pro, s1) + output_filename

        except Exception as error:
            print("2-2.5Error while inserting data:", error)





    def get_sd_targeting_311(self, market, startdate, enddate):
        """广告计划优化分析：-3-1.1  在 2024.04.01 至 2024.04.14 这段时间内，美国SD广告的 平均ACOS ，总广告消耗cost，去重后广告标签categories的总数量 和 总点击量 分别是多少？"""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            query = """
            SELECT
            SUM(cost)/SUM(sales) as acos,
            SUM(cost) as total_cost, SUM(sales) as total_sales,
            COUNT(DISTINCT targetingId) as unique_targeting_count,
            SUM(clicks) as total_clicks
            FROM amazon_targeting_reports_sd
            WHERE market = '{}' AND date BETWEEN '{}' AND '{}'
            """.format(market, startdate, enddate)
            df = pd.read_sql(query, con=conn)
            # return df
            acos = df.loc[0, 'acos']
            total_cost = df.loc[0, 'total_cost']
            unique_targeting_count = df.loc[0, 'unique_targeting_count']
            total_clicks = df.loc[0, 'total_clicks']
            result_str = "平均ACOS:{} - 总广告消耗(Cost):{}  去重后广告标签categories的总数量:{}个 - 总点击量(Clicks):{}次".format(acos, total_cost,
                                                                                                unique_targeting_count,
                                                                                                total_clicks)
            return result_str

            print("3.1.1 Data inserted successfully!")

        except Exception as error:
            print("3.1.1 Error while inserting data:", error)


    def get_sd_targeting_312(self, market, startdate, enddate):
        """广告计划优化分析：-3-1.2 在 2024.04.01 至 2024.04.14 这段时间内，  美国SD广告中 低于 20% 10% 的 去重后广告标签categories 数量 和 总点击量 是多少"""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            # ACOS求值放到sql中
            # 增加判断逻辑 如果ACOS>0.2 则以原ACOS为准，否则以0.2为基准
            query1 = """
            SELECT
            SUM(cost)/SUM(sales7d) as ACOS,
            SUM(cost) AS total_cost,
            COUNT(DISTINCT searchTerm) AS unique_search_terms,
            SUM(clicks) AS total_clicks
            FROM amazon_search_term_reports_sp WHERE market = '{}' AND date BETWEEN '{}' AND '{}'
            """.format(market, startdate, enddate)
            df1 = pd.read_sql(query1, con=conn)
            # return df
            acos = df1.loc[0, 'ACOS']
            avgacos = acos if acos > 0.2 else 0.2
            # print(avgacos)

            query = """
             SELECT campaignId,campaignName,adGroupName,adGroupId,targetingId, targetingText, SUM(clicks) AS total_clicks ,SUM(cost) as total_cost, SUM(sales) as total_sales,(SUM(cost) / SUM(sales)) as ACOS
FROM amazon_targeting_reports_sd
        WHERE
            date BETWEEN '{}' AND '{}'
            AND market = '{}'
        GROUP BY campaignId,campaignName,adGroupName,adGroupId,targetingId, targetingText
HAVING ACOS < (0.2 * 0.9)
                    """.format(startdate, enddate, market)
            df = pd.read_sql(query, con=conn)
            print(df)
            # 保存到CSV文件中
            output_filename = get_timestamp() + '_sd_targeting_1_2.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            s1 = f"去重后广告标签categories 数量：{df[['targetingId', 'targetingText','adGroupName','adGroupId','campaignId','campaignName']].drop_duplicates().shape[0]}"
            acos_pro = '针对SD广告筛选，ACOS统一取值0.2进行计算'

            print("3-1.2 Data inserted successfully!")
            print("[ACOS:{}][{}]查询已完成，请查看文件： ".format(acos_pro, s1) + output_filename)
            return "[ACOS:{}][{}]查询已完成，请查看文件： ".format(acos_pro, s1) + output_filename

        except Exception as error:
            print("3-1.2Error while inserting data:", error)

    def get_sd_targeting_313(self, market, startdate, enddate):
        """广告计划优化分析：-3-1.3  找出在 2024.04.01 至 2024.04.14 这段时间内，  美国SD广告中 低于 20% 30% 以上的 广告标签categories。将这些广告标签categories信息生成csv文件，里面记录这些广告标签categories的以下信息，CPC， ACOS, Clicks，adgroupid."""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            # ACOS求值放到sql中
            # 增加判断逻辑 如果ACOS>0.2 则以原ACOS为准，否则以0.2为基准
            query1 = """
                                SELECT
                                SUM(cost)/SUM(sales7d) as ACOS,
                                SUM(cost) AS total_cost,
                                COUNT(DISTINCT searchTerm) AS unique_search_terms,
                                SUM(clicks) AS total_clicks
                                FROM amazon_search_term_reports_sp WHERE market = '{}' AND date BETWEEN '{}' AND '{}'
                                """.format(market, startdate, enddate)
            df1 = pd.read_sql(query1, con=conn)
            # return df
            acos = df1.loc[0, 'ACOS']
            avgacos = acos if acos > 0.2 else 0.2
            # print(avgacos)

            query = """
            SELECT campaignId,campaignName,adGroupName,adGroupId,targetingId, targetingText , SUM(clicks) AS total_clicks ,SUM(cost) as total_cost, SUM(sales) as total_sales,(SUM(cost) / SUM(sales)) as ACOS
FROM amazon_targeting_reports_sd
        WHERE
            date BETWEEN '{}' AND '{}'
            AND market = '{}'
        GROUP BY campaignId,campaignName,adGroupName,adGroupId,targetingId, targetingText
HAVING ACOS < (0.2 * 0.7)
                    """.format(startdate, enddate, market)
            df = pd.read_sql(query, con=conn)
            print(df)
            # 保存到CSV文件中
            output_filename = get_timestamp() + '_sd_targeting_1_3.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            s1 = f"去重后广告标签categories 数量：{df[['targetingId', 'targetingText','adGroupName','adGroupId','campaignId','campaignName']].drop_duplicates().shape[0]}"
            acos_pro = '针对SD广告筛选，ACOS统一取值0.2进行计算'

            print("313 Data inserted successfully!")
            print("[ACOS:{}][{}]查询已完成，请查看文件： ".format(acos_pro, s1) + output_filename)
            return "[ACOS:{}][{}]查询已完成，请查看文件： ".format(acos_pro, s1) + output_filename

        except Exception as error:
            print("313Error while inserting data:", error)


    def get_sd_targeting_314(self, market, startdate, enddate):
        """广告计划优化分析：-3-1.4  找出在 2024.04.01 至 2024.04.14 这段时间内，  美国SD广告中 低于 20% 20% - 30% 的 广告标签categories。将这些广告标签categories信息生成csv文件，里面记录这些广告标签categories的以下信息，CPC， ACOS, Clicks，adgroupid."""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            # ACOS求值放到sql中
            # 增加判断逻辑 如果ACOS>0.2 则以原ACOS为准，否则以0.2为基准
            query1 = """
            SELECT
            SUM(cost)/SUM(sales7d) as ACOS,
            SUM(cost) AS total_cost,
            COUNT(DISTINCT searchTerm) AS unique_search_terms,
            SUM(clicks) AS total_clicks
            FROM amazon_search_term_reports_sp WHERE market = '{}' AND date BETWEEN '{}' AND '{}'
            """.format(market, startdate, enddate)
            df1 = pd.read_sql(query1, con=conn)
            # return df
            acos = df1.loc[0, 'ACOS']
            avgacos = acos if acos > 0.2 else 0.2
            # print(avgacos)

            query = """
                    SELECT campaignId,campaignName,adGroupName,adGroupId,targetingId, targetingText , SUM(clicks) AS total_clicks ,SUM(cost) as total_cost, SUM(sales) as total_sales,(SUM(cost) / SUM(sales)) as ACOS
FROM amazon_targeting_reports_sd
                WHERE
                    date BETWEEN '{}' AND '{}'
                    AND market = '{}'
                GROUP BY campaignId,campaignName,adGroupName,adGroupId,targetingId, targetingText
HAVING  ACOS < (0.2 * 0.8) and (0.2 * 0.7) <ACOS
                            """.format(startdate, enddate, market)
            df = pd.read_sql(query, con=conn)
            print(df)
            # 保存到CSV文件中
            output_filename = get_timestamp() + '_sd_targeting_1_4.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            s1 = f"去重后广告标签categories 数量：{df[['targetingId', 'targetingText','adGroupName','adGroupId','campaignId','campaignName']].drop_duplicates().shape[0]}"
            acos_pro = '针对SD广告筛选，ACOS统一取值0.2进行计算'

            print("314 Data inserted successfully!")
            print("[ACOS:{}][{}]查询已完成，请查看文件： ".format(acos_pro, s1) + output_filename)
            return "[ACOS:{}][{}]查询已完成，请查看文件： ".format(acos_pro, s1) + output_filename

        except Exception as error:
            print("314 Error while inserting data:", error)


    def get_sd_targeting_315(self, market, startdate, enddate):
        """广告计划优化分析：-3-1.5  找出在 2024.04.01 至 2024.04.14 这段时间内，  美国SD广告中 低于 20% 10% - 20% 的 广告标签categories。将这些广告标签categories信息生成csv文件，里面记录这些广告标签categories的以下信息，CPC， ACOS, Clicks，adgroupid."""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            # ACOS求值放到sql中
            # 增加判断逻辑 如果ACOS>0.2 则以原ACOS为准，否则以0.2为基准
            query1 = """
            SELECT
            SUM(cost)/SUM(sales7d) as ACOS,
            SUM(cost) AS total_cost,
            COUNT(DISTINCT searchTerm) AS unique_search_terms,
            SUM(clicks) AS total_clicks
            FROM amazon_search_term_reports_sp WHERE market = '{}' AND date BETWEEN '{}' AND '{}'
            """.format(market, startdate, enddate)
            df1 = pd.read_sql(query1, con=conn)
            # return df
            acos = df1.loc[0, 'ACOS']
            avgacos = acos if acos > 0.2 else 0.2
            # print(avgacos)

            query = """
                    SELECT campaignId,campaignName,adGroupName,adGroupId,targetingId, targetingText, SUM(clicks) AS total_clicks ,SUM(cost) as total_cost, SUM(sales) as total_sales,(SUM(cost) / SUM(sales)) as ACOS
FROM amazon_targeting_reports_sd
                WHERE
                    date BETWEEN '{}' AND '{}'
                    AND market = '{}'
                GROUP BY campaignId,campaignName,adGroupName,adGroupId,targetingId, targetingText
HAVING  ACOS < (0.2 * 0.9) and (0.2 * 0.8) <ACOS
                            """.format(startdate, enddate, market)
            df = pd.read_sql(query, con=conn)
            print(df)
            # 保存到CSV文件中
            output_filename = get_timestamp() + '_sd_targeting_1_5.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            s1 = f"去重后广告标签categories 数量：{df[['targetingId', 'targetingText','adGroupName','adGroupId','campaignId','campaignName']].drop_duplicates().shape[0]}"
            acos_pro = '针对SD广告筛选，ACOS统一取值0.2进行计算'

            print("315 Data inserted successfully!")
            print("[ACOS:{}][{}]查询已完成，请查看文件： ".format(acos_pro, s1) + output_filename)
            return "[ACOS:{}][{}]查询已完成，请查看文件： ".format(acos_pro, s1) + output_filename

        except Exception as error:
            print("315Error while inserting data:", error)






    def get_sd_targeting_321(self, market, startdate, enddate):
        """广告计划优化分析：-3-2.1  在 2024.04.01 至 2024.04.14 这段时间内，美国SD广告的 平均ACOS ，总广告消耗cost，去重后广告标签categories的总数量 和 总点击量 分别是多少？"""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            query = """
            SELECT
            SUM(cost)/SUM(sales) as acos,
            SUM(cost) as total_cost, SUM(sales) as total_sales,
            COUNT(DISTINCT targetingId) as unique_targeting_count,
            SUM(clicks) as total_clicks
            FROM amazon_targeting_reports_sd
            WHERE market = '{}' AND date BETWEEN '{}' AND '{}'
            """.format(market, startdate, enddate)
            df = pd.read_sql(query, con=conn)
            # return df
            acos = df.loc[0, 'acos']
            total_cost = df.loc[0, 'total_cost']
            unique_targeting_count = df.loc[0, 'unique_targeting_count']
            total_clicks = df.loc[0, 'total_clicks']
            result_str = "平均ACOS:{} - 总广告消耗(Cost):{}  去重后广告标签categories的总数量:{}个 - 总点击量(Clicks):{}次".format(acos, total_cost,
                                                                                                unique_targeting_count,
                                                                                                total_clicks)
            return result_str

            print("3.2.1 Data inserted successfully!")

        except Exception as error:
            print("3.2.1 Error while inserting data:", error)


    def get_sd_targeting_322(self, market, startdate, enddate):
        """广告计划优化分析：-3-2.2 在 2024.04.01 至 2024.04.14 这段时间内，  美国SD广告中 低于 20% 10% 的 去重后广告标签categories 数量 和 总点击量 是多少"""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            # ACOS求值放到sql中
            # 增加判断逻辑 如果ACOS>0.2 则以原ACOS为准，否则以0.2为基准
            query1 = """
            SELECT
            SUM(cost)/SUM(sales7d) as ACOS,
            SUM(cost) AS total_cost,
            COUNT(DISTINCT searchTerm) AS unique_search_terms,
            SUM(clicks) AS total_clicks
            FROM amazon_search_term_reports_sp WHERE market = '{}' AND date BETWEEN '{}' AND '{}'
            """.format(market, startdate, enddate)
            df1 = pd.read_sql(query1, con=conn)
            # return df
            acos = df1.loc[0, 'ACOS']
            avgacos = acos if acos > 0.2 else 0.2
            # print(avgacos)

            query = """
             SELECT campaignId,campaignName,adGroupName,adGroupId,targetingId, targetingText, SUM(clicks) AS total_clicks ,SUM(cost) as total_cost, SUM(sales) as total_sales,(SUM(cost) / SUM(sales)) as ACOS
FROM amazon_targeting_reports_sd
        WHERE
            date BETWEEN '{}' AND '{}'
            AND market = '{}'
        GROUP BY campaignId,campaignName,adGroupName,adGroupId,targetingId, targetingText
HAVING ACOS > 0.22
                    """.format(startdate, enddate, market)
            df = pd.read_sql(query, con=conn)
            print(df)
            # 保存到CSV文件中
            output_filename = get_timestamp() + '_sd_targeting_2_2.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            s1 = f"去重后广告标签categories 数量：{df[['targetingId', 'targetingText','adGroupName','adGroupId','campaignId','campaignName']].drop_duplicates().shape[0]}"
            acos_pro = '针对SD广告筛选，ACOS统一取值0.2进行计算'

            print("3-2.2 Data inserted successfully!")
            print("[ACOS:{}][{}]查询已完成，请查看文件： ".format(acos_pro, s1) + output_filename)
            return "[ACOS:{}][{}]查询已完成，请查看文件： ".format(acos_pro, s1) + output_filename

        except Exception as error:
            print("3-2.2Error while inserting data:", error)

    def get_sd_targeting_323(self, market, startdate, enddate):
        """广告计划优化分析：-3-2.3  找出在 2024.04.01 至 2024.04.14 这段时间内，  美国SD广告中 低于 20% 30% 以上的 广告标签categories。将这些广告标签categories信息生成csv文件，里面记录这些广告标签categories的以下信息，CPC， ACOS, Clicks，adgroupid."""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            # ACOS求值放到sql中
            # 增加判断逻辑 如果ACOS>0.2 则以原ACOS为准，否则以0.2为基准
            query1 = """
                                SELECT
                                SUM(cost)/SUM(sales7d) as ACOS,
                                SUM(cost) AS total_cost,
                                COUNT(DISTINCT searchTerm) AS unique_search_terms,
                                SUM(clicks) AS total_clicks
                                FROM amazon_search_term_reports_sp WHERE market = '{}' AND date BETWEEN '{}' AND '{}'
                                """.format(market, startdate, enddate)
            df1 = pd.read_sql(query1, con=conn)
            # return df
            acos = df1.loc[0, 'ACOS']
            avgacos = acos if acos > 0.2 else 0.2
            # print(avgacos)

            query = """
            SELECT campaignId,campaignName,adGroupName,adGroupId,targetingId, targetingText, SUM(clicks) AS total_clicks ,SUM(cost) as total_cost, SUM(sales) as total_sales,(SUM(cost) / SUM(sales)) as ACOS
FROM amazon_targeting_reports_sd
        WHERE
            date BETWEEN '{}' AND '{}'
            AND market = '{}'
        GROUP BY campaignId,campaignName,adGroupName,adGroupId,targetingId, targetingText
HAVING  (0.2 * 1.3) <ACOS
                    """.format(startdate, enddate, market)
            df = pd.read_sql(query, con=conn)
            print(df)
            # 保存到CSV文件中
            output_filename = get_timestamp() + '_sd_targeting_2_3.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            s1 = f"去重后广告标签categories 数量：{df[['targetingId', 'targetingText','adGroupName','adGroupId','campaignId','campaignName']].drop_duplicates().shape[0]}"
            acos_pro = '针对SD广告筛选，ACOS统一取值0.2进行计算'

            print("323 Data inserted successfully!")
            print("[ACOS:{}][{}]查询已完成，请查看文件： ".format(acos_pro, s1) + output_filename)
            return "[ACOS:{}][{}]查询已完成，请查看文件： ".format(acos_pro, s1) + output_filename

        except Exception as error:
            print("323Error while inserting data:", error)


    def get_sd_targeting_324(self, market, startdate, enddate):
        """广告计划优化分析：-3-2.4  找出在 2024.04.01 至 2024.04.14 这段时间内，  美国SD广告中 低于 20% 20% - 30% 的 广告标签categories。将这些广告标签categories信息生成csv文件，里面记录这些广告标签categories的以下信息，CPC， ACOS, Clicks，adgroupid."""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            # ACOS求值放到sql中
            # 增加判断逻辑 如果ACOS>0.2 则以原ACOS为准，否则以0.2为基准
            query1 = """
            SELECT
            SUM(cost)/SUM(sales7d) as ACOS,
            SUM(cost) AS total_cost,
            COUNT(DISTINCT searchTerm) AS unique_search_terms,
            SUM(clicks) AS total_clicks
            FROM amazon_search_term_reports_sp WHERE market = '{}' AND date BETWEEN '{}' AND '{}'
            """.format(market, startdate, enddate)
            df1 = pd.read_sql(query1, con=conn)
            # return df
            acos = df1.loc[0, 'ACOS']
            avgacos = acos if acos > 0.2 else 0.2
            # print(avgacos)

            query = """
                    SELECT campaignId,campaignName,adGroupName,adGroupId,targetingId, targetingText, SUM(clicks) AS total_clicks ,SUM(cost) as total_cost, SUM(sales) as total_sales,(SUM(cost) / SUM(sales)) as ACOS
FROM amazon_targeting_reports_sd
                WHERE
                    date BETWEEN '{}' AND '{}'
                    AND market = '{}'
                GROUP BY campaignId,campaignName,adGroupName,adGroupId,targetingId, targetingText
HAVING  ACOS < (0.2 * 1.3) and (0.2 * 1.2) <ACOS
                            """.format(startdate, enddate, market)
            df = pd.read_sql(query, con=conn)
            print(df)
            # 保存到CSV文件中
            output_filename = get_timestamp() + '_sd_targeting_2_4.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            s1 = f"去重后广告标签categories 数量：{df[['targetingId', 'targetingText','adGroupName','adGroupId','campaignId','campaignName']].drop_duplicates().shape[0]}"
            acos_pro = '针对SD广告筛选，ACOS统一取值0.2进行计算'

            print("324 Data inserted successfully!")
            print("[ACOS:{}][{}]查询已完成，请查看文件： ".format(acos_pro, s1) + output_filename)
            return "[ACOS:{}][{}]查询已完成，请查看文件： ".format(acos_pro, s1) + output_filename

        except Exception as error:
            print("324 Error while inserting data:", error)


    def get_sd_targeting_325(self, market, startdate, enddate):
        """广告计划优化分析：-3-2.5  找出在 2024.04.01 至 2024.04.14 这段时间内，  美国SD广告中 低于 20% 10% - 20% 的 广告标签categories。将这些广告标签categories信息生成csv文件，里面记录这些广告标签categories的以下信息，CPC， ACOS, Clicks，adgroupid."""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            # ACOS求值放到sql中
            # 增加判断逻辑 如果ACOS>0.2 则以原ACOS为准，否则以0.2为基准
            query1 = """
            SELECT
            SUM(cost)/SUM(sales7d) as ACOS,
            SUM(cost) AS total_cost,
            COUNT(DISTINCT searchTerm) AS unique_search_terms,
            SUM(clicks) AS total_clicks
            FROM amazon_search_term_reports_sp WHERE market = '{}' AND date BETWEEN '{}' AND '{}'
            """.format(market, startdate, enddate)
            df1 = pd.read_sql(query1, con=conn)
            # return df
            acos = df1.loc[0, 'ACOS']
            avgacos = acos if acos > 0.2 else 0.2
            # print(avgacos)

            query = """
                    SELECT campaignId,campaignName,adGroupName,adGroupId,targetingId, targetingText, SUM(clicks) AS total_clicks ,SUM(cost) as total_cost, SUM(sales) as total_sales,(SUM(cost) / SUM(sales)) as ACOS
FROM amazon_targeting_reports_sd
                WHERE
                    date BETWEEN '{}' AND '{}'
                    AND market = '{}'
                GROUP BY campaignId,campaignName,adGroupName,adGroupId,targetingId, targetingText
HAVING  ACOS < (0.2 * 1.2) and (0.2 * 1.1) <ACOS
                            """.format(startdate, enddate, market)
            df = pd.read_sql(query, con=conn)
            print(df)
            # 保存到CSV文件中
            output_filename = get_timestamp() + '_sd_targeting_2_5.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            s1 = f"去重后广告标签categories 数量：{df[['targetingId', 'targetingText','adGroupName','adGroupId','campaignId','campaignName']].drop_duplicates().shape[0]}"
            acos_pro = '针对SD广告筛选，ACOS统一取值0.2进行计算'

            print("325 Data inserted successfully!")
            print("[ACOS:{}][{}]查询已完成，请查看文件： ".format(acos_pro, s1) + output_filename)
            return "[ACOS:{}][{}]查询已完成，请查看文件： ".format(acos_pro, s1) + output_filename

        except Exception as error:
            print("325Error while inserting data:", error)



# if __name__ == '__main__':
#     print("====amazon ====")
#     startdate = '2024-04-01'
#     endate = '2024-04-14'
#     market = 'US'
#     dwx = AmazonMysqlSDRagUitl(db_info)
#     daily_cost_rate = dwx.get_sp_searchterm_keyword_info_belowavg10per(market, startdate,endate)
#     print(daily_cost_rate)
#     # daily_cost_rate2 = dwx.get_sp_searchterm_keyword_info_belowavg10per(market, startdate,endate)
#     # daily_cost_rate3 = dwx.get_sp_searchterm_keyword_info_belowavg10toper_notintarget(market, startdate,endate)
#     # print(daily_cost_rate)
#     # print(daily_cost_rate2)
#     # print(daily_cost_rate3)



