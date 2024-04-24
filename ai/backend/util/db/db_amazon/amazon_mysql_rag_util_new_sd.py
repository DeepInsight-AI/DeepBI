import pymysql
import pandas as pd
from datetime import datetime
import warnings

# 忽略特定类型的警告
warnings.filterwarnings("ignore", category=UserWarning)

db_info = {'host': '****', 'user': '****', 'passwd': '****', 'port': 3308,
               'db': '****',
               'charset': 'utf8mb4', 'use_unicode': True, }

def get_timestamp():
    # 获取当前时间
    current_time = datetime.now()
    timestamp = int(current_time.timestamp())
    date_string = current_time.strftime("%Y-%m-%d")
    # 组合日期和时间戳
    date_timestamp_string = f"{date_string}_{timestamp}"
    return date_timestamp_string

class AmazonMysqlNEWSDRagUitl:

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


    def get_sd_product_111(self, startdate , enddate):
        """优秀标签-1.统计美国SD广告优质标签：-1-1.1 在 2024.04.01 至 2024.04.14 这段时间内，统计美国SD广告中平均ACOS ，总广告消耗cost，去重后广告标签categories的总数量 和 总点击量 分别是多少"""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            query = """
            SELECT SUM(cost)/SUM(sales) as ACOS ,SUM(cost) AS total_cost, SUM(sales) AS total_sales,
             COUNT(DISTINCT targetingId) AS unique_targeting_id_count, SUM(clicks) AS total_clicks
             FROM amazon_targeting_reports_sd
            WHERE market = 'US' AND date BETWEEN '{}' AND '{}'
            """.format(startdate, enddate)
            df = pd.read_sql(query, con=conn)
            # return df
            acos=df.loc[0,'ACOS']
            total_cost = df.loc[0, 'total_cost']
            unique_targeting_id_count = df.loc[0, 'unique_targeting_id_count']
            total_clicks  = df.loc[0, 'total_clicks']
            result_str = "平均ACOS:{} - 总广告消耗(Cost):{}  去重后广告标签categories的总数量:{}个 - 总点击量(Clicks):{}次".format(acos,total_cost,unique_targeting_id_count,total_clicks)
            return result_str

            print("1.1.1 Data inserted successfully!")

        except Exception as error:
            print("1.1.1 Error while inserting data:", error)


    def get_sd_product_112(self, startdate, enddate):
        """优秀标签-1.统计美国SD广告优质标签：-1-1.2 在 2024.04.01 至 2024.04.14 这段时间内，  美国SD广告中ACOS 低于  20% 的广告标签categories 数量 和 总点击量 是多少"""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            query = """
SELECT count(DISTINCT targetingId) AS targeting_id, SUM(clicks) AS total_clicks ,SUM(cost) as total_cost,
 SUM(sales) as total_sales,adGroupName,adGroupId,(SUM(cost) / SUM(sales)) as ACOS
FROM amazon_targeting_reports_sd
 WHERE market = 'US' AND date BETWEEN '{}' AND '{}'
GROUP BY targetingId, targetingText ,adGroupName,adGroupId
HAVING ACOS < 0.2
            """.format( startdate, enddate )
            df = pd.read_sql(query, con=conn)
            # return df
            targeting_id = df['targeting_id'].sum()
            total_clicks = df['total_clicks'].sum()
            total_cost = df.loc[0, 'total_cost']
            total_sales = df.loc[0, 'total_sales']
            result_str = "美国的SD广告中符合要求的不重复的广告标签的数量是:{}个，总点击量:{}".format(targeting_id,total_clicks)
            return result_str

            print("1.1.2 Data inserted successfully!")

        except Exception as error:
            print("1-1.2Error while inserting data:", error)


    def get_sd_product_113(self, startdate, enddate):
        """优秀标签-1.统计美国SD广告优质标签：-1-1.3  找出在 2024.04.01 至 2024.04.14 这段时间内，  美国SD广告中 低于 20% 30% 以上的 sku广告。将这些信息生成csv文件，里面记录这些广告的以下信息，CPC，SKU/ASIN， ACOS,  Clicks，adgroupid."""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US

            query = """
                SELECT targetingId, targetingText, SUM(clicks) AS total_clicks ,SUM(cost) as total_cost_dollar,
                SUM(sales) as total_sales_dollar,adGroupName,adGroupId,(SUM(cost) / SUM(sales)) as ACOS,(SUM(cost) / SUM(clicks)) as CPC_dollar,
                (SUM(cost) / SUM(clicks))*0.94 AS CPC_EU ,campaignName
                FROM amazon_targeting_reports_sd
                WHERE market = 'US' AND date BETWEEN '{}' AND '{}'
                GROUP BY targetingId, targetingText ,adGroupName,adGroupId,campaignName
                HAVING ACOS < 0.2
                    """.format(startdate, enddate)
            df = pd.read_sql(query, con=conn)
            print(df)
            # 保存到CSV文件中
            output_filename = get_timestamp() + '_new_sd_targetting_1_3.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            s1 = f"优质广告标签categories：{df[['targetingId', 'targetingText', 'adGroupName','adGroupId','campaignName']].drop_duplicates().shape[0]}"
            print("1.1.3 Data inserted successfully!")
            print("[{}]查询已完成，请查看文件： ".format(s1) + output_filename)
            return "[{}]查询已完成，请查看文件： ".format(s1) + output_filename

        except Exception as error:
            print("1-1.3Error while inserting data:", error)


    def get_sd_product_121(self, startdate, enddate):
        """优秀标签-1.统计德国SD广告优质标签：-1-2.1 在 2024.04.01 至 2024.04.14 这段时间内，统计德国SD广告中平均ACOS ，总广告消耗cost，去重后广告标签categories的总数量 和 总点击量 分别是多少"""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            query = """
            SELECT SUM(cost)/SUM(sales) as ACOS ,SUM(cost) AS total_cost, SUM(sales) AS total_sales,
             COUNT(DISTINCT targetingId) AS unique_targeting_id_count, SUM(clicks) AS total_clicks
             FROM amazon_targeting_reports_sd
            WHERE market = 'DE' AND date BETWEEN '{}' AND '{}'
            """.format(startdate, enddate)
            df = pd.read_sql(query, con=conn)
            # return df
            acos=df.loc[0,'ACOS']
            total_cost = df.loc[0, 'total_cost']
            unique_targeting_id_count = df.loc[0, 'unique_targeting_id_count']
            total_clicks  = df.loc[0, 'total_clicks']
            result_str = "平均ACOS:{} - 总广告消耗(Cost):{}  去重后广告标签categories的总数量:{}个 - 总点击量(Clicks):{}次".format(acos,total_cost,unique_targeting_id_count,total_clicks)
            return result_str

            print("1.1.1 Data inserted successfully!")

        except Exception as error:
            print("1.1.1 Error while inserting data:", error)


    def get_sd_product_122(self, startdate, enddate):
        """优秀标签-1.统计德国SD广告优质标签：-1-2.2 在 2024.04.01 至 2024.04.14 这段时间内，  德国SD广告中ACOS 低于  20% 的广告标签categories 数量 和 总点击量 是多少"""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            query = """
SELECT count(DISTINCT targetingId) AS targeting_id, SUM(clicks) AS total_clicks ,SUM(cost) as total_cost,
 SUM(sales) as total_sales,adGroupName,adGroupId,(SUM(cost) / SUM(sales)) as ACOS
FROM amazon_targeting_reports_sd
 WHERE market = 'DE' AND date BETWEEN '{}' AND '{}'
GROUP BY targetingId, targetingText ,adGroupName,adGroupId
HAVING ACOS < 0.2
            """.format( startdate, enddate )
            df = pd.read_sql(query, con=conn)
            # return df
            targeting_id = df['targeting_id'].sum()
            total_clicks = df['total_clicks'].sum()
            total_cost = df.loc[0, 'total_cost']
            total_sales = df.loc[0, 'total_sales']
            result_str = "德国的SD广告中符合要求的不重复的广告标签的数量是:{}个，总点击量:{}".format(targeting_id,total_clicks)
            return result_str

            print("1.2.2 Data inserted successfully!")

        except Exception as error:
            print("1-2.2Error while inserting data:", error)

    def get_sd_product_123(self, startdate, enddate):
        """优秀标签-1.统计德国SD广告优质标签：-1-2.3  找出在 2024.04.01 至 2024.04.14 这段时间内，  德国SD广告中 低于 20% 30% 以上的 sku广告。将这些信息生成csv文件，里面记录这些广告的以下信息，CPC，SKU/ASIN， ACOS,  Clicks，adgroupid."""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US

            query = """
                SELECT targetingId, targetingText, SUM(clicks) AS total_clicks ,SUM(cost) as total_cost_dollar,
                SUM(sales) as total_sales_dollar,adGroupName,adGroupId,(SUM(cost) / SUM(sales)) as ACOS,(SUM(cost) / SUM(clicks)) as CPC_dollar,
                (SUM(cost) / SUM(clicks))*0.94 AS CPC_EU ,campaignName
                FROM amazon_targeting_reports_sd
                WHERE market = 'DE' AND date BETWEEN '{}' AND '{}'
                GROUP BY targetingId, targetingText ,adGroupName,adGroupId,campaignName
                HAVING ACOS < 0.2
                    """.format(startdate, enddate)
            df = pd.read_sql(query, con=conn)
            print(df)
            # 保存到CSV文件中
            output_filename = get_timestamp() + '_new_sd_targetting_2_3.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            s1 = f"优质广告标签categories：{df[['targetingId', 'targetingText', 'adGroupName','adGroupId','campaignName']].drop_duplicates().shape[0]}"
            print("1.2.3 Data inserted successfully!")
            print("[{}]查询已完成，请查看文件： ".format(s1) + output_filename)
            return "[{}]查询已完成，请查看文件： ".format(s1) + output_filename

        except Exception as error:
            print("1-2.3Error while inserting data:", error)


    def get_sd_advertise_211(self, market, startdate, enddate):
        """优秀关键词：-2-1.1在 2024.04.01 至 2024.04.14 这段时间内，本国SP广告中平均ACOS ，总广告消耗cost，去重后广告关键词的总数量 和 总点击量 分别是多少"""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            query = """
            SELECT
            SUM(cost)/SUM(sales7d) as ACOS,
            SUM(cost) AS total_cost, SUM(sales7d) AS total_sales_7d, COUNT(DISTINCT(keyword)) AS unique_keywords_count,
             SUM(clicks) AS total_clicks
             FROM amazon_targeting_reports_sp
            WHERE market = '{}' AND date BETWEEN '{}' AND '{}'
            """.format(market,startdate, enddate)
            df = pd.read_sql(query, con=conn)
            # return df
            acos=df.loc[0,'ACOS']
            total_cost = df.loc[0, 'total_cost']
            unique_keywords_count = df.loc[0, 'unique_keywords_count']
            total_clicks  = df.loc[0, 'total_clicks']
            result_str = "平均ACOS:{} - 总广告消耗(Cost):{}  去重后关键词的总数量:{}个 - 总点击量(Clicks):{}次".format(acos,total_cost,unique_keywords_count,total_clicks)
            return result_str

            print("2.1.1 Data inserted successfully!")

        except Exception as error:
            print("2.1.1 Error while inserting data:", error)


    def get_sd_advertise_212(self, market, startdate, enddate):
        """优秀关键词：-2-1.2在 2024.04.01 至 2024.04.14 这段时间内，  本国SP广告中ACOS 低于  20% 的 去重后广告关键词数量 和 总点击量 是多少"""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            query = """
             SELECT keyword, SUM(cost) as total_cost_7d, SUM(sales1d) as total_sales_7d,
             SUM(clicks) as total_clicks
             FROM amazon_targeting_reports_sp
            WHERE market = '{}' AND date BETWEEN '{}' AND '{}'
            GROUP BY keyword HAVING SUM(sales7d) > 0 AND (SUM(cost) / SUM(sales7d)) < 0.2
            """.format(market,startdate, enddate)
            df = pd.read_sql(query, con=conn)
            # return df
            unique_keywords_count = df['keyword'].nunique()
            total_clicks = df['total_clicks'].sum()
            result_str = "针对{}市场的Sponsored Products（SP）广告中满足ACOS（广告成本销售比）小于0.2的条件的去重后的广告关键词数量是:{}个，这些关键词的总点击量为:{}次。".format(market,unique_keywords_count,total_clicks)
            return result_str

            print("2.1.2 Data inserted successfully!")

        except Exception as error:
            print("2.1.2 Error while inserting data:", error)

    def get_sd_advertise_213(self, market, startdate, enddate):
        """优秀关键词：-2-1.3 找出在 2024.04.01 至 2024.04.14 这段时间内，  本国SP广告中ACOS 低于 20% 以上的 广告关键词。将这些广告关键词信息生成csv文件，里面记录这些广告关键词的以下信息：CPC（美元和欧元分两列标出）， ACOS, Clicks，adgroupId，adGroupName，keywordText（原本关键词text和翻译后的text），keywordId."""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US

            query = """
                SELECT
keywordId,keyword,SUM(clicks) AS total_clicks ,sum(cost) as total_cost_EU ,sum(sales14d) as total_sales, (SUM(cost) / SUM(sales14d)) as ACOS, adGroupId,adGroupName,(SUM(cost) / SUM(clicks)) as CPC_EU, campaignName, targeting
FROM amazon_targeting_reports_sp
WHERE date BETWEEN '{}' AND '{}' AND market = '{}'
group by keywordId,keyword,adGroupId,adGroupName,campaignName, targeting
having  ACOS < 0.2
                    """.format(startdate, enddate,market)
            df = pd.read_sql(query, con=conn)
            print(df)
            # 保存到CSV文件中
            output_filename = get_timestamp() + '_new_sd_keyword_1_3.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            s1 = f"优质广告标签categories：{df[['keywordId', 'keyword', 'adGroupId','adGroupName','campaignName','targeting']].drop_duplicates().shape[0]}"
            print("2.1.3 Data inserted successfully!")
            print("[{}]查询已完成，请查看文件： ".format(s1) + output_filename)
            return "[{}]查询已完成，请查看文件： ".format(s1) + output_filename

        except Exception as error:
            print("2.1.3 Error while inserting data:", error)


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
             SELECT targetingId, targetingText, SUM(clicks) AS total_clicks ,SUM(cost) as total_cost, SUM(sales) as total_sales,adGroupName,adGroupId,(SUM(cost) / SUM(sales)) as ACOS
FROM amazon_targeting_reports_sd
        WHERE
            date BETWEEN '{}' AND '{}'
            AND market = '{}'
        GROUP BY targetingId, targetingText ,adGroupName,adGroupId
HAVING ACOS < (0.2 * 0.9)
                    """.format(startdate, enddate, market)
            df = pd.read_sql(query, con=conn)
            print(df)
            # 保存到CSV文件中
            output_filename = get_timestamp() + '_sd_targeting_1_2.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            s1 = f"去重后广告标签categories 数量：{df[['targetingId', 'targetingText','adGroupName','adGroupId']].drop_duplicates().shape[0]}"
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
            SELECT targetingId, targetingText, SUM(clicks) AS total_clicks ,SUM(cost) as total_cost, SUM(sales) as total_sales,adGroupName,adGroupId,(SUM(cost) / SUM(sales)) as ACOS
FROM amazon_targeting_reports_sd
        WHERE
            date BETWEEN '{}' AND '{}'
            AND market = '{}'
        GROUP BY targetingId, targetingText ,adGroupName,adGroupId
HAVING ACOS < (0.2 * 0.7)
                    """.format(startdate, enddate, market)
            df = pd.read_sql(query, con=conn)
            print(df)
            # 保存到CSV文件中
            output_filename = get_timestamp() + '_sd_targeting_1_3.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            s1 = f"去重后广告标签categories 数量：{df[['targetingId', 'targetingText','adGroupName','adGroupId']].drop_duplicates().shape[0]}"
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
                    SELECT targetingId, targetingText, SUM(clicks) AS total_clicks ,SUM(cost) as total_cost, SUM(sales) as total_sales,adGroupName,adGroupId,(SUM(cost) / SUM(sales)) as ACOS
FROM amazon_targeting_reports_sd
                WHERE
                    date BETWEEN '{}' AND '{}'
                    AND market = '{}'
                GROUP BY targetingId, targetingText ,adGroupName,adGroupId
HAVING  ACOS < (0.2 * 0.8) and (0.2 * 0.7) <ACOS
                            """.format(startdate, enddate, market)
            df = pd.read_sql(query, con=conn)
            print(df)
            # 保存到CSV文件中
            output_filename = get_timestamp() + '_sd_targeting_1_4.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            s1 = f"去重后广告标签categories 数量：{df[['targetingId', 'targetingText','adGroupName','adGroupId']].drop_duplicates().shape[0]}"
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
                    SELECT targetingId, targetingText, SUM(clicks) AS total_clicks ,SUM(cost) as total_cost, SUM(sales) as total_sales,adGroupName,adGroupId,(SUM(cost) / SUM(sales)) as ACOS
FROM amazon_targeting_reports_sd
                WHERE
                    date BETWEEN '{}' AND '{}'
                    AND market = '{}'
                GROUP BY targetingId, targetingText ,adGroupName,adGroupId
HAVING  ACOS < (0.2 * 0.9) and (0.2 * 0.8) <ACOS
                            """.format(startdate, enddate, market)
            df = pd.read_sql(query, con=conn)
            print(df)
            # 保存到CSV文件中
            output_filename = get_timestamp() + '_sd_targeting_1_5.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            s1 = f"去重后广告标签categories 数量：{df[['targetingId', 'targetingText','adGroupName','adGroupId']].drop_duplicates().shape[0]}"
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
             SELECT targetingId, targetingText, SUM(clicks) AS total_clicks ,SUM(cost) as total_cost, SUM(sales) as total_sales,adGroupName,adGroupId,(SUM(cost) / SUM(sales)) as ACOS
FROM amazon_targeting_reports_sd
        WHERE
            date BETWEEN '{}' AND '{}'
            AND market = '{}'
        GROUP BY targetingId, targetingText ,adGroupName,adGroupId
HAVING ACOS > 0.22
                    """.format(startdate, enddate, market)
            df = pd.read_sql(query, con=conn)
            print(df)
            # 保存到CSV文件中
            output_filename = get_timestamp() + '_sd_targeting_2_2.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            s1 = f"去重后广告标签categories 数量：{df[['targetingId', 'targetingText','adGroupName','adGroupId']].drop_duplicates().shape[0]}"
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
            SELECT targetingId, targetingText, SUM(clicks) AS total_clicks ,SUM(cost) as total_cost, SUM(sales) as total_sales,adGroupName,adGroupId,(SUM(cost) / SUM(sales)) as ACOS
FROM amazon_targeting_reports_sd
        WHERE
            date BETWEEN '{}' AND '{}'
            AND market = '{}'
        GROUP BY targetingId, targetingText ,adGroupName,adGroupId
HAVING  (0.2 * 1.3) <ACOS
                    """.format(startdate, enddate, market)
            df = pd.read_sql(query, con=conn)
            print(df)
            # 保存到CSV文件中
            output_filename = get_timestamp() + '_sd_targeting_2_3.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            s1 = f"去重后广告标签categories 数量：{df[['targetingId', 'targetingText','adGroupName','adGroupId']].drop_duplicates().shape[0]}"
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
                    SELECT targetingId, targetingText, SUM(clicks) AS total_clicks ,SUM(cost) as total_cost, SUM(sales) as total_sales,adGroupName,adGroupId,(SUM(cost) / SUM(sales)) as ACOS
FROM amazon_targeting_reports_sd
                WHERE
                    date BETWEEN '{}' AND '{}'
                    AND market = '{}'
                GROUP BY targetingId, targetingText ,adGroupName,adGroupId
HAVING  ACOS < (0.2 * 1.3) and (0.2 * 1.2) <ACOS
                            """.format(startdate, enddate, market)
            df = pd.read_sql(query, con=conn)
            print(df)
            # 保存到CSV文件中
            output_filename = get_timestamp() + '_sd_targeting_2_4.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            s1 = f"去重后广告标签categories 数量：{df[['targetingId', 'targetingText','adGroupName','adGroupId']].drop_duplicates().shape[0]}"
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
                    SELECT targetingId, targetingText, SUM(clicks) AS total_clicks ,SUM(cost) as total_cost, SUM(sales) as total_sales,adGroupName,adGroupId,(SUM(cost) / SUM(sales)) as ACOS
FROM amazon_targeting_reports_sd
                WHERE
                    date BETWEEN '{}' AND '{}'
                    AND market = '{}'
                GROUP BY targetingId, targetingText ,adGroupName,adGroupId
HAVING  ACOS < (0.2 * 1.2) and (0.2 * 1.1) <ACOS
                            """.format(startdate, enddate, market)
            df = pd.read_sql(query, con=conn)
            print(df)
            # 保存到CSV文件中
            output_filename = get_timestamp() + '_sd_targeting_2_5.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            s1 = f"去重后广告标签categories 数量：{df[['targetingId', 'targetingText','adGroupName','adGroupId']].drop_duplicates().shape[0]}"
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
    # daily_cost_rate2 = dwx.get_sp_searchterm_keyword_info_belowavg10per(market, startdate,endate)
    # daily_cost_rate3 = dwx.get_sp_searchterm_keyword_info_belowavg10toper_notintarget(market, startdate,endate)
    # print(daily_cost_rate)
    # print(daily_cost_rate2)
    # print(daily_cost_rate3)
    # print('-------------2---------------')
    # res2_1 = dwx.get_sp_searchterm_keyword_info_target(market, startdate,endate)
    # res2_2 = dwx.get_sp_searchterm_keyword_info_target_below10per(market, startdate,endate)
    # res2_3 = dwx.get_sp_searchterm_keyword_info_target_below30per_csv(market, startdate, endate)
    # res2_4 = dwx.get_sp_searchterm_keyword_info_target_20and30per_csv(market, startdate, endate)
    # res2_4 = dwx.get_sp_searchterm_keyword_info_target_10and20per_csv(market, startdate, endate)
    # print('-------------3---------------')
    # # res4_1=dwx.get_sp__keyword_info_targetacos(market, startdate,endate)
    # # res4_1=dwx.get_sp_keyword_target_up10per(market, startdate,endate)
    # # print(res4_1)
    #
    # # print("-------------------------产品优化-------------")
    # # res2_2 = dwx.get_sp_product_upacos10to20(market, startdate,endate)
    # # print(res2_2)
    # print("-------------------------广告计划优化-------------")
    # res2_2 = dwx.get_sp_campaignplacement_below10to20(market, startdate,endate)
    # print(res2_2)




