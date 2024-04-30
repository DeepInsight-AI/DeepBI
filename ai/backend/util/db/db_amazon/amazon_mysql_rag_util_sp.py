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


    def get_sp_searchterm_keyword_info(self, market, startdate , enddate):
        """关键词优化分析：-1-1.1 在 2024.04.01 至 2024.04.14 这段时间内，美国SP广告的 平均ACOS ，总广告消耗，去重后用户搜索关键词 数量 和 总点击量 分别是多少？"""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            query = """
            SELECT
            SUM(cost)/SUM(sales7d) as ACOS,
            SUM(cost) AS total_cost,
            COUNT(DISTINCT searchTerm) AS unique_search_terms,
            SUM(clicks) AS total_clicks
            FROM amazon_search_term_reports_sp WHERE market = '{}' AND date BETWEEN '{}' AND '{}'
            """.format(market, startdate, enddate)
            df = pd.read_sql(query, con=conn)
            # return df
            acos=df.loc[0,'ACOS']
            total_cost = df.loc[0, 'total_cost']
            unique_search_terms  = df.loc[0, 'unique_search_terms']
            total_clicks  = df.loc[0, 'total_clicks']
            result_str = "平均ACOS:{} - 总广告消耗(Cost):{} - 去重后用户搜索关键词数量:{}个 - 总点击量(Clicks):{}次".format(acos,total_cost,unique_search_terms,total_clicks)
            return result_str

            print("1.1.1 Data inserted successfully!")

        except Exception as error:
            print("1.1.1 Error while inserting data:", error)


    def get_sp_searchterm_keyword_info_belowavg10per(self, market, startdate, enddate):
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

            query = """ SELECT searchTerm, SUM(clicks) as total_clicks, SUM(cost)/SUM(sales7d) as ACOS
FROM amazon_search_term_reports_sp
WHERE `date` BETWEEN '{}' AND '{}' AND market='{}'
GROUP BY searchTerm HAVING IFNULL(SUM(sales7d), 0) > 0 AND SUM(cost)/SUM(sales7d) < {} *(1-0.1)""".format( startdate, enddate ,market ,avgacos)
            df = pd.read_sql(query, con=conn)
            # 计算去重后的关键词数量和总点击量
            unique_search_terms_count = df['searchTerm'].nunique()
            total_clicks = df['total_clicks'].sum()
            # 打印结果
            res={ "unique_search_terms_count": unique_search_terms_count, "total_clicks": total_clicks }
            return res

            print("1.1.2 Data inserted successfully!")

        except Exception as error:
            print("1-1.2Error while inserting data:", error)


    def get_sp_searchterm_keyword_info_belowavg10toper_notintarget1(self, market, startdate, enddate):
        """关键词优化分析：-1-1.3  在 2024.04.01 至 2024.04.14 这段时间内，  美国SP广告中 低于 平均ACOS值（替换为第一问结论） 10% 的用户搜索关键词 中， 没有在广告定向投放中出现的关键词数量 和 总点击量 是多少"""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US

            # 增加acos判断
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

            query = """

SELECT count(DISTINCT keywordId) as  keywordIdcount ,sum(clicks)  as totalclicks FROM amazon_search_term_reports_sp
WHERE
    (market = '{}' and date BETWEEN '{}' AND '{}')
    AND (cost/sales14d) < ({}*0.9)
    AND keywordId not in (
        SELECT DISTINCT keywordId FROM amazon_targeting_reports_sp
        WHERE
            (market = '{}' and date BETWEEN '{}' AND '{}')
            AND (cost/sales14d) < ({}*0.9)
    )
    """.format( market,startdate, enddate ,avgacos,market, startdate, enddate ,avgacos )
            df = pd.read_sql(query, con=conn)
            # return df

            print("Data inserted successfully!")
            keywordIdcount = df.loc[0, 'keywordIdcount']
            totalclicks = df.loc[0, 'totalclicks']
            result_str = "去重后用户搜索关键词数量:{}个 - 总点击量(Clicks):{}次".format(keywordIdcount,totalclicks)
            print(result_str)
            return result_str

        except Exception as error:
            print("1-1.3Error while inserting data:", error)


    def get_sp_searchterm_keyword_info_belowavg10toper_notintarget(self, market, startdate, enddate):
        """关键词优化分析：-1-1.4  找出 在 2024.04.01 至 2024.04.14 这段时间内，  美国SP广告中 低于 平均ACOS值（替换为第一问结论） 10%  ，并且 没有在广告定向投放中出现的 用户搜索关键词。 将这些关键词信息生成csv文件，里面记录这些关键词的以下信息，CPC，SKU/ASIN， ACOS, Clicks."""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            # 增加acos判断
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
            query = """
            with temp_kw as (
select  keywordId,sum(cost),sum(sales14d)
from amazon_search_term_reports_sp  astrs
where
astrs.market = '{}'
and astrs.DATE BETWEEN '{}'  AND '{}'
and not exists (select * from amazon_targeting_reports_sp atrs where atrs.keywordId = astrs.keywordId)
group by keywordId
having sum(cost)/sum(sales14d)<(
select
sum(cost) / sum(sales14d)
from amazon_search_term_reports_sp
where market = '{}'   and DATE BETWEEN '{}'  AND '{}'
))
select
keywordId,cost/clicks as cpc,
cost/sales14d AS ACOS,
clicks
from amazon_search_term_reports_sp
where market = '{}'   and DATE BETWEEN '{}'  AND '{}'
and keywordId in (select keywordId from temp_kw)
            """.format(market, startdate, enddate, market, startdate, enddate, market, startdate, enddate)
            df = pd.read_sql(query, con=conn)
            # return df
            output_filename = get_timestamp() + '_targeting_keywords_1_4.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            # return df
            print("1.1.4 Data inserted successfully!")
            return "查询已完成，请查看文件： " + output_filename


            print("Data inserted successfully!")

        except Exception as error:
            print("1-1.4Error while inserting data:", error)


    def get_sp_searchterm_keyword_info_target(self, market, startdate, enddate):
        """关键词优化分析：-1-2.1  在 2024.04.01 至 2024.04.14 这段时间内，美国SP广告的 平均ACOS ，总广告消耗，去重后定向投放关键词 数量 和 总点击量 分别是多少？"""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            query = """
            SELECT
SUM(cost)/SUM(sales14d) AS avg_acos,
sum(cost) as total_cost,
count(distinct keywordId) as keyword_count,
sum(clicks) as total_clicks
FROM amazon_targeting_reports_sp WHERE date BETWEEN '{}' AND '{}' AND market = '{}' and  targeting != '*'
            """.format(startdate, enddate,market)
            df = pd.read_sql(query, con=conn)
            # return df
            avg_acos = df.loc[0, 'avg_acos']
            total_cost = df.loc[0, 'total_cost']
            keyword_count = df.loc[0, 'keyword_count']
            total_clicks = df.loc[0, 'total_clicks']
            result_str = "美国SP广告的 平均ACOS 为{}，总广告消耗:{}，去重后定向投放关键词数量:{} 和 总点击量是{}".format(avg_acos, total_cost ,keyword_count,total_clicks)
            return result_str

            print("1-2.1 Data inserted successfully!")

        except Exception as error:
            print("1-2.1Error while inserting data:", error)


    def get_sp_searchterm_keyword_info_target_below10per(self, market, startdate, enddate):
        """关键词优化分析：-1-2.2  在 2024.04.01 至 2024.04.14 这段时间内，  美国SP广告中 低于 平均ACOS值（替换为第一问结论） 10% 的 去重后定向投放关键词 数量 和 总点击量 是多少"""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            wherestr="date BETWEEN '{}' AND '{}' AND market = '{}' and  targeting != '*'".format(startdate, enddate,market)
            query1 = """
                        SELECT
            SUM(cost)/SUM(sales14d) AS avg_acos,
            sum(cost) as total_cost,
            count(distinct keywordId) as keyword_count,
            sum(clicks) as total_clicks
            FROM amazon_targeting_reports_sp WHERE date BETWEEN '{}' AND '{}' AND market = '{}' and  targeting != '*'
                        """.format(startdate, enddate, market)
            df = pd.read_sql(query1, con=conn)
            # return df
            avg_acos = df.loc[0, 'avg_acos']
            # avgacos = avg_acos if avg_acos > 0.2 else 0.2
            acos_pro=str(avg_acos)
            if avg_acos < 0.2:
                avg_acos=0.2
                acos_pro="由于平均ACOS小于20%，为了提升优化效果这里优化指标以20%为标准"

            query = """
            with tempacos as (
SELECT
SUM(cost)/SUM(sales14d) AS avg_acos
FROM amazon_targeting_reports_sp WHERE {}
)
SELECT
COUNT(DISTINCT targeting) AS keyword_count, SUM(clicks) AS total_clicks
FROM amazon_targeting_reports_sp t  WHERE {}
AND (cost / sales14d) < {} AND (cost / sales14d) > {} * (1-0.1)
            """.format(wherestr,wherestr,avg_acos,avg_acos)
            df = pd.read_sql(query, con=conn)
            # return df
            keyword_count = df.loc[0, 'keyword_count']
            total_clicks = df.loc[0, 'total_clicks']
            result_str = "美国SP广告中 低于 平均ACOS值（{}） 10% 的 去重后定向投放关键词 数量为:{} 和 总点击量 是:{}".format(acos_pro,keyword_count, total_clicks)
            return result_str

            print("1-2.2Data inserted successfully!")

        except Exception as error:
            print("1-2.2Error while inserting data:", error)

    def get_sp_searchterm_keyword_info_target_below30per_csv(self, market, startdate, enddate):
        """关键词优化分析：-1-2.3  找出在 2024.04.01 至 2024.04.14 这段时间内，  美国SP广告中 低于 平均ACOS值（替换为第一问结论） 30% 以上的 定向投放关键词。将这些关键词信息生成csv文件，里面记录这些关键词的以下信息，CPC，SKU/ASIN， ACOS, Clicks，adgroupid"""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            wherestr="t.date BETWEEN '{}' AND '{}' AND t.market = '{}' and  t.targeting != '*'".format(startdate, enddate,market)
            query1 = """
                                    SELECT
                        SUM(cost)/SUM(sales14d) AS avg_acos,
                        sum(cost) as total_cost,
                        count(distinct keywordId) as keyword_count,
                        sum(clicks) as total_clicks
                        FROM amazon_targeting_reports_sp WHERE date BETWEEN '{}' AND '{}' AND market = '{}' and  targeting != '*'
                                    """.format(startdate, enddate, market)
            df = pd.read_sql(query1, con=conn)
            # return df
            avg_acos = df.loc[0, 'avg_acos']
            # avgacos = avg_acos if avg_acos > 0.2 else 0.2
            acos_pro = str(avg_acos)
            if avg_acos < 0.2:
                avg_acos = 0.2
                acos_pro = "由于平均ACOS小于20%，为了提升优化效果这里优化指标以20%为标准"
            query = """
            SELECT
keywordId,keyword,
COUNT(DISTINCT targeting) AS keyword_count, SUM(clicks) AS total_clicks ,
sum(cost) as totalcost,sum(sales14d) as totalsales
FROM amazon_targeting_reports_sp t
WHERE {}
group by keywordId,keyword
having  totalcost/totalsales > 0 AND totalcost/totalsales < {} * 0.7
            """.format(wherestr,avg_acos)
            # 插叙返回结果
            # df = pd.read_sql(query, con=conn)
            # return df

            # 读取数据到DataFrame中
            df_keywords = pd.read_sql(query, con=conn)
            print(df_keywords)
            # 保存到CSV文件中
            output_filename = get_timestamp()+'_targeting_keywords_2_3.csv'
            df_keywords.to_csv(output_filename, index=False, encoding='utf-8-sig')
            s1=f"关键词数量：{df_keywords['keywordId'].nunique()}"
            s2=f"总点击量：{df_keywords['total_clicks'].sum()}"

            print("Data inserted successfully!")
            print("[ACOS:{}][{},{}]查询已完成，请查看文件： ".format(acos_pro,s1,s2)+output_filename)
            return "[ACOS:{}][{},{}]查询已完成，请查看文件： ".format(acos_pro,s1,s2)+output_filename

        except Exception as error:
            print("1-2.3Error while inserting data:", error)

    def get_sp_searchterm_keyword_info_target_20and30per_csv(self, market, startdate, enddate):
        """关键词优化分析：-1-2.4  找出在 2024.04.01 至 2024.04.14 这段时间内，  美国SP广告中 低于 平均ACOS值（替换为第一问结论） 20% - 30% 的 定向投放关键词。将这些关键词信息生成csv文件，里面记录这些关键词的以下信息，CPC，SKU/ASIN， ACOS, Clicks，adgroupid."""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            wherestr = "t.date BETWEEN '{}' AND '{}' AND t.market = '{}' and  t.targeting != '*'".format(
                startdate, enddate, market)
            query1 = """
                                    SELECT
                        SUM(cost)/SUM(sales14d) AS avg_acos,
                        sum(cost) as total_cost,
                        count(distinct keywordId) as keyword_count,
                        sum(clicks) as total_clicks
                        FROM amazon_targeting_reports_sp WHERE date BETWEEN '{}' AND '{}' AND market = '{}' and  targeting != '*'
                                    """.format(startdate, enddate, market)
            df = pd.read_sql(query1, con=conn)
            # return df
            avg_acos = df.loc[0, 'avg_acos']
            # avgacos = avg_acos if avg_acos > 0.2 else 0.2
            acos_pro = str(avg_acos)
            if avg_acos < 0.2:
                avg_acos = 0.2
                acos_pro = "由于平均ACOS小于20%，为了提升优化效果这里优化指标以20%为标准"

            query = """
            with tempacos as (
SELECT
SUM(cost)/SUM(sales14d) AS avg_acos
FROM amazon_targeting_reports_sp t WHERE {}
)
SELECT DISTINCT t.keyword AS Keyword, t.costPerClick AS CPC, t.cost / t.sales14d AS ACOS, t.clicks AS Clicks, t.adGroupId AS AdGroupId, p.advertisedAsin AS ASIN, p.advertisedSku AS SKU
FROM amazon_targeting_reports_sp AS t
JOIN amazon_purchased_product_reports_sp AS p
ON t.keywordId = p.keywordId
WHERE {} AND (t.cost / t.sales14d) BETWEEN ({} * 0.7) AND ({} * 0.8)
            """.format(wherestr, wherestr,avg_acos,avg_acos)
            # 插叙返回结果
            # df = pd.read_sql(query, con=conn)
            # return df

            # 读取数据到DataFrame中
            df_keywords = pd.read_sql(query, con=conn)
            print(df_keywords)
            # 将ACOS列中的数值转换为小数形式
            df_keywords['ACOS'] = df_keywords['ACOS'].apply(lambda x: x / 100.0)
            # 保存到CSV文件中
            output_filename = get_timestamp() + '_targeting_keywords_2_4.csv'
            df_keywords.to_csv(output_filename, index=False, encoding='utf-8-sig')
            s1 = f"关键词数量：{df_keywords['Keyword'].nunique()}"
            s2 = f"总点击量：{df_keywords['Clicks'].sum()}"

            print("Data inserted successfully!")
            print("[ACOS:{}][{},{}]查询已完成，请查看文件： ".format(acos_pro, s1, s2) + output_filename)

            return "[ACOS:{}][{},{}]查询已完成，请查看文件： ".format(acos_pro, s1, s2) + output_filename

        except Exception as error:
            print("1-2.4Error while inserting data:", error)


    def get_sp_searchterm_keyword_info_target_10and20per_csv(self, market, startdate, enddate):
        """关键词优化分析：-1-2.5  找出在 2024.04.01 至 2024.04.14 这段时间内，  美国SP广告中 低于 平均ACOS值（替换为第一问结论） 10% - 20% 的 定向投放关键词。将这些关键词信息生成csv文件，里面记录这些关键词的以下信息，CPC，SKU/ASIN， ACOS, Clicks，adgroupid."""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            wherestr = "t.date BETWEEN '{}' AND '{}' AND t.market = '{}' and  t.targeting != '*'".format(
                startdate, enddate, market)
            query1 = """
                            SELECT
                SUM(cost)/SUM(sales14d) AS avg_acos,
                sum(cost) as total_cost,
                count(distinct keywordId) as keyword_count,
                sum(clicks) as total_clicks
                FROM amazon_targeting_reports_sp WHERE date BETWEEN '{}' AND '{}' AND market = '{}' and  targeting != '*'
                            """.format(startdate, enddate, market)
            df = pd.read_sql(query1, con=conn)
            # return df
            avg_acos = df.loc[0, 'avg_acos']
            # avgacos = avg_acos if avg_acos > 0.2 else 0.2
            acos_pro = str(avg_acos)
            if avg_acos < 0.2:
                avg_acos = 0.2
                acos_pro = "由于平均ACOS小于20%，为了提升优化效果这里优化指标以20%为标准"
            query = """
            with tempacos as (
SELECT
SUM(cost)/SUM(sales14d) AS avg_acos
FROM amazon_targeting_reports_sp t WHERE {}
)
SELECT DISTINCT t.keyword AS Keyword, t.costPerClick AS CPC, t.cost / t.sales14d AS ACOS, t.clicks AS Clicks, t.adGroupId AS AdGroupId, p.advertisedAsin AS ASIN, p.advertisedSku AS SKU
FROM amazon_targeting_reports_sp AS t
JOIN amazon_purchased_product_reports_sp AS p
ON t.keywordId = p.keywordId
WHERE {} AND (t.cost / t.sales14d) BETWEEN ( {}* 0.8) AND ({} * 0.9)
            """.format(wherestr, wherestr,avg_acos,avg_acos)
            # 插叙返回结果
            # df = pd.read_sql(query, con=conn)
            # return df

            # 读取数据到DataFrame中
            df_keywords = pd.read_sql(query, con=conn)
            print(df_keywords)
            # 将ACOS列中的数值转换为小数形式
            df_keywords['ACOS'] = df_keywords['ACOS'].apply(lambda x: x / 100.0)
            # 保存到CSV文件中
            output_filename = get_timestamp() + '_targeting_keywords_2_5.csv'
            df_keywords.to_csv(output_filename, index=False, encoding='utf-8-sig')
            s1 = f"关键词数量：{df_keywords['Keyword'].nunique()}"
            s2 = f"总点击量：{df_keywords['Clicks'].sum()}"

            print("Data inserted successfully!")
            print("[ACOS:{}][{},{}]查询已完成，请查看文件： ".format(acos_pro, s1, s2) + output_filename)
            return "[ACOS:{}][{},{}]查询已完成，请查看文件： ".format(acos_pro, s1, s2) + output_filename

        except Exception as error:
            print("1-2.5Error while inserting data:", error)



    def get_sp__keyword_info_targetacos(self, market, startdate, enddate):
        """关键词优化分析：-1-4.1  在 2024.04.01 至 2024.04.14 这段时间内，美国SP广告的 平均ACOS ，总广告消耗，去重后定向投放关键词 数量 和 总点击量 分别是多少？"""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            query = """
            SELECT SUM(cost)/SUM(sales7d) AS avgacos, COUNT(DISTINCT keyword) AS unique_keywords, SUM(clicks) AS total_clicks
            FROM amazon_targeting_reports_sp WHERE date >= '2024-04-01' AND date <= '2024-04-14' AND market = 'US'
            """.format(startdate, enddate,market)
            df = pd.read_sql(query, con=conn)

            print("Data inserted successfully!")
            # return df
            avgacos = df.loc[0, 'avgacos']
            unique_keywords = df.loc[0, 'unique_keywords']
            total_clicks = df.loc[0, 'total_clicks']
            result_str = "{} SP广告的 平均ACOS 为{}，去重后定向投放关键词 数量为:{} 和 总点击量 分别是{}".format(market,avgacos, unique_keywords, total_clicks)
            return result_str


        except Exception as error:
            print("1-4.1Error while inserting data:", error)


    def get_sp_keyword_target_up10per(self, market, startdate, enddate):
        """关键词优化分析：-1-4.2  在 2024.04.01 至 2024.04.14 这段时间内，  美国SP广告中 高于 平均ACOS值（替换为第一问结论） 10% 的 去重后定向投放关键词 数量 和 总点击量 是多少"""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            wherestr="date BETWEEN '{}' AND '{}' AND market = '{}'".format(startdate, enddate,market)
            query1 = """
                            SELECT
                SUM(cost)/SUM(sales14d) AS avg_acos
                FROM amazon_campaign_reports_sp WHERE date BETWEEN '{}' AND '{}' AND market = '{}'
                            """.format(startdate, enddate, market)
            df = pd.read_sql(query1, con=conn)
            # return df
            avg_acos = df.loc[0, 'avg_acos']
            # avgacos = avg_acos if avg_acos > 0.2 else 0.2
            acos_pro = str(avg_acos)
            if avg_acos < 0.2:
                avg_acos = 0.2
                acos_pro = "由于平均ACOS小于20%，为了提升优化效果这里优化指标以20%为标准"
            query = """
SELECT
count(distinct targeting) as distinctcount,SUM(clicks) as total_clicks
FROM amazon_targeting_reports_sp WHERE {}
and  cost/sales7d> {}*1.1
            """.format(wherestr,avg_acos)
            df = pd.read_sql(query, con=conn)
            print("Data inserted successfully!")
            # return df
            distinctcount = df.loc[0, 'distinctcount']
            total_clicks = df.loc[0, 'total_clicks']
            result_str = "[ACOS:({})]去重后定向投放关键词 数量为:{} 和 总点击量 分别是{}".format( acos_pro,distinctcount, total_clicks)
            return result_str

        except Exception as error:
            print("1-4.2Error while inserting data:", error)


    def get_sp_searchterm_keyword_info_target_up30per_csv(self, market, startdate, enddate):
        """关键词优化分析：-1-4.3  找出在 2024.04.01 至2024.04.14 这段时间内，  美国SP广告中ACOS是21.66%的130% 以上的定向投放关键词。将这些关键词信息生成csv文件，里面记录这些关键词的以下信息：CPC，SKU/ASIN， ACOS,  Clicks，adgroupid."""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            wherestr = "date BETWEEN '{}' AND '{}' AND market = '{}'".format(startdate, enddate, market)
            query1 = """
                            SELECT
                SUM(cost)/SUM(sales14d) AS avg_acos
                FROM amazon_campaign_reports_sp WHERE date BETWEEN '{}' AND '{}' AND market = '{}'
                            """.format(startdate, enddate, market)
            df = pd.read_sql(query1, con=conn)
            # return df
            avg_acos = df.loc[0, 'avg_acos']
            # avgacos = avg_acos if avg_acos > 0.2 else 0.2
            acos_pro = str(avg_acos)
            if avg_acos < 0.2:
                avg_acos = 0.2
                acos_pro = "由于平均ACOS小于20%，为了提升优化效果这里优化指标以20%为标准"
            query = """
SELECT
keywordId,keyword,
COUNT(DISTINCT targeting) AS keyword_count, SUM(clicks) AS total_clicks ,
sum(cost) as totalcost,sum(sales14d) as totalsales
FROM amazon_targeting_reports_sp
WHERE {} AND targeting != '*'
group by keywordId,keyword
having   totalcost/totalsales > {} * 1.3
            """.format(wherestr,avg_acos)
            # 插叙返回结果
            # df = pd.read_sql(query, con=conn)
            # return df
            # 读取数据到DataFrame中
            df_keywords = pd.read_sql(query, con=conn)
            print(df_keywords)

            # 保存到CSV文件中
            output_filename = get_timestamp()+'_targeting_keywords_4_3.csv'
            df_keywords.to_csv(output_filename, index=False, encoding='utf-8-sig')

            # print("Data inserted successfully!")
            # s1 = f"关键词数量：{df_keywords['keywordId'].nunique()}"
            # s2 = f"总点击量：{df_keywords['total_clicks'].sum()}"
            # result_str = "[ACOS:({})]去重后定向投放关键词 数量为:{} 和 总点击量 分别是{}".format(acos_pro, s1, s2)
            # # return result_str
            # df_keywords.to_csv(output_filename, index=False, encoding='utf-8-sig')
            s1 = f"关键词数量：{df_keywords['keyword_count'].sum()}"
            s2 = f"总点击量：{df_keywords['total_clicks'].sum()}"

            print("Data inserted successfully!")

            return "[ACOS:{}][{},{}]查询已完成，请查看文件： ".format(acos_pro, s1, s2) + output_filename



        except Exception as error:
            print("1-4.3Error while inserting data:", error)


    def get_sp_searchterm_keyword_info_target_up20to30per_csv(self, market, startdate, enddate):
        """关键词优化分析：-1-4.4 找出在 2024.04.01 至 2024.04.14 这段时间内，  美国SP广告中 高于 平均ACOS值（替换为第一问结论） 20% - 30% 的 定向投放关键词。将这些关键词信息生成csv文件，里面记录这些关键词的以下信息，CPC，SKU/ASIN， ACOS, Clicks，adgroupid."""

        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            wherestr = "date BETWEEN '{}' AND '{}' AND market = '{}'".format(startdate, enddate, market)
            query1 = """
                            SELECT
                SUM(cost)/SUM(sales14d) AS avg_acos
                FROM amazon_campaign_reports_sp WHERE date BETWEEN '{}' AND '{}' AND market = '{}'
                            """.format(startdate, enddate, market)
            df = pd.read_sql(query1, con=conn)
            # return df
            avg_acos = df.loc[0, 'avg_acos']
            # avgacos = avg_acos if avg_acos > 0.2 else 0.2
            acos_pro = str(avg_acos)
            if avg_acos < 0.2:
                avg_acos = 0.2
                acos_pro = "由于平均ACOS小于20%，为了提升优化效果这里优化指标以20%为标准"
            query = """
    SELECT
    keywordId,keyword,
    COUNT(DISTINCT targeting) AS keyword_count, SUM(clicks) AS total_clicks ,
    sum(cost) as totalcost,sum(sales14d) as totalsales
    FROM amazon_targeting_reports_sp
    WHERE {} AND targeting != '*'
    group by keywordId,keyword
    having   totalcost/totalsales > {} * 1.2 and totalcost/totalsales < {} * 1.3
                """.format(wherestr, avg_acos,avg_acos)
            # 插叙返回结果
            # df = pd.read_sql(query, con=conn)
            # return df
            # 读取数据到DataFrame中
            df_keywords = pd.read_sql(query, con=conn)
            print(df_keywords)

            # 保存到CSV文件中
            output_filename = get_timestamp() + '_targeting_keywords_4_4.csv'
            df_keywords.to_csv(output_filename, index=False, encoding='utf-8-sig')

            # print("Data inserted successfully!")
            # s1 = f"关键词数量：{df_keywords['keywordId'].nunique()}"
            # s2 = f"总点击量：{df_keywords['total_clicks'].sum()}"
            # result_str = "[ACOS:({})]去重后定向投放关键词 数量为:{} 和 总点击量 分别是{}".format(acos_pro, s1, s2)
            # return result_str
            s1 = f"关键词数量：{df_keywords['keyword_count'].sum()}"
            s2 = f"总点击量：{df_keywords['total_clicks'].sum()}"

            print("Data inserted successfully!")
            return "[ACOS:{}][{},{}]查询已完成，请查看文件： ".format(acos_pro, s1, s2) + output_filename

        except Exception as error:
            print("1-4.4 Error while inserting data:", error)




    def get_sp_searchterm_keyword_info_target_up10to20per_csv(self, market, startdate, enddate):
        """关键词优化分析：-1-4.5  找出在 2024.04.01 至 2024.04.14 这段时间内，  美国SP广告中 高于 平均ACOS值（替换为第一问结论） 10% - 20% 的 定向投放关键词。将这些关键词信息生成csv文件，里面记录这些关键词的以下信息，CPC，SKU/ASIN， ACOS, Clicks，adgroupid."""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            wherestr = "date BETWEEN '{}' AND '{}' AND market = '{}'".format(startdate, enddate, market)
            query1 = """
                                SELECT
                    SUM(cost)/SUM(sales14d) AS avg_acos
                    FROM amazon_campaign_reports_sp WHERE date BETWEEN '{}' AND '{}' AND market = '{}'
                                """.format(startdate, enddate, market)
            df = pd.read_sql(query1, con=conn)
            # return df
            avg_acos = df.loc[0, 'avg_acos']
            # avgacos = avg_acos if avg_acos > 0.2 else 0.2
            acos_pro = str(avg_acos)
            if avg_acos < 0.2:
                avg_acos = 0.2
                acos_pro = "由于平均ACOS小于20%，为了提升优化效果这里优化指标以20%为标准"
            query = """
        SELECT
        keywordId,keyword,
        COUNT(DISTINCT targeting) AS keyword_count, SUM(clicks) AS total_clicks ,
        sum(cost) as totalcost,sum(sales14d) as totalsales
        FROM amazon_targeting_reports_sp
        WHERE {} AND targeting != '*'
        group by keywordId,keyword
        having   totalcost/totalsales > {} * 1.1 and totalcost/totalsales < {} * 1.2
                    """.format(wherestr, avg_acos, avg_acos)
            # 插叙返回结果
            # df = pd.read_sql(query, con=conn)
            # return df
            # 读取数据到DataFrame中
            df_keywords = pd.read_sql(query, con=conn)
            print(df_keywords)

            # 保存到CSV文件中
            output_filename = get_timestamp() + '_targeting_keywords_4_5.csv'
            df_keywords.to_csv(output_filename, index=False, encoding='utf-8-sig')

            s1 = f"关键词数量：{df_keywords['keyword_count'].sum()}"
            s2 = f"总点击量：{df_keywords['total_clicks'].sum()}"

            print("Data inserted successfully!")
            return "[ACOS:{}][{},{}]查询已完成，请查看文件： ".format(acos_pro, s1, s2) + output_filename

        except Exception as error:
            print("1-4.5 Error while inserting data:", error)



    def get_sp_product_info(self, market, startdate, enddate):
        """商品优化分析：-2-1.1  在 2024.04.01 至 2024.04.14 这段时间内，美国SP广告的 平均ACOS ，总广告消耗，sku广告数量 和 总点击量 分别是多少？"""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            query = """
            SELECT cost, sales14d, advertisedSku, adGroupId, campaignId, clicks FROM amazon_advertised_product_reports_sp WHERE market='{}' AND date >= '{}' AND date <= '{}'
            """.format(market,startdate, enddate)
            df = pd.read_sql(query, con=conn)
            # 使用Pandas读取数据 df = pd.read_sql(query, connection)
            # 关闭数据库连接 connection.close()
            # 计算总广告费用
            total_ad_cost = df['cost'].sum()
            # 计算总销售额
            total_sales = df['sales14d'].sum()
            # 计算平均ACOS（广告费用/销售额）
            average_acos = total_ad_cost / total_sales if total_sales else 0
            # 计算sku广告总数（通过聚合去重）
            sku_ad_count = df.drop_duplicates(subset=['advertisedSku', 'adGroupId', 'campaignId']).shape[0]
            # 计算广告总点击量
            total_clicks = df['clicks'].sum()
            # return df
            res = "平均ACOS是："+str(average_acos)+",sku广告总数（通过聚合去重） 是："+ str(sku_ad_count)+ ",计算广告总点击量是："+str(total_clicks)
            print("Data inserted successfully!")
            return res

        except Exception as error:
            print("2-1.1 Error while inserting data:", error)


    def get_sp_product_acosblow10(self, market, startdate, enddate):
        """商品优化分析：-2-1.2  在 2024.04.01 至 2024.04.14 这段时间内，  美国SP广告中 低于 平均ACOS值（替换为第一问结论） 10% 的 sku广告数量 数量 和 总点击量 是多少"""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            query1 = """
                            SELECT
                SUM(cost)/SUM(sales14d) AS avg_acos
                FROM amazon_advertised_product_reports_sp WHERE date BETWEEN '{}' AND '{}' AND market = '{}'
                            """.format(startdate, enddate, market)
            df = pd.read_sql(query1, con=conn)
            # return df
            avg_acos = df.loc[0, 'avg_acos']
            # avgacos = avg_acos if avg_acos > 0.2 else 0.2
            acos_pro = str(avg_acos)
            if avg_acos < 0.2:
                avg_acos = 0.2
                acos_pro = "由于平均ACOS小于20%，为了提升优化效果这里优化指标以20%为标准"

            query = """
            with tempacos as (
            SELECT sum(cost)/sum(sales14d) as avgacos FROM amazon_advertised_product_reports_sp WHERE market='{}' AND date >= '{}' AND date <= '{}'
            ),temp2 as (
             SELECT
        advertisedAsin,
        adGroupId,
        adGroupName,
        campaignName,
        SUM( cost ) AS total_cost,
        SUM( sales14d ) AS total_sales14d,
        SUM( clicks ) AS total_clicks
FROM
        amazon_advertised_product_reports_sp
WHERE
        market='{}' AND date >= '{}' AND date <= '{}'
GROUP BY
        adGroupId,
        adGroupName,
        advertisedAsin,
        campaignName
HAVING
        total_sales14d > 0
        AND ( total_cost / total_sales14d ) < {}*0.9)
        select
        count(distinct campaignName,adGroupName,advertisedAsin) as advertisecount,
        sum(total_clicks) as total_clicks
        from temp2
            """.format(market,startdate, enddate,market,startdate, enddate,avg_acos)
            df = pd.read_sql(query, con=conn)
            # return df
            advertisecount = df.loc[0, 'advertisecount']
            total_clicks = df.loc[0, 'total_clicks']
            result_str = "低于 平均ACOS值[ACOS:({})] 10% 的 sku广告数量 :{} 和 总点击量 是:{}".format(acos_pro,advertisecount, total_clicks)
            return result_str


        except Exception as error:
            print("2-1.2 Error while inserting data:", error)


    def get_sp_product_acosblow30(self, market, startdate, enddate):
        """商品优化分析：-2-1.3  找出在 2024.04.01 至 2024.04.14 这段时间内，  美国SP广告中 低于 平均ACOS值（替换为第一问结论） 30% 以上的 sku广告。将这些信息生成csv文件，里面记录这些关键词的以下信息，CPC，SKU/ASIN， ACOS,  Clicks，adgroupid."""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US

            query1 = """
                                        SELECT
                            SUM(cost)/SUM(sales14d) AS avg_acos
                            FROM amazon_advertised_product_reports_sp WHERE date BETWEEN '{}' AND '{}' AND market = '{}'
                                        """.format(startdate, enddate, market)
            df = pd.read_sql(query1, con=conn)
            # return df
            avg_acos = df.loc[0, 'avg_acos']
            # avgacos = avg_acos if avg_acos > 0.2 else 0.2
            acos_pro = str(avg_acos)
            if avg_acos < 0.2:
                avg_acos = 0.2
                acos_pro = "由于平均ACOS小于20%，为了提升优化效果这里优化指标以20%为标准"
            query = """
                        with tempacos as (
                        SELECT sum(cost)/sum(sales14d) as avgacos FROM amazon_advertised_product_reports_sp WHERE market='{}' AND date >= '{}' AND date <= '{}'
                        ),temp2 as (
                         SELECT
                    advertisedAsin,
                    adGroupId,
                    adGroupName,
                    campaignName,
                    SUM( cost ) AS total_cost,
                    SUM( sales14d ) AS total_sales14d,
                    SUM( clicks ) AS total_clicks
            FROM
                    amazon_advertised_product_reports_sp
            WHERE
                    market='{}' AND date >= '{}' AND date <= '{}'
            GROUP BY
                    adGroupId,
                    adGroupName,
                    advertisedAsin,
                    campaignName
            HAVING
                    total_sales14d > 0
                    AND ( total_cost / total_sales14d ) < {}*0.7)
                    select
                    count(distinct campaignName,adGroupName,advertisedAsin) as advertisecount,
                    sum(total_clicks) as total_clicks
                    from temp2
                        """.format(market, startdate, enddate, market, startdate, enddate,avg_acos)
            df = pd.read_sql(query, con=conn)
            output_filename = get_timestamp() + '_prodcut_1_3.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            # return df
            s1 = f"优质广告数量：{df['advertisecount'].sum()}"
            s2 = f"广告的总点击量：{df['total_clicks'].sum()}"
            print("Data inserted successfully!")
            return "[ACOS:{}][{},{}]查询已完成，请查看文件： ".format(acos_pro, s1, s2) + output_filename
            # return "查询已完成，请查看文件： " + output_filename

        except Exception as error:
            print("2-1.3Error while inserting data:", error)



    def get_sp_product_belowacos20to30(self, market, startdate, enddate):
        """商品优化分析：-2-1.4  找出在 2024.04.01 至 2024.04.14 这段时间内，  美国SP广告中 低于 平均ACOS值（替换为第一问结论） 20% - 30% 的 sku广告。将这些信息生成csv文件，里面记录这些关键词的以下信息，CPC，SKU/ASIN， ACOS, Clicks，adgroupid."""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US

            query1 = """
                        SELECT
            SUM(cost)/SUM(sales14d) AS avg_acos
            FROM amazon_advertised_product_reports_sp WHERE date BETWEEN '{}' AND '{}' AND market = '{}'
                        """.format(startdate, enddate, market)
            df = pd.read_sql(query1, con=conn)
            # return df
            avg_acos = df.loc[0, 'avg_acos']
            # avgacos = avg_acos if avg_acos > 0.2 else 0.2
            acos_pro = str(avg_acos)
            if avg_acos < 0.2:
                avg_acos = 0.2
                acos_pro = "由于平均ACOS小于20%，为了提升优化效果这里优化指标以20%为标准"
            query = """
                        with tempacos as (
                        SELECT sum(cost)/sum(sales14d) as avgacos FROM amazon_advertised_product_reports_sp WHERE market='{}' AND date >= '{}' AND date <= '{}'
                        ),temp2 as (
                         SELECT
                    advertisedAsin,
                    adGroupId,
                    adGroupName,
                    campaignName,
                    SUM( cost ) AS total_cost,
                    SUM( sales14d ) AS total_sales14d,
                    SUM( clicks ) AS total_clicks
            FROM
                    amazon_advertised_product_reports_sp
            WHERE
                    market='{}' AND date >= '{}' AND date <= '{}'
            GROUP BY
                    adGroupId,
                    adGroupName,
                    advertisedAsin,
                    campaignName
            HAVING
                    total_sales14d > 0
                    AND ( total_cost / total_sales14d ) < {}*0.8
                    and ( total_cost / total_sales14d ) > {}*0.7)
                    select
                    count(distinct campaignName,adGroupName,advertisedAsin) as advertisecount,
                    sum(total_clicks) as total_clicks
                    from temp2
                        """.format(market, startdate, enddate, market, startdate, enddate,avg_acos,avg_acos)
            df = pd.read_sql(query, con=conn)
            output_filename = get_timestamp() + '_prodcut_1_4.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            # return df
            s1 = f"优质广告数量：{df['advertisecount'].sum()}"
            s2 = f"广告的总点击量：{df['total_clicks'].sum()}"
            print("Data inserted successfully!")
            return "[ACOS:{}][{},{}]查询已完成，请查看文件： ".format(acos_pro, s1, s2) + output_filename
            # return "查询已完成，请查看文件： " + output_filename

        except Exception as error:
            print("2-1.4 Error while inserting data:", error)




    def get_sp_product_belowacos10to20(self, market, startdate, enddate):
        """商品优化分析：-2-1.5  找出在 2024.04.01 至 2024.04.14 这段时间内，  美国SP广告中 低于 平均ACOS值（替换为第一问结论） 10% - 20% 的 sku广告。将这些信息生成csv文件，里面记录这些关键词的以下信息，CPC，SKU/ASIN， ACOS, Clicks，adgroupid."""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US

            query1 = """
                        SELECT
            SUM(cost)/SUM(sales14d) AS avg_acos
            FROM amazon_advertised_product_reports_sp WHERE date BETWEEN '{}' AND '{}' AND market = '{}'
                        """.format(startdate, enddate, market)
            df = pd.read_sql(query1, con=conn)
            # return df
            avg_acos = df.loc[0, 'avg_acos']
            # avgacos = avg_acos if avg_acos > 0.2 else 0.2
            acos_pro = str(avg_acos)
            if avg_acos < 0.2:
                avg_acos = 0.2
                acos_pro = "由于平均ACOS小于20%，为了提升优化效果这里优化指标以20%为标准"
            query = """
                        with tempacos as (
                        SELECT sum(cost)/sum(sales14d) as avgacos FROM amazon_advertised_product_reports_sp WHERE market='{}' AND date >= '{}' AND date <= '{}'
                        ),temp2 as (
                         SELECT
                    advertisedAsin,
                    adGroupId,
                    adGroupName,
                    campaignName,
                    SUM( cost ) AS total_cost,
                    SUM( sales14d ) AS total_sales14d,
                    SUM( clicks ) AS total_clicks
            FROM
                    amazon_advertised_product_reports_sp
            WHERE
                    market='{}' AND date >= '{}' AND date <= '{}'
            GROUP BY
                    adGroupId,
                    adGroupName,
                    advertisedAsin,
                    campaignName
            HAVING
                    total_sales14d > 0
                    AND ( total_cost / total_sales14d ) < {}*0.9
                    and ( total_cost / total_sales14d ) > {}*0.8)
                    select
                    count(distinct campaignName,adGroupName,advertisedAsin) as advertisecount,
                    sum(total_clicks) as total_clicks
                    from temp2
                        """.format(market, startdate, enddate, market, startdate, enddate,avg_acos,avg_acos)
            df = pd.read_sql(query, con=conn)
            output_filename = get_timestamp() + '_prodcut_1_5.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            # return df
            s1 = f"优质广告数量：{df['advertisecount'].sum()}"
            s2 = f"广告的总点击量：{df['total_clicks'].sum()}"
            print("Data inserted successfully!")
            return "[ACOS:{}][{},{}]查询已完成，请查看文件： ".format(acos_pro, s1, s2) + output_filename
            # return "查询已完成，请查看文件： " + output_filename

        except Exception as error:
            print("2-1.5Error while inserting data:", error)


    def get_sp_product_info_2(self, market, startdate, enddate):
        """商品优化分析：-2-2.1  在 2024.04.01 至 2024.04.14 这段时间内，美国SP广告的 平均ACOS ，总广告消耗，sku广告数量 和 总点击量 分别是多少？"""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            # query = """
            # SELECT cost, sales14d, advertisedSku, adGroupId, campaignId, clicks FROM amazon_advertised_product_reports_sp WHERE market='{}' AND date >= '{}' AND date <= '{}'
            # """.format(market,startdate, enddate)
            query="""
            SELECT SUM(cost)/SUM(sales14d) as avgacos,SUM(cost) AS total_cost, SUM(sales14d) AS total_sales14d, COUNT(DISTINCT advertisedSku) AS sku_count, SUM(clicks) AS total_clicks
            FROM amazon_advertised_product_reports_sp
            WHERE market='{}' AND date >= '{}' AND date <= '{}'
            """.format(market,startdate, enddate)
            df = pd.read_sql(query, con=conn)
            # 使用Pandas读取数据 df = pd.read_sql(query, connection)
            # 关闭数据库连接 connection.close()
            # 计算总广告费用
            total_ad_cost = df['total_cost'].sum()
            # 计算总销售额
            total_sales = df['total_sales14d'].sum()
            # 计算平均ACOS（广告费用/销售额）
            average_acos = total_ad_cost / total_sales if total_sales else 0
            sku_count = df['sku_count'].sum()
            # 计算广告总点击量
            total_clicks = df['total_clicks'].sum()
            # return df
            res = "平均ACOS是："+str(average_acos)+",sku广告总数（通过聚合去重） 是："+ str(sku_count)+ ",计算广告总点击量是："+str(total_clicks)+",计算广告总花费是："+str(total_ad_cost)
            print("Data inserted successfully!")
            return res
        except Exception as error:
            print("2-2.1Error while inserting data:", error)

    def get_sp_product_acosup10(self, market, startdate, enddate):
        """商品优化分析：-2-2.2  在 2024.04.01 至 2024.04.14 这段时间内，  美国SP广告中 高于 平均ACOS值（替换为第一问结论） 10% 的 sku广告数量 数量 和 总点击量 是多少"""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            query1 = """
                            SELECT
                SUM(cost)/SUM(sales14d) AS avg_acos
                FROM amazon_advertised_product_reports_sp WHERE date BETWEEN '{}' AND '{}' AND market = '{}'
                            """.format(startdate, enddate, market)
            df = pd.read_sql(query1, con=conn)
            # return df
            avg_acos = df.loc[0, 'avg_acos']
            # avgacos = avg_acos if avg_acos > 0.2 else 0.2
            acos_pro = str(avg_acos)
            if avg_acos < 0.2:
                avg_acos = 0.2
                acos_pro = "由于平均ACOS小于20%，为了提升优化效果这里优化指标以20%为标准"

            query = """
                        with tempacos as (
                        SELECT sum(cost)/sum(sales14d) as avgacos FROM amazon_advertised_product_reports_sp WHERE market='{}' AND date >= '{}' AND date <= '{}'
                        ),temp2 as (
                         SELECT
                    advertisedAsin,
                    adGroupId,
                    adGroupName,
                    campaignName,
                    SUM( cost ) AS total_cost,
                    SUM( sales14d ) AS total_sales14d,
                    SUM( clicks ) AS total_clicks
            FROM
                    amazon_advertised_product_reports_sp
            WHERE
                    market='{}' AND date >= '{}' AND date <= '{}'
            GROUP BY
                    adGroupId,
                    adGroupName,
                    advertisedAsin,
                    campaignName
            HAVING
                    total_sales14d > 0
                    AND ( total_cost / total_sales14d ) > {}*1.1)
                    select
                    count(distinct campaignName,adGroupName,advertisedAsin) as advertisecount,
                    sum(total_clicks) as total_clicks
                    from temp2
                        """.format(market, startdate, enddate, market, startdate, enddate,avg_acos)
            df = pd.read_sql(query, con=conn)
            # return df
            advertisecount = df.loc[0, 'advertisecount']
            total_clicks = df.loc[0, 'total_clicks']
            result_str = "高于 平均ACOS值[{}] 10% 的 sku广告数量 :{} 和 总点击量 是:{}".format(acos_pro,advertisecount, total_clicks)
            return result_str

        except Exception as error:
            print("2-2.2Error while inserting data:", error)


    def get_sp_product_acosup30(self, market, startdate, enddate):
        """商品优化分析：-2-2.3  找出在 2024.04.01 至 2024.04.14 这段时间内，  美国SP广告中 高于 平均ACOS值（替换为第一问结论） 30% 以上的 sku广告。将这些信息生成csv文件，里面记录这些关键词的以下信息，CPC，SKU/ASIN， ACOS,  Clicks，adgroupid."""
        try:
            conn = self.conn

            query1 = """
                                        SELECT
                            SUM(cost)/SUM(sales14d) AS avg_acos
                            FROM amazon_advertised_product_reports_sp WHERE date BETWEEN '{}' AND '{}' AND market = '{}'
                                        """.format(startdate, enddate, market)
            df = pd.read_sql(query1, con=conn)
            # return df
            avg_acos = df.loc[0, 'avg_acos']
            # avgacos = avg_acos if avg_acos > 0.2 else 0.2
            acos_pro = str(avg_acos)
            if avg_acos < 0.2:
                avg_acos = 0.2
                acos_pro = "由于平均ACOS小于20%，为了提升优化效果这里优化指标以20%为标准"
            query = """
                    with tempacos as (
                    SELECT sum(cost)/sum(sales14d) as avgacos FROM amazon_advertised_product_reports_sp WHERE market='{}' AND date >= '{}' AND date <= '{}'
                    ),temp2 as (
                     SELECT
                advertisedAsin,
                adGroupId,
                adGroupName,
                campaignName,
                SUM( cost ) AS total_cost,
                SUM( sales14d ) AS total_sales14d,
                SUM( clicks ) AS total_clicks
        FROM
                amazon_advertised_product_reports_sp
        WHERE
                market='{}' AND date >= '{}' AND date <= '{}'
        GROUP BY
                adGroupId,
                adGroupName,
                advertisedAsin,
                campaignName
        HAVING
                total_sales14d > 0
                AND ( total_cost / total_sales14d ) > {}*1.3)
                select
                *
                from temp2
                    """.format(market, startdate, enddate, market, startdate, enddate,avg_acos)
            df = pd.read_sql(query, con=conn)
            output_filename = get_timestamp() + '_prodcut_2_3.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            # return df
            # print("Data inserted successfully!")
            # return "查询已完成，请查看文件： " + output_filename
            # s1 = f"广告数量：{df['advertisecount'].nunique()}"
            s1 = f"广告数量：{df[['campaignName', 'adGroupName','advertisedAsin']].drop_duplicates().shape[0]}"
            s2 = f"总点击量：{df['total_clicks'].sum()}"

            print("Data inserted successfully!")

            return "[ACOS:{}][{},{}]查询已完成，请查看文件： ".format(acos_pro, s1, s2) + output_filename

        except Exception as error:
            print("2-2.3Error while inserting data:", error)



    def get_sp_product_upacos20to30(self, market, startdate, enddate):
        """商品优化分析：-2-2.4  找出在 2024.04.01 至 2024.04.14 这段时间内，  美国SP广告中 高于 平均ACOS值（替换为第一问结论） 20% - 30% 的 sku广告。将这些信息生成csv文件，里面记录这些关键词的以下信息，CPC，SKU/ASIN， ACOS, Clicks，adgroupid."""
        try:
            conn = self.conn

            query1 = """
                                        SELECT
                            SUM(cost)/SUM(sales14d) AS avg_acos
                            FROM amazon_advertised_product_reports_sp WHERE date BETWEEN '{}' AND '{}' AND market = '{}'
                                        """.format(startdate, enddate, market)
            df = pd.read_sql(query1, con=conn)
            # return df
            avg_acos = df.loc[0, 'avg_acos']
            # avgacos = avg_acos if avg_acos > 0.2 else 0.2
            acos_pro = str(avg_acos)
            if avg_acos < 0.2:
                avg_acos = 0.2
                acos_pro = "由于平均ACOS小于20%，为了提升优化效果这里优化指标以20%为标准"
            query = """
                    with tempacos as (
                    SELECT sum(cost)/sum(sales14d) as avgacos FROM amazon_advertised_product_reports_sp WHERE market='{}' AND date >= '{}' AND date <= '{}'
                    ),temp2 as (
                     SELECT
                advertisedAsin,
                adGroupId,
                adGroupName,
                campaignName,
                SUM( cost ) AS total_cost,
                SUM( sales14d ) AS total_sales14d,
                SUM( clicks ) AS total_clicks
        FROM
                amazon_advertised_product_reports_sp
        WHERE
                market='{}' AND date >= '{}' AND date <= '{}'
        GROUP BY
                adGroupId,
                adGroupName,
                advertisedAsin,
                campaignName
        HAVING
                total_sales14d > 0
                AND ( total_cost / total_sales14d ) > {}*1.2 and ( total_cost / total_sales14d ) < {}*1.3)
                select
                *
                from temp2
                    """.format(market, startdate, enddate, market, startdate, enddate,avg_acos,avg_acos)
            df = pd.read_sql(query, con=conn)
            output_filename = get_timestamp() + '_prodcut_2_4.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            # return df
            # print("Data inserted successfully!")
            # return "查询已完成，请查看文件： " + output_filename
            s1 = f"广告数量：{df[['campaignName', 'adGroupName', 'advertisedAsin']].drop_duplicates().shape[0]}"
            s2 = f"总点击量：{df['total_clicks'].sum()}"

            print("Data inserted successfully!")

            return "[ACOS:{}][{},{}]查询已完成，请查看文件： ".format(acos_pro, s1, s2) + output_filename

        except Exception as error:
            print("2-2.4Error while inserting data:", error)




    def get_sp_product_upacos10to20(self, market, startdate, enddate):
        """商品优化分析：-2-2.5  找出在 2024.04.01 至 2024.04.14 这段时间内，  美国SP广告中 高于 平均ACOS值（替换为第一问结论） 10% - 20% 的 sku广告。将这些信息生成csv文件，里面记录这些关键词的以下信息，CPC，SKU/ASIN， ACOS, Clicks，adgroupid."""
        try:
            conn = self.conn

            query1 = """
                                        SELECT
                            SUM(cost)/SUM(sales14d) AS avg_acos
                            FROM amazon_advertised_product_reports_sp WHERE date BETWEEN '{}' AND '{}' AND market = '{}'
                                        """.format(startdate, enddate, market)
            df = pd.read_sql(query1, con=conn)
            # return df
            avg_acos = df.loc[0, 'avg_acos']
            # avgacos = avg_acos if avg_acos > 0.2 else 0.2
            acos_pro = str(avg_acos)
            if avg_acos < 0.2:
                avg_acos = 0.2
                acos_pro = "由于平均ACOS小于20%，为了提升优化效果这里优化指标以20%为标准"
            query = """
                    with tempacos as (
                    SELECT sum(cost)/sum(sales14d) as avgacos FROM amazon_advertised_product_reports_sp WHERE market='{}' AND date >= '{}' AND date <= '{}'
                    ),temp2 as (
                     SELECT
                advertisedAsin,
                adGroupId,
                adGroupName,
                campaignName,
                SUM( cost ) AS total_cost,
                SUM( sales14d ) AS total_sales14d,
                SUM( clicks ) AS total_clicks
        FROM
                amazon_advertised_product_reports_sp
        WHERE
                market='{}' AND date >= '{}' AND date <= '{}'
        GROUP BY
                adGroupId,
                adGroupName,
                advertisedAsin,
                campaignName
        HAVING
                total_sales14d > 0
                AND ( total_cost / total_sales14d ) > {}*1.1 and ( total_cost / total_sales14d ) < {}*1.2)
                select
                *
                from temp2
                    """.format(market, startdate, enddate, market, startdate, enddate,avg_acos,avg_acos)
            df = pd.read_sql(query, con=conn)
            output_filename = get_timestamp() + '_prodcut_2_5.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            # return df
            # print("Data inserted successfully!")
            # return "查询已完成，请查看文件： " + output_filename
            s1 = f"广告数量：{df[['campaignName', 'adGroupName', 'advertisedAsin']].drop_duplicates().shape[0]}"
            s2 = f"总点击量：{df['total_clicks'].sum()}"

            print("Data inserted successfully!")

            return "[ACOS:{}][{},{}]查询已完成，请查看文件： ".format(acos_pro, s1, s2) + output_filename

        except Exception as error:
            print("2-2.5 Error while inserting data:", error)


    def get_sp_campaign_info(self, market, startdate, enddate):
        """广告计划优化分析：-3-1.1  在 2024.04.01 至 2024.04.14 这段时间内，美国SP广告的 平均ACOS ，总广告消耗， campaign 广告活动数量 和 总点击量 分别是多少？"""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            query = """SELECT SUM(cost)/SUM(sales14d) AS avgacos, SUM(cost) as total_cost,COUNT(DISTINCT campaignId) AS campaign_count, SUM(clicks) AS total_clicks
             FROM amazon_campaign_reports_sp
             WHERE market = '{}' and date >= '{}' AND date <= '{}' """.format(market,startdate, enddate)
            df = pd.read_sql(query, con=conn)
            # return df
            avgacos = df.loc[0, 'avgacos']
            total_cost = df.loc[0, 'total_cost']
            campaign_count = df.loc[0, 'campaign_count']
            total_clicks = df.loc[0, 'total_clicks']
            result_str = "平均ACOS:{} ，总广告消耗:{}， campaign 广告活动数量:{} 和 总点击量是:{}".format(avgacos, total_cost,campaign_count,total_clicks)
            return result_str

        except Exception as error:
            print("3-1.1Error while inserting data:", error)


    def get_sp_campaign_below10(self, market, startdate, enddate):
        """广告计划优化分析：-3-1.2  在 2024.04.01 至 2024.04.14 这段时间内，  美国SP广告中 低于 平均ACOS值（替换为第一问结论） 10% 的  campaign 广告活动数量 数量 和 总点击量 是多少"""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            query = """with tempacos as (
SELECT SUM(cost)/SUM(sales14d) AS avgacos FROM amazon_campaign_reports_sp WHERE market = '{}' and date >= '{}' AND date <= '{}'
)
,temp2 as (
SELECT campaignId, SUM(cost) AS total_cost, SUM(sales14d) AS total_sales, (SUM(cost) / SUM(sales14d))  AS ACOS,
AVG(costPerClick) AS CPC, SUM(clicks) AS Clicks, SUM(spend) AS Spend
FROM amazon_advertised_product_reports_sp
WHERE market = '{}' and date >= '{}' AND date <= '{}'
GROUP BY campaignId HAVING ACOS < (select avgacos from tempacos)* (1 - 0.10) ORDER BY ACOS ASC)
select
count(distinct campaignId) as campaignId_count,sum(Clicks) as total_clicks
from temp2""".format(market,startdate, enddate,market,startdate, enddate)
            df = pd.read_sql(query, con=conn)
            # return df
            campaignId_count = df.loc[0, 'campaignId_count']
            total_clicks = df.loc[0, 'total_clicks']
            result_str = "SP广告中 低于 平均ACOS值（替换为第一问结论） 10% 的  campaign 广告活动数量 :{} 和 总点击量 是:{}".format(campaignId_count, total_clicks)
            return result_str

        except Exception as error:
            print("3-1.2Error while inserting data:", error)

    def spcampaign316(self, market, startdate, enddate):
        """广告计划优化分析：-3-1.6  在 2024.04.01 至 2024.04.14 这段时间内，  美国SP广告中 高于 平均ACOS值（替换为第一问结论） 10% 的  campaign 广告活动数量 数量 和 总点击量 是多少"""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            query = """with tempacos as (
SELECT SUM(cost)/SUM(sales14d) AS avgacos FROM amazon_campaign_reports_sp WHERE market = '{}' and date >= '{}' AND date <= '{}'
)
,temp2 as (
SELECT campaignId, SUM(cost) AS total_cost, SUM(sales14d) AS total_sales, (SUM(cost) / SUM(sales14d))  AS ACOS,
AVG(costPerClick) AS CPC, SUM(clicks) AS Clicks, SUM(spend) AS Spend
FROM amazon_advertised_product_reports_sp
WHERE market = '{}' and date >= '{}' AND date <= '{}'
GROUP BY campaignId HAVING ACOS > (select avgacos from tempacos)* (1 + 0.10) ORDER BY ACOS ASC)
select
count(distinct campaignId) as campaignId_count,sum(Clicks) as total_clicks
from temp2""".format(market, startdate, enddate, market, startdate, enddate)
            df = pd.read_sql(query, con=conn)
            # return df
            campaignId_count = df.loc[0, 'campaignId_count']
            total_clicks = df.loc[0, 'total_clicks']
            result_str = "SP广告中 高于 平均ACOS值（替换为第一问结论） 10% 的  campaign 广告活动数量 :{} 和 总点击量 是:{}".format(
                campaignId_count, total_clicks)
            return result_str

        except Exception as error:
            print("3-1.6Error while inserting data:", error)



    def get_sp_campaign_below30(self, market, startdate, enddate):
        """广告计划优化分析：-3-1.3  找出在 2024.04.01 至 2024.04.14 这段时间内，  美国SP广告中 低于 平均ACOS值（替换为第一问结论） 30% 以上的  campaign 广告活动。将这些信息生成csv文件，里面记录这些关键词的以下信息，CPC， ACOS,  Clicks，campaignid，spend."""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            query = """with tempacos as (
SELECT SUM(cost)/SUM(sales14d) AS avgacos FROM amazon_campaign_reports_sp WHERE market = '{}' and date >= '{}' AND date <= '{}'
)
,temp2 as (
SELECT campaignId, SUM(cost) AS total_cost, SUM(sales14d) AS total_sales, (SUM(cost) / SUM(sales14d))  AS ACOS,
AVG(costPerClick) AS CPC, SUM(clicks) AS Clicks, SUM(spend) AS Spend
FROM amazon_advertised_product_reports_sp
WHERE market = '{}' and date >= '{}' AND date <= '{}'
GROUP BY campaignId HAVING ACOS < (select avgacos from tempacos)* (1 - 0.30) ORDER BY ACOS ASC)
select
count(distinct campaignId) as campaignId_count,sum(Clicks) as total_clicks
from temp2""".format(market,startdate, enddate,market,startdate, enddate)
            df = pd.read_sql(query, con=conn)
            output_filename = get_timestamp() + '_campaign_1_3.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            return "查询已完成，请查看文件： " + output_filename
            # df = pd.read_sql(query, con=conn)
            # return df
        except Exception as error:
            print("3-1.3Error while inserting data:", error)

    def spcampaign317(self, market, startdate, enddate):
        """广告计划优化分析：-3-1.7  找出在 2024.04.01 至 2024.04.14 这段时间内，  美国SP广告中 高于于 平均ACOS值（替换为第一问结论） 30% 以上的  campaign 广告活动。将这些信息生成csv文件，里面记录这些关键词的以下信息，CPC， ACOS,  Clicks，campaignid，spend."""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            query = """with tempacos as (
SELECT SUM(cost)/SUM(sales14d) AS avgacos FROM amazon_campaign_reports_sp WHERE market = '{}' and date >= '{}' AND date <= '{}'
)
,temp2 as (
SELECT campaignId, SUM(cost) AS total_cost, SUM(sales14d) AS total_sales, (SUM(cost) / SUM(sales14d))  AS ACOS,
AVG(costPerClick) AS CPC, SUM(clicks) AS Clicks, SUM(spend) AS Spend
FROM amazon_advertised_product_reports_sp
WHERE market = '{}' and date >= '{}' AND date <= '{}'
GROUP BY campaignId HAVING ACOS > (select avgacos from tempacos)* (1 + 0.30) ORDER BY ACOS ASC)
select
count(distinct campaignId) as campaignId_count,sum(Clicks) as total_clicks
from temp2""".format(market, startdate, enddate, market, startdate, enddate)
            df = pd.read_sql(query, con=conn)
            output_filename = get_timestamp() + '_campaign_1_7.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            return "查询已完成，请查看文件： " + output_filename
            # df = pd.read_sql(query, con=conn)
            # return df
        except Exception as error:
            print("3-1.7Error while inserting data:", error)



    def get_sp_campaign_below20to30(self, market, startdate, enddate):
        """广告计划优化分析：-3-1.4  找出在 2024.04.01 至 2024.04.14 这段时间内，  美国SP广告中 低于 平均ACOS值（替换为第一问结论） 20% - 30% 的 campaign 广告活动。将这些信息生成csv文件，里面记录这些关键词的以下信息，CPC， ACOS,  Clicks，campaignid，spend."""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            query = """with tempacos as (
SELECT SUM(cost)/SUM(sales14d) AS avgacos FROM amazon_campaign_reports_sp WHERE market = '{}' and date >= '{}' AND date <= '{}'
)
,temp2 as (
SELECT campaignId, SUM(cost) AS total_cost, SUM(sales14d) AS total_sales, (SUM(cost) / SUM(sales14d))  AS ACOS,
AVG(costPerClick) AS CPC, SUM(clicks) AS Clicks, SUM(spend) AS Spend
FROM amazon_advertised_product_reports_sp
WHERE market = '{}' and date >= '{}' AND date <= '{}'
GROUP BY campaignId HAVING ACOS < (select avgacos from tempacos)* (1 - 0.20) and ACOS > (select avgacos from tempacos)* (1 - 0.30) ORDER BY ACOS ASC)
select
count(distinct campaignId) as campaignId_count,sum(Clicks) as total_clicks
from temp2""".format(market,startdate, enddate,market,startdate, enddate)
            df = pd.read_sql(query, con=conn)
            output_filename = get_timestamp() + '_campaign_1_4.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            return "查询已完成，请查看文件： " + output_filename
            # df = pd.read_sql(query, con=conn)
            # return df
        except Exception as error:
            print("3-1.4Error while inserting data:", error)

    def apcampaign318(self, market, startdate, enddate):
        """广告计划优化分析：-3-1.8  找出在 2024.04.01 至 2024.04.14 这段时间内，  美国SP广告中 高于 平均ACOS值（替换为第一问结论） 20% - 30% 的 campaign 广告活动。将这些信息生成csv文件，里面记录这些关键词的以下信息，CPC， ACOS,  Clicks，campaignid，spend."""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            query = """with tempacos as (
SELECT SUM(cost)/SUM(sales14d) AS avgacos FROM amazon_campaign_reports_sp WHERE market = '{}' and date >= '{}' AND date <= '{}'
)
,temp2 as (
SELECT campaignId, SUM(cost) AS total_cost, SUM(sales14d) AS total_sales, (SUM(cost) / SUM(sales14d))  AS ACOS,
AVG(costPerClick) AS CPC, SUM(clicks) AS Clicks, SUM(spend) AS Spend
FROM amazon_advertised_product_reports_sp
WHERE market = '{}' and date >= '{}' AND date <= '{}'
GROUP BY campaignId HAVING ACOS < (select avgacos from tempacos)* (1 + 0.30) and ACOS > (select avgacos from tempacos)* (1 + 0.20) ORDER BY ACOS ASC)
select
count(distinct campaignId) as campaignId_count,sum(Clicks) as total_clicks
from temp2""".format(market, startdate, enddate, market, startdate, enddate)
            df = pd.read_sql(query, con=conn)
            output_filename = get_timestamp() + '_campaign_1_8.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            return "查询已完成，请查看文件： " + output_filename
            # df = pd.read_sql(query, con=conn)
            # return df
        except Exception as error:
            print("3-1.8Error while inserting data:", error)





    def get_sp_campaign_below10to20(self, market, startdate, enddate):
        """广告计划优化分析：-3-1.5  找出在 2024.04.01 至 2024.04.14 这段时间内，  美国SP广告中 低于 平均ACOS值（替换为第一问结论） 10% - 20% 的 campaign 广告活动。将这些信息生成csv文件，里面记录这些关键词的以下信息，CPC， ACOS,  Clicks，campaignid，spend."""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            query = """with tempacos as (
SELECT SUM(cost)/SUM(sales14d) AS avgacos FROM amazon_campaign_reports_sp WHERE market = '{}' and date >= '{}' AND date <= '{}'
)
,temp2 as (
SELECT campaignId, SUM(cost) AS total_cost, SUM(sales14d) AS total_sales, (SUM(cost) / SUM(sales14d))  AS ACOS,
AVG(costPerClick) AS CPC, SUM(clicks) AS Clicks, SUM(spend) AS Spend
FROM amazon_advertised_product_reports_sp
WHERE market = '{}' and date >= '{}' AND date <= '{}'
GROUP BY campaignId HAVING ACOS < (select avgacos from tempacos)* (1 - 0.10) and ACOS > (select avgacos from tempacos)* (1 - 0.20) ORDER BY ACOS ASC)
select
count(distinct campaignId) as campaignId_count,sum(Clicks) as total_clicks
from temp2""".format(market,startdate, enddate,market,startdate, enddate)
            df = pd.read_sql(query, con=conn)
            output_filename = get_timestamp() + '_campaign_1_5.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            return "查询已完成，请查看文件： " + output_filename
            # df = pd.read_sql(query, con=conn)
            # return df
        except Exception as error:
            print("3-1.5Error while inserting data:", error)

    def apcampaign319(self, market, startdate, enddate):
        """广告计划优化分析：-3-1.9  找出在 2024.04.01 至 2024.04.14 这段时间内，  美国SP广告中 高于 平均ACOS值（替换为第一问结论） 10% - 20% 的 campaign 广告活动。将这些信息生成csv文件，里面记录这些关键词的以下信息，CPC， ACOS,  Clicks，campaignid，spend."""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            query = """with tempacos as (
SELECT SUM(cost)/SUM(sales14d) AS avgacos FROM amazon_campaign_reports_sp WHERE market = '{}' and date >= '{}' AND date <= '{}'
)
,temp2 as (
SELECT campaignId, SUM(cost) AS total_cost, SUM(sales14d) AS total_sales, (SUM(cost) / SUM(sales14d))  AS ACOS,
AVG(costPerClick) AS CPC, SUM(clicks) AS Clicks, SUM(spend) AS Spend
FROM amazon_advertised_product_reports_sp
WHERE market = '{}' and date >= '{}' AND date <= '{}'
GROUP BY campaignId HAVING ACOS < (select avgacos from tempacos)* (1 + 0.20) and ACOS > (select avgacos from tempacos)* (1 + 0.10) ORDER BY ACOS ASC)
select
count(distinct campaignId) as campaignId_count,sum(Clicks) as total_clicks
from temp2""".format(market, startdate, enddate, market, startdate, enddate)
            df = pd.read_sql(query, con=conn)
            output_filename = get_timestamp() + '_campaign_1_9.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            return "查询已完成，请查看文件： " + output_filename
            # df = pd.read_sql(query, con=conn)
            # return df
        except Exception as error:
            print("3-1.9Error while inserting data:", error)











    def get_sp_campaignplacement_info(self, market, startdate, enddate):
        """广告计划优化分析：-3-2.1  在 2024.04.01 至 2024.04.14 这段时间内，美国SP广告的 平均ACOS ，总广告消耗， campaign 广告活动中 placement 数量 和 总点击量 分别是多少？"""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            query = """SELECT SUM(cost)/SUM(sales14d) AS avgacos,
            SUM(cost) as total_cost,
            COUNT(DISTINCT campaignId) AS campaign_count,
            SUM(clicks) AS total_clicks
            FROM amazon_campaign_reports_sp
             WHERE market = '{}' and date >= '{}' AND date <= '{}' """.format(market,startdate, enddate)
            df = pd.read_sql(query, con=conn)
            # return df
            avgacos = df.loc[0, 'avgacos']
            total_cost = df.loc[0, 'total_cost']
            campaign_count = df.loc[0, 'campaign_count']
            total_clicks = df.loc[0, 'total_clicks']
            result_str = "SP广告的 平均ACOS:{} ，总广告消耗:{}， campaign 广告活动中 placement 数量:{} 和 总点击量是:{}".format(avgacos, total_cost,campaign_count, total_clicks)
            return result_str
        except Exception as error:
            print("3-2.1Error while inserting data:", error)

    def get_sp_campaignplacement_below10(self, market, startdate, enddate):
        """广告计划优化分析：-3-2.2  在 2024.04.01 至 2024.04.14 这段时间内，  美国SP广告中 低于 平均ACOS值（替换为第一问结论） 10% 的  campaign 广告活动中placement 数量 和 总点击量 是多少"""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            query = """with tempacos as (
 SELECT SUM(cost)/SUM(sales14d) AS avgacos
 FROM amazon_campaign_reports_sp WHERE market = '{}' and date >= '{}' AND date <= '{}'
 )
 ,temp2 as (
 SELECT
        campaignId,placementClassification AS placement,AVG( costPerClick ) AS CPC,SUM( cost ) AS spend, SUM( clicks ) AS Clicks,
        (SUM( cost ) / SUM( sales14d )) AS ACOS
FROM amazon_campaign_placement_reports_sp
WHERE market = '{}' and date >= '{}' AND date <= '{}'
GROUP BY campaignId,placement
HAVING ACOS < (select avgacos from tempacos) *0.9 )
select count(distinct campaignId,placement) as placementcount  ,sum(Clicks) as totalclicks from  temp2
""".format(market,startdate, enddate,market,startdate, enddate)
            df = pd.read_sql(query, con=conn)
            # return df
            placementcount = df.loc[0, 'placementcount']
            totalclicks = df.loc[0, 'totalclicks']

            result_str = "低于 平均ACOS值（替换为第一问结论） 10% 的  campaign 广告活动中placement 数量:{} 和 总点击量 是:{}".format(placementcount,totalclicks)
            return result_str
        except Exception as error:
            print("3-2.2Error while inserting data:", error)

    def apcampaign326(self, market, startdate, enddate):
        """广告计划优化分析：-3-2.6  在 2024.04.01 至 2024.04.14 这段时间内，  美国SP广告中 高于 平均ACOS值（替换为第一问结论） 10% 的  campaign 广告活动中placement 数量 和 总点击量 是多少"""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            query = """with tempacos as (
 SELECT SUM(cost)/SUM(sales14d) AS avgacos
 FROM amazon_campaign_reports_sp WHERE market = '{}' and date >= '{}' AND date <= '{}'
 )
 ,temp2 as (
 SELECT
        campaignId,placementClassification AS placement,AVG( costPerClick ) AS CPC,SUM( cost ) AS spend, SUM( clicks ) AS Clicks,
        (SUM( cost ) / SUM( sales14d )) AS ACOS
FROM amazon_campaign_placement_reports_sp
WHERE market = '{}' and date >= '{}' AND date <= '{}'
GROUP BY campaignId,placement
HAVING ACOS > (select avgacos from tempacos) *1.1 )
select count(distinct campaignId,placement) as placementcount  ,sum(Clicks) as totalclicks from  temp2
""".format(market, startdate, enddate, market, startdate, enddate)
            df = pd.read_sql(query, con=conn)
            # return df
            placementcount = df.loc[0, 'placementcount']
            totalclicks = df.loc[0, 'totalclicks']

            result_str = "高于 平均ACOS值（替换为第一问结论） 10% 的  campaign 广告活动中placement 数量:{} 和 总点击量 是:{}".format(
                placementcount, totalclicks)
            return result_str
        except Exception as error:
            print("3-2.6Error while inserting data:", error)








    def get_sp_campaignplacement_below30(self, market, startdate, enddate):
        """广告计划优化分析：-3-2.3  找出在 2024.04.01 至 2024.04.14 这段时间内，  美国SP广告中 低于 平均ACOS值（替换为第一问结论） 30% 以上的  campaign 广告活动中 placement。将这些信息生成csv文件，里面记录这些关键词的以下信息，CPC， ACOS,  Clicks，campaignid，spend， placement."""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            query = """with tempacos as (
             SELECT SUM(cost)/SUM(sales14d) AS avgacos
             FROM amazon_campaign_reports_sp WHERE market = '{}' and date >= '{}' AND date <= '{}'
             )
             ,temp2 as (
             SELECT
                    campaignId,placementClassification AS placement,AVG( costPerClick ) AS CPC,SUM( cost ) AS spend, SUM( clicks ) AS Clicks,
                    (SUM( cost ) / SUM( sales14d )) AS ACOS
            FROM amazon_campaign_placement_reports_sp
            WHERE market = '{}' and date >= '{}' AND date <= '{}'
            GROUP BY campaignId,placement
            HAVING ACOS < (select avgacos from tempacos) *0.7 )
            select count(distinct campaignId,placement),sum(Clicks) from  temp2
            """.format(market, startdate, enddate, market, startdate, enddate)
            df = pd.read_sql(query, con=conn)
            output_filename = get_timestamp() + '_campaign_2_3.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            return "查询已完成，请查看文件： " + output_filename
            # df = pd.read_sql(query, con=conn)
            # return df
        except Exception as error:
            print("3-2.3Error while inserting data:", error)


    def apcampaign327(self, market, startdate, enddate):
        """广告计划优化分析：-3-2.7  找出在 2024.04.01 至 2024.04.14 这段时间内，  美国SP广告中 高于 平均ACOS值（替换为第一问结论） 30% 以上的  campaign 广告活动中 placement。将这些信息生成csv文件，里面记录这些关键词的以下信息，CPC， ACOS,  Clicks，campaignid，spend， placement."""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            query = """with tempacos as (
             SELECT SUM(cost)/SUM(sales14d) AS avgacos
             FROM amazon_campaign_reports_sp WHERE market = '{}' and date >= '{}' AND date <= '{}'
             )
             ,temp2 as (
             SELECT
                    campaignId,placementClassification AS placement,AVG( costPerClick ) AS CPC,SUM( cost ) AS spend, SUM( clicks ) AS Clicks,
                    (SUM( cost ) / SUM( sales14d )) AS ACOS
            FROM amazon_campaign_placement_reports_sp
            WHERE market = '{}' and date >= '{}' AND date <= '{}'
            GROUP BY campaignId,placement
            HAVING ACOS > (select avgacos from tempacos) *1.3 )
            select count(distinct campaignId,placement),sum(Clicks) from  temp2
            """.format(market, startdate, enddate, market, startdate, enddate)
            df = pd.read_sql(query, con=conn)
            output_filename = get_timestamp() + '_campaign_2_7.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            return "查询已完成，请查看文件： " + output_filename
            # df = pd.read_sql(query, con=conn)
            # return df
        except Exception as error:
            print("3-2.7Error while inserting data:", error)



    def get_sp_campaignplacement_below20to30(self, market, startdate, enddate):
        """广告计划优化分析：-3-2.4 找出在 2024.04.01 至 2024.04.14 这段时间内，  美国SP广告中 低于 平均ACOS值（替换为第一问结论） 20% - 30% 的 campaign 广告活动中 placement。将这些信息生成csv文件，里面记录这些关键词的以下信息，CPC， ACOS,  Clicks，campaignid，spend， placement."""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            query = """with tempacos as (
             SELECT SUM(cost)/SUM(sales14d) AS avgacos
             FROM amazon_campaign_reports_sp WHERE market = '{}' and date >= '{}' AND date <= '{}'
             )
             ,temp2 as (
             SELECT
                    campaignId,placementClassification AS placement,AVG( costPerClick ) AS CPC,SUM( cost ) AS spend, SUM( clicks ) AS Clicks,
                    (SUM( cost ) / SUM( sales14d )) AS ACOS
            FROM amazon_campaign_placement_reports_sp
            WHERE market = '{}' and date >= '{}' AND date <= '{}'
            GROUP BY campaignId,placement
            HAVING ACOS < (select avgacos from tempacos) *0.8 and ACOS > (select avgacos from tempacos) *0.7 )
            select count(distinct campaignId,placement),sum(Clicks) from  temp2
            """.format(market, startdate, enddate, market, startdate, enddate)
            df = pd.read_sql(query, con=conn)
            output_filename = get_timestamp() + '_campaign_2_4.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            return "查询已完成，请查看文件： " + output_filename
            # df = pd.read_sql(query, con=conn)
            # return df
        except Exception as error:
            print("3-2.4Error while inserting data:", error)

    def apcampaign328(self, market, startdate, enddate):
        """广告计划优化分析：-3-2.8 找出在 2024.04.01 至 2024.04.14 这段时间内，  美国SP广告中 高于 平均ACOS值（替换为第一问结论） 20% - 30% 的 campaign 广告活动中 placement。将这些信息生成csv文件，里面记录这些关键词的以下信息，CPC， ACOS,  Clicks，campaignid，spend， placement."""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            query = """with tempacos as (
             SELECT SUM(cost)/SUM(sales14d) AS avgacos
             FROM amazon_campaign_reports_sp WHERE market = '{}' and date >= '{}' AND date <= '{}'
             )
             ,temp2 as (
             SELECT
                    campaignId,placementClassification AS placement,AVG( costPerClick ) AS CPC,SUM( cost ) AS spend, SUM( clicks ) AS Clicks,
                    (SUM( cost ) / SUM( sales14d )) AS ACOS
            FROM amazon_campaign_placement_reports_sp
            WHERE market = '{}' and date >= '{}' AND date <= '{}'
            GROUP BY campaignId,placement
            HAVING ACOS < (select avgacos from tempacos) *1.3 and ACOS > (select avgacos from tempacos) *1.2 )
            select count(distinct campaignId,placement),sum(Clicks) from  temp2
            """.format(market, startdate, enddate, market, startdate, enddate)
            df = pd.read_sql(query, con=conn)
            output_filename = get_timestamp() + '_campaign_2_8.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            return "查询已完成，请查看文件： " + output_filename
            # df = pd.read_sql(query, con=conn)
            # return df
        except Exception as error:
            print("3-2.8Error while inserting data:", error)


    def get_sp_campaignplacement_below10to20(self, market, startdate, enddate):
        """广告计划优化分析：-3-2.5 找出在 2024.04.01 至 2024.04.14 这段时间内，  美国SP广告中 低于 平均ACOS值（替换为第一问结论） 10% - 20% 的 campaign 广告活动中的 placement。将这些信息生成csv文件，里面记录这些关键词的以下信息，CPC， ACOS,  Clicks，campaignid，spend， placement."""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            query = """with tempacos as (
             SELECT SUM(cost)/SUM(sales14d) AS avgacos
             FROM amazon_campaign_reports_sp WHERE market = '{}' and date >= '{}' AND date <= '{}'
             )
             ,temp2 as (
             SELECT
                    campaignId,placementClassification AS placement,AVG( costPerClick ) AS CPC,SUM( cost ) AS spend, SUM( clicks ) AS Clicks,
                    (SUM( cost ) / SUM( sales14d )) AS ACOS
            FROM amazon_campaign_placement_reports_sp
            WHERE market = '{}' and date >= '{}' AND date <= '{}'
            GROUP BY campaignId,placement
            HAVING ACOS < (select avgacos from tempacos) *0.9 and ACOS > (select avgacos from tempacos) *0.8 )
            select count(distinct campaignId,placement),sum(Clicks) from  temp2
            """.format(market, startdate, enddate, market, startdate, enddate)
            df = pd.read_sql(query, con=conn)
            output_filename = get_timestamp() + '_campaign_2_5.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            return "查询已完成，请查看文件： " + output_filename
            # df = pd.read_sql(query, con=conn)
            # return df
        except Exception as error:
            print("3-2.5Error while inserting data:", error)

    def apcampaign329(self, market, startdate, enddate):
        """广告计划优化分析：-3-2.9 找出在 2024.04.01 至 2024.04.14 这段时间内，  美国SP广告中 高于 平均ACOS值（替换为第一问结论） 10% - 20% 的 campaign 广告活动中的 placement。将这些信息生成csv文件，里面记录这些关键词的以下信息，CPC， ACOS,  Clicks，campaignid，spend， placement."""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            query = """with tempacos as (
             SELECT SUM(cost)/SUM(sales14d) AS avgacos
             FROM amazon_campaign_reports_sp WHERE market = '{}' and date >= '{}' AND date <= '{}'
             )
             ,temp2 as (
             SELECT
                    campaignId,placementClassification AS placement,AVG( costPerClick ) AS CPC,SUM( cost ) AS spend, SUM( clicks ) AS Clicks,
                    (SUM( cost ) / SUM( sales14d )) AS ACOS
            FROM amazon_campaign_placement_reports_sp
            WHERE market = '{}' and date >= '{}' AND date <= '{}'
            GROUP BY campaignId,placement
            HAVING ACOS < (select avgacos from tempacos) *1.2 and ACOS > (select avgacos from tempacos) *1.1 )
            select count(distinct campaignId,placement),sum(Clicks) from  temp2
            """.format(market, startdate, enddate, market, startdate, enddate)
            df = pd.read_sql(query, con=conn)
            output_filename = get_timestamp() + '_campaign_2_9.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            return "查询已完成，请查看文件： " + output_filename
            # df = pd.read_sql(query, con=conn)
            # return df
        except Exception as error:
            print("3-2.9Error while inserting data:", error)


    # 最后一部分
    def get_sp_adgroup_info(self, market, startdate, enddate):
        """广告计划优化分析：-3-3.1"""
        pass
    def adcampaigm331(self, market, startdate, enddate):
        """广告计划优化分析：-3-3.2 在 2024.04.01 至 2024.04.14 这段时间内，美国SP广告中 低于 平均ACOS值（20.38%） 10% 的  广告 adgroup 数量 数量 和 总点击量 是多少"""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            query = """	SELECT
	count(distinct adGroupId) as adgroupcount,
	sum(total_clicks) as totalclicks
	FROM (
		SELECT
        adGroupId,
        campaignName,
        adGroupName,
        sum( cost ) / sum( sales14d ) AS acos,
        sum( clicks ) AS total_clicks
FROM
        amazon_advertised_product_reports_sp
WHERE
        date BETWEEN '{}'
        AND '{}'
        AND market = '{}'
GROUP BY
        campaignName,
        adGroupName,
adGroupId
HAVING
        acos < ( 20.38 * 0.9 ) / 100) A
        """.format(startdate, enddate, market)
            df = pd.read_sql(query, con=conn)
            # return df
            adgroupcount = df.loc[0, 'adgroupcount']
            totalclicks = df.loc[0, 'totalclicks']

            result_str = "低于 ACOS(0.2) 10% 的  广告 adgroup 数量为：{} 和 总点击量 是：{}".format(adgroupcount,totalclicks)
            return result_str
        except Exception as error:
            print("3-3.2Error while inserting data:", error)

    def adcampaigm336(self, market, startdate, enddate):
        """广告计划优化分析：-3-3.6 在 2024.04.01 至 2024.04.14 这段时间内，美国SP广告中 高于 平均ACOS值（20.38%） 10% 的  广告 adgroup 数量 数量 和 总点击量 是多少"""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            query = """	SELECT
    count(distinct adGroupId) as adgroupcount,
    sum(total_clicks) as totalclicks
    FROM (
        SELECT
        adGroupId,
        campaignName,
        adGroupName,
        sum( cost ) / sum( sales14d ) AS acos,
        sum( clicks ) AS total_clicks
FROM
        amazon_advertised_product_reports_sp
WHERE
        date BETWEEN '{}'
        AND '{}'
        AND market = '{}'
GROUP BY
        campaignName,
        adGroupName,
adGroupId
HAVING
        acos > ( 20.38 * 1.1 ) / 100) A
        """.format(startdate, enddate, market)
            df = pd.read_sql(query, con=conn)
            # return df
            adgroupcount = df.loc[0, 'adgroupcount']
            totalclicks = df.loc[0, 'totalclicks']

            result_str = "高于 ACOS(0.2) 10% 的  广告 adgroup 数量为：{} 和 总点击量 是：{}".format(adgroupcount, totalclicks)
            return result_str
        except Exception as error:
            print("3-3.6Error while inserting data:", error)




    def adcampaigm333(self, market, startdate, enddate):
        """广告计划优化分析：-3-3.3 找出在 2024.04.01 至 2024.04.14 这段时间内，  美国SP广告中 低于 平均ACOS值（20.38%） 30% 以上的  广告 adgroup。将这些信息生成csv文件，里面记录这些关键词的以下信息，CPC， ACOS,  Clicks，adgroupid，spend."""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            query = """
            SELECT adGroupId, campaignName,adGroupName,SUM(clicks) AS Clicks,
             SUM(cost) AS Spend, CASE WHEN SUM(sales14d) = 0 THEN NULL ELSE SUM(cost) / SUM(sales14d) END AS ACOS,
             CASE WHEN SUM(clicks) = 0 THEN NULL ELSE SUM(cost) / SUM(clicks) END AS CPC FROM
             amazon_advertised_product_reports_sp
             WHERE date >= '{}' AND date <= '{}' AND market='{}' GROUP BY  campaignName,adGroupName,adGroupId HAVING ACOS < 0.14266 AND ACOS > 0
            """.format(startdate, enddate, market)
            df = pd.read_sql(query, con=conn)
            output_filename = get_timestamp() + '_campaign_3_3.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            return "查询已完成，请查看文件： " + output_filename
            # df = pd.read_sql(query, con=conn)
            # return df
        except Exception as error:
            print("3-3.3Error while inserting data:", error)


    def adcampaigm337(self, market, startdate, enddate):
        """广告计划优化分析：-3-3.7 找出在 2024.04.01 至 2024.04.14 这段时间内，  美国SP广告中 高于 平均ACOS值（20.38%） 30% 以上的  广告 adgroup。将这些信息生成csv文件，里面记录这些关键词的以下信息，CPC， ACOS,  Clicks，adgroupid，spend."""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            query = """
            SELECT adGroupId, campaignName,adGroupName,SUM(clicks) AS Clicks,
             SUM(cost) AS Spend, CASE WHEN SUM(sales14d) = 0 THEN NULL ELSE SUM(cost) / SUM(sales14d) END AS ACOS,
             CASE WHEN SUM(clicks) = 0 THEN NULL ELSE SUM(cost) / SUM(clicks) END AS CPC FROM
             amazon_advertised_product_reports_sp
             WHERE date >= '{}' AND date <= '{}' AND market='{}' GROUP BY  campaignName,adGroupName,adGroupId HAVING ACOS > 0.2038*1.3 AND ACOS > 0
            """.format(startdate, enddate, market)
            df = pd.read_sql(query, con=conn)
            output_filename = get_timestamp() + '_campaign_3_7.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            return "查询已完成，请查看文件： " + output_filename
            # df = pd.read_sql(query, con=conn)
            # return df
        except Exception as error:
            print("3-3.7Error while inserting data:", error)



    def adcampaigm334(self, market, startdate, enddate):
        """广告计划优化分析：-3-3.4 """
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            query = """
            SELECT
        adGroupId,
        campaignName,
        adGroupName,
        SUM( clicks ) AS Clicks,
        SUM( cost ) AS Spend,
CASE

                WHEN SUM( sales14d ) = 0 THEN
                NULL ELSE SUM( cost ) / SUM( sales14d )
        END AS ACOS,
CASE

                WHEN SUM( clicks ) = 0 THEN
                NULL ELSE SUM( cost ) / SUM( clicks )
        END AS CPC
FROM
        amazon_advertised_product_reports_sp
WHERE
        date >= '{}'
        AND date <= '{}'
        AND market = '{}'
GROUP BY
        campaignName,
        adGroupName,
        adGroupId
HAVING
        ACOS < 0.163 AND ACOS > 0.142
            """.format(startdate, enddate, market)
            df = pd.read_sql(query, con=conn)
            output_filename = get_timestamp() + '_campaign_3_4.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            return "查询已完成，请查看文件： " + output_filename
            # df = pd.read_sql(query, con=conn)
            # return df
        except Exception as error:
            print("3-3.4Error while inserting data:", error)

    def adcampaigm338(self, market, startdate, enddate):
        """广告计划优化分析：-3-3.8 P广告中 高于（20-30%）ACOS 介于20.38%） 20% - 30%之间的 广告 adgroup 。将这些信息"""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            query = """
            SELECT
        adGroupId,
        campaignName,
        adGroupName,
        SUM( clicks ) AS Clicks,
        SUM( cost ) AS Spend,
CASE

                WHEN SUM( sales14d ) = 0 THEN
                NULL ELSE SUM( cost ) / SUM( sales14d )
        END AS ACOS,
CASE

                WHEN SUM( clicks ) = 0 THEN
                NULL ELSE SUM( cost ) / SUM( clicks )
        END AS CPC
FROM
        amazon_advertised_product_reports_sp
WHERE
        date >= '{}'
        AND date <= '{}'
        AND market = '{}'
GROUP BY
        campaignName,
        adGroupName,
        adGroupId
HAVING
        ACOS < 0.2038*1.3 AND ACOS > 0.2038*1.2
            """.format(startdate, enddate, market)
            df = pd.read_sql(query, con=conn)
            output_filename = get_timestamp() + '_campaign_3_8.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            return "查询已完成，请查看文件： " + output_filename
            # df = pd.read_sql(query, con=conn)
            # return df
        except Exception as error:
            print("3-3.8Error while inserting data:", error)




    def adcampaigm335(self, market, startdate, enddate):
        """广告计划优化分析：-3-3.5 找出在 2024.04.01 至 2024.04.14 这段时间内，  美国SP广告中 低于 平均ACOS值（20.38%） 10% - 20% 的 广告 adgroup。将这些信息生成csv文件，文件命名优质_5.csv，里面记录这些关键词的以下信息，CPC， ACOS,  Clicks，adgroupid，spend."""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            query = """
            SELECT adGroupId, campaignName,adGroupName, SUM(clicks) AS Clicks, SUM(cost) AS Spend,
             CASE WHEN SUM(sales14d) = 0 THEN NULL ELSE SUM(cost) / SUM(sales14d) END AS ACOS,
             CASE WHEN SUM(clicks) = 0 THEN NULL ELSE SUM(cost) / SUM(clicks) END AS CPC
             FROM amazon_advertised_product_reports_sp
             WHERE date >= '{}' AND date <= '{}' AND market='{}'
             GROUP BY campaignName,adGroupName,adGroupId HAVING ACOS < 0.1834 AND ACOS > 0.16304
            """.format(startdate, enddate, market)
            df = pd.read_sql(query, con=conn)
            output_filename = get_timestamp() + '_campaign_3_5.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            return "查询已完成，请查看文件： " + output_filename
            # df = pd.read_sql(query, con=conn)
            # return df
        except Exception as error:
            print("3-3.5Error while inserting data:", error)



    def adcampaigm339(self, market, startdate, enddate):
        """广告计划优化分析：-3-3.9 找出在 2024.04.01 至 2024.04.14 这段时间内，  美国SP广告中 高于 平均ACOS值（20.38%） 10% - 20% 的 广告 adgroup。将这些信息生成csv文件，文件命名优质_5.csv，里面记录这些关键词的以下信息，CPC， ACOS,  Clicks，adgroupid，spend."""
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            query = """
            SELECT adGroupId, campaignName,adGroupName, SUM(clicks) AS Clicks, SUM(cost) AS Spend,
             CASE WHEN SUM(sales14d) = 0 THEN NULL ELSE SUM(cost) / SUM(sales14d) END AS ACOS,
             CASE WHEN SUM(clicks) = 0 THEN NULL ELSE SUM(cost) / SUM(clicks) END AS CPC
             FROM amazon_advertised_product_reports_sp
             WHERE date >= '{}' AND date <= '{}' AND market='{}'
             GROUP BY campaignName,adGroupName,adGroupId HAVING ACOS < 0.2038*1.2 AND ACOS > 0.2038*1.1
            """.format(startdate, enddate, market)
            df = pd.read_sql(query, con=conn)
            output_filename = get_timestamp() + '_campaign_3_9.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            return "查询已完成，请查看文件： " + output_filename
            # df = pd.read_sql(query, con=conn)
            # return df
        except Exception as error:
            print("3-3.9Error while inserting data:", error)

#



