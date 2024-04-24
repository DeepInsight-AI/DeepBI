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

class AmazonMysqlNewSBRagUitl:

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


    # SP案例
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

    #  SB
    def get_sb_keyword_111(self, startdate , enddate):
        """关键词优化分析：-1-1.1  1.找出西班牙的SB广告的优质关键词"""
        try:
            conn = self.conn

            query = """
            SELECT
              campaignName,
              adGroupName,
        adGroupId,
        keywordBid,
        keywordText,
        (SUM( cost )/SUM( clicks )) AS CPC,
        SUM( cost ) AS totalCost,
        SUM( sales ) AS totalSales,
        SUM( clicks ) AS totalClicks,
        ( SUM( cost ) / SUM( sales ) ) AS ACOS
FROM
        amazon_targeting_reports_sb
WHERE
        market = 'ES'
        AND date BETWEEN '{}'
        AND '{}'
GROUP BY
        adGroupId,
        keywordBid,
        keywordText,
                                campaignName,
                                adGroupName
HAVING
        ( SUM( cost ) / SUM( sales ) ) < 0.2
            """.format(startdate, enddate)
            df = pd.read_sql(query, con=conn)
            # return df
            output_filename = get_timestamp() + '_new_SB_keyword_1_1.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            s1 = f"西班牙的SB广告的优质关键词数量：{df[['adGroupId','keywordBid','keywordText','campaignName','adGroupName']].drop_duplicates().shape[0]}"
            result_str = "[{}]生成文件为：{}".format(s1,output_filename)
            print("1.1.1 Data inserted successfully!")
            return result_str
        except Exception as error:
            print("1.1.1 Error while inserting data:", error)

    def get_sb_keyword_121(self, startdate, enddate):
        """关键词优化分析：-1-2.1  1.找出法国的SB广告的优质关键词"""
        try:
            conn = self.conn

            query = """
            SELECT
              campaignName,
              adGroupName,
        adGroupId,
        keywordBid,
        keywordText,
        (SUM( cost )/SUM( clicks )) AS CPC,
        SUM( cost ) AS totalCost,
        SUM( sales ) AS totalSales,
        SUM( clicks ) AS totalClicks,
        ( SUM( cost ) / SUM( sales ) ) AS ACOS
FROM
        amazon_targeting_reports_sb
WHERE
        market =  'FR'
        AND date BETWEEN '{}'
        AND '{}'
GROUP BY
        adGroupId,
        keywordBid,
        keywordText,
                                campaignName,
                                adGroupName
HAVING
        ( SUM( cost ) / SUM( sales ) ) < 0.2
            """.format(startdate, enddate)
            df = pd.read_sql(query, con=conn)
            # return df
            output_filename = get_timestamp() + '_new_SB_keyword_2_1.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            s1 = f"法国的SB广告的优质关键词数量：{df[['adGroupId','keywordBid','keywordText','campaignName','adGroupName']].drop_duplicates().shape[0]}"
            result_str = "[{}]生成文件为：{}".format(s1,output_filename)
            print("1.2.1 Data inserted successfully!")
            return result_str
        except Exception as error:
            print("1.2.1 Error while inserting data:", error)



    def get_sb_keyword_122(self, market, startdate, enddate):
        """关键词优化分析：-1-2.2  2.在 2024.04.01 至 2024.04.14 这段时间内，  法国SB广告中ACOS值在14%到16% 的 定向投放关键词，将这些关键词信息生成csv文件，里面记录这些关键词的以下信息，CPC，SKU/ASIN， ACOS, Clicks，adgroupid.(ACOS20%的-20%-30%)"""
        try:
            conn = self.conn

            query = """
            SELECT
              campaignName,
              adGroupName,
        adGroupId,
        keywordBid,
        keywordText,
        (SUM( cost )/SUM( clicks )) AS CPC,
        SUM( cost ) AS totalCost,
        SUM( sales ) AS totalSales,
        SUM( clicks ) AS totalClicks,
        ( SUM( cost ) / SUM( sales ) ) AS ACOS
FROM
        amazon_targeting_reports_sb
WHERE
        market = '{}'
        AND date BETWEEN '{}'
        AND '{}'
GROUP BY
        adGroupId,
        keywordBid,
        keywordText,
                                campaignName,
                                adGroupName
HAVING
        ( SUM( cost ) / SUM( sales ) ) < 0.16 and ( SUM( cost ) / SUM( sales ) ) > 0.14
            """.format(market,startdate, enddate)
            df = pd.read_sql(query, con=conn)
            # return df
            output_filename = get_timestamp() + '_SB_keyword_2_2.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            s1 = f"关键词数量：{df[['campaignName','adGroupName','keywordBid','keywordText']].drop_duplicates().shape[0]}"
            result_str = "[ACOS取值为0.2][{}]生成文件为：{}".format(s1, output_filename)
            print("1.2.2 Data inserted successfully!")
            return result_str
        except Exception as error:
            print("1.2.2 Error while inserting data:", error)



    def get_sb_keyword_123(self, market, startdate, enddate):
        """关键词优化分析：-1-2.3  3 在 2024.04.01 至 2024.04.14 这段时间内，  法国SB广告中ACOS值在16%到18%的定向投放关键词。将这些关键词信息生成csv文件，里面记录这些关键词的以下信息，CPC，SKU/ASIN， ACOS, Clicks，adgroupid.(ACOS20%的-10%-20%)"""
        try:
            conn = self.conn

            query = """
            SELECT
              campaignName,
              adGroupName,
        adGroupId,
        keywordBid,
        keywordText,
        (SUM( cost )/SUM( clicks )) AS CPC,
        SUM( cost ) AS totalCost,
        SUM( sales ) AS totalSales,
        SUM( clicks ) AS totalClicks,
        ( SUM( cost ) / SUM( sales ) ) AS ACOS
FROM
        amazon_targeting_reports_sb
WHERE
        market = '{}'
        AND date BETWEEN '{}'
        AND '{}'
GROUP BY
        adGroupId,
        keywordBid,
        keywordText,
                                campaignName,
                                adGroupName
HAVING
        ( SUM( cost ) / SUM( sales ) ) < 0.18 and ( SUM( cost ) / SUM( sales ) ) > 0.16
            """.format(market,startdate, enddate)
            df = pd.read_sql(query, con=conn)
            # return df
            output_filename = get_timestamp() + '_SB_keyword_2_3.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            s1 = f"关键词数量：{df[['campaignName','adGroupName','keywordBid','keywordText']].drop_duplicates().shape[0]}"
            result_str = "[ACOS取值为0.2][{}]生成文件为：{}".format(s1, output_filename)
            print("1.2.3 Data inserted successfully!")
            return result_str
        except Exception as error:
            print("1.2.3 Error while inserting data:", error)


    def get_sb_keyword_131(self, startdate, enddate):
        """关键词优化分析：-1-3.1  1.找出美国的SB广告的优质关键词"""
        try:
            conn = self.conn

            query = """
 SELECT
              campaignName,
              adGroupName,
        adGroupId,
        keywordBid,
        keywordText,
        (SUM( cost )/SUM( clicks )) AS CPC,
        SUM( cost ) AS totalCost,
        SUM( sales ) AS totalSales,
        SUM( clicks ) AS totalClicks,
        ( SUM( cost ) / SUM( sales ) ) AS ACOS
FROM
        amazon_targeting_reports_sb
WHERE
        market = 'US'
        AND date BETWEEN '{}'
        AND '{}'
GROUP BY
        adGroupId,
        keywordBid,
        keywordText,
        campaignName,
        adGroupName
HAVING
        ( SUM( cost ) / SUM( sales ) ) < (
				select case when us_avg_acos <0.2 then 0.2 else us_avg_acos end from (
				select
				sum(cost)/sum(sales) as us_avg_acos
				from
				amazon_targeting_reports_sb
				where market='US' and date BETWEEN '{}'
        AND '{}' ) usacos
				)
            """.format(startdate, enddate,startdate, enddate)
            df = pd.read_sql(query, con=conn)
            # return df
            output_filename = get_timestamp() + '_new_SB_keyword_3_1.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            s1 = f"美国的SB广告的优质关键词数量：{df[['adGroupId','keywordBid','keywordText','campaignName','adGroupName']].drop_duplicates().shape[0]}"
            result_str = "[{}]生成文件为：{}".format(s1,output_filename)
            print("1.3.1 Data inserted successfully!")
            return result_str
        except Exception as error:
            print("1.3.1 Error while inserting data:", error)




    def get_sb_keyword_132(self, market, startdate, enddate):
        """关键词优化分析：-1-3.2  在 2024.04.01 至 2024.04.14 这段时间内，  法国SP广告中ACOS值在24%到26% 区间的 定向投放关键词。将这些关键词信息生成csv文件，里面记录这些关键词的以下信息，CPC，SKU/ASIN， ACOS, Clicks，adgroupid.（ACOS20%的+20%+30%)"""
        try:
            conn = self.conn

            query = """
            SELECT
              campaignName,
              adGroupName,
        adGroupId,
        keywordBid,
        keywordText,
        (SUM( cost )/SUM( clicks )) AS CPC,
        SUM( cost ) AS totalCost,
        SUM( sales ) AS totalSales,
        SUM( clicks ) AS totalClicks,
        ( SUM( cost ) / SUM( sales ) ) AS ACOS
FROM
        amazon_targeting_reports_sb
WHERE
        market = '{}'
        AND date BETWEEN '{}'
        AND '{}'
GROUP BY
        adGroupId,
        keywordBid,
        keywordText,
                                campaignName,
                                adGroupName
HAVING
        ( SUM( cost ) / SUM( sales ) ) < 0.26 and ( SUM( cost ) / SUM( sales ) ) > 0.24
            """.format(market,startdate, enddate)
            df = pd.read_sql(query, con=conn)
            # return df
            output_filename = get_timestamp() + '_SB_keyword_3_2.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            s1 = f"关键词数量：{df[['campaignName','adGroupName','keywordBid','keywordText']].drop_duplicates().shape[0]}"
            result_str = "[ACOS取值为0.2][{}]生成文件为：{}".format(s1, output_filename)
            print("1.3.2 Data inserted successfully!")
            return result_str
        except Exception as error:
            print("1.3.2 Error while inserting data:", error)



    def get_sb_keyword_133(self, market, startdate, enddate):
        """关键词优化分析：-1-3.3  3 在 2024.04.01 至 2024.04.14 这段时间内，  法国SB广告中 ACOS值在22%到24%区间 的 定向投放关键词。将这些关键词信息生成csv文件，里面记录这些关键词的以下信息，CPC，SKU/ASIN， ACOS, Clicks，adgroupid.（ACOS20%的+10%+20%)"""
        try:
            conn = self.conn

            query = """
            SELECT
              campaignName,
              adGroupName,
        adGroupId,
        keywordBid,
        keywordText,
        (SUM( cost )/SUM( clicks )) AS CPC,
        SUM( cost ) AS totalCost,
        SUM( sales ) AS totalSales,
        SUM( clicks ) AS totalClicks,
        ( SUM( cost ) / SUM( sales ) ) AS ACOS
FROM
        amazon_targeting_reports_sb
WHERE
        market = '{}'
        AND date BETWEEN '{}'
        AND '{}'
GROUP BY
        adGroupId,
        keywordBid,
        keywordText,
                                campaignName,
                                adGroupName
HAVING
        ( SUM( cost ) / SUM( sales ) ) < 0.24 and ( SUM( cost ) / SUM( sales ) ) > 0.22
            """.format(market,startdate, enddate)
            df = pd.read_sql(query, con=conn)
            # return df
            output_filename = get_timestamp() + '_SB_keyword_3_3.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            s1 = f"关键词数量：{df[['campaignName','adGroupName','keywordBid','keywordText']].drop_duplicates().shape[0]}"
            result_str = "[ACOS取值为0.2][{}]生成文件为：{}".format(s1, output_filename)
            print("1.3.3 Data inserted successfully!")
            return result_str
        except Exception as error:
            print("1.3.3 Error while inserting data:", error)


    def get_sb_advertise_311(self, startdate, enddate):
        """广告优化分析：-3-1.1  找出意大利SP广告的优质关键词"""
        try:
            conn = self.conn

            query = """
SELECT
              campaignName,
              adGroupName,
        adGroupId,
        keywordBid,
        keyword,
				keywordId,
        (SUM( cost )/SUM( clicks )) AS CPC,
        SUM( cost ) AS totalCost,
        SUM( sales14d ) AS totalSales,
        SUM( clicks ) AS totalClicks,
        ( SUM( cost ) / SUM( sales14d ) ) AS ACOS
FROM
        amazon_targeting_reports_sp
WHERE
        market = 'It'
        AND date BETWEEN '{}'
        AND '{}'
GROUP BY
        adGroupId,
        keywordBid,
        keyword,
				keywordId,
				campaignName,
				adGroupName
HAVING
        ( SUM( cost ) / SUM( sales14d ) ) < 0.2
            """.format(startdate, enddate)
            df = pd.read_sql(query, con=conn)
            # return df
            output_filename = get_timestamp() + '_SB_advertise_1_1.csv'
            # df.to_csv("http://192.168.5.191:5173/src/assets/csv/"+output_filename, index=False, encoding='utf-8-sig')
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            s1 = f"意大利优质关键词数量：{df[['adGroupId','keywordBid','keyword','keywordId','campaignName','adGroupName']].drop_duplicates().shape[0]}"
            result_str = "[{}]生成文件为：{}".format(s1, output_filename)
            print("3.1.1 Data inserted successfully!")
            return result_str
        except Exception as error:
            print("3.1.1 Error while inserting data:", error)

    def get_sb_advertise_312(self, market, startdate, enddate):
        """广告优化分析：-3-1.2  2..在 2024.04.01 至 2024.04.14 这段时间内，  法国SB广告中 ACOS值在14%到16% 区间的 campaign 广告活动。将这些信息生成csv文件，里面记录这些关键词的以下信息，CPC， ACOS,  Clicks，campaignid，spend.（-20-30）"""
        try:
            conn = self.conn

            query = """
            SELECT
        campaignId,
        campaignName,
        campaignBudgetAmount,
        SUM( clicks ) AS totalClicks,
        SUM( cost ) AS totalCost,
        SUM( sales ) AS totalSales,
        ( SUM( cost ) / SUM( sales ) )  AS ACOS,
        ( SUM( cost ) / SUM( clicks ) ) AS CPC
FROM
        amazon_campaign_reports_sb
WHERE
        date BETWEEN '{}'
        AND '{}'
        AND market = '{}'
GROUP BY
        campaignId,
        campaignName,
        campaignBudgetAmount
HAVING
        ACOS < 0.16 and ACOS > 0.14
ORDER BY
        ACOS ASC

            """.format(startdate, enddate, market)
            df = pd.read_sql(query, con=conn)
            # return df
            output_filename = get_timestamp() + '_SB_advertise_1_2.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            s1 = f"关键词数量：{df[['campaignId','campaignName']].drop_duplicates().shape[0]}"
            result_str = "[ACOS取值为0.2][{}]生成文件为：{}".format(s1, output_filename)
            print("3.1.2 Data inserted successfully!")
            return result_str
        except Exception as error:
            print("3.1.2 Error while inserting data:", error)



    def get_sb_advertise_313(self, market, startdate, enddate):
        """广告优化分析：-3-1.3  3.在 2024.04.01 至 2024.04.14 这段时间内，  法国SB广告中ACOS值在16%到18%区间 的 campaign 广告活动。将这些信息生成csv文件，里面记录这些关键词的以下信息，CPC， ACOS,  Clicks，campaignid，spend.（-10%-20%"""
        try:
            conn = self.conn

            query = """
            SELECT
        campaignId,
        campaignName,
        campaignBudgetAmount,
        SUM( clicks ) AS totalClicks,
        SUM( cost ) AS totalCost,
        SUM( sales ) AS totalSales,
        ( SUM( cost ) / SUM( sales ) )  AS ACOS,
        ( SUM( cost ) / SUM( clicks ) ) AS CPC
FROM
        amazon_campaign_reports_sb
WHERE
        date BETWEEN '{}'
        AND '{}'
        AND market = '{}'
GROUP BY
        campaignId,
        campaignName,
        campaignBudgetAmount
HAVING
        ACOS < 0.18 and ACOS > 0.16
ORDER BY
        ACOS ASC

            """.format(startdate, enddate, market)
            df = pd.read_sql(query, con=conn)
            # return df
            output_filename = get_timestamp() + '_SB_advertise_1_3.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            s1 = f"关键词数量：{df[['campaignId','campaignName']].drop_duplicates().shape[0]}"
            result_str = "[ACOS取值为0.2][{}]生成文件为：{}".format(s1, output_filename)
            print("3.1.3 Data inserted successfully!")
            return result_str
        except Exception as error:
            print("3.1.3 Error while inserting data:", error)

    def get_sb_advertise_321(self, market, startdate, enddate):
        """广告优化分析：-3-2.1  1.在 2024.04.01 至 2024.04.14 这段时间内，  法国SB广告中ACOS值 大于26%的  campaign 广告活动。将这些信息生成csv文件，里面记录这些关键词的以下信息，CPC， ACOS,  Clicks，campaignid，spend.（+30%）"""
        try:
            conn = self.conn

            query = """
            SELECT
        campaignId,
        campaignName,
        campaignBudgetAmount,
        SUM( clicks ) AS totalClicks,
        SUM( cost ) AS totalCost,
        SUM( sales ) AS totalSales,
        ( SUM( cost ) / SUM( sales ) )  AS ACOS,
        ( SUM( cost ) / SUM( clicks ) ) AS CPC
FROM
        amazon_campaign_reports_sb
WHERE
        date BETWEEN '{}'
        AND '{}'
        AND market = '{}'
GROUP BY
        campaignId,
        campaignName,
        campaignBudgetAmount
HAVING
        ACOS > 0.26
ORDER BY
        ACOS ASC

            """.format(startdate, enddate, market)
            df = pd.read_sql(query, con=conn)
            # return df
            output_filename = get_timestamp() + '_SB_advertise_2_1.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            s1 = f"关键词数量：{df[['campaignId', 'campaignName']].drop_duplicates().shape[0]}"
            result_str = "[ACOS取值为0.2][{}]生成文件为：{}".format(s1, output_filename)
            print("3.2.1 Data inserted successfully!")
            return result_str
        except Exception as error:
            print("3.2.1 Error while inserting data:", error)

    def get_sb_advertise_322(self, market, startdate, enddate):
        """广告优化分析：-3-2.2  2.在 2024.04.01 至 2024.04.14 这段时间内，  法国SB广告中ACOS值在24%到26%区间的 campaign 广告活动。将这些信息生成csv文件，里面记录这些关键词的以下信息，CPC， ACOS,  Clicks，campaignid，spend.（+20% +30%）"""
        try:
            conn = self.conn

            query = """
            SELECT
        campaignId,
        campaignName,
        campaignBudgetAmount,
        SUM( clicks ) AS totalClicks,
        SUM( cost ) AS totalCost,
        SUM( sales ) AS totalSales,
        ( SUM( cost ) / SUM( sales ) )  AS ACOS,
        ( SUM( cost ) / SUM( clicks ) ) AS CPC
FROM
        amazon_campaign_reports_sb
WHERE
        date BETWEEN '{}'
        AND '{}'
        AND market = '{}'
GROUP BY
        campaignId,
        campaignName,
        campaignBudgetAmount
HAVING
        ACOS < 0.26 and ACOS > 0.24
ORDER BY
        ACOS ASC

            """.format(startdate, enddate, market)
            df = pd.read_sql(query, con=conn)
            # return df
            output_filename = get_timestamp() + '_SB_advertise_2_2.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            s1 = f"关键词数量：{df[['campaignId', 'campaignName']].drop_duplicates().shape[0]}"
            result_str = "[ACOS取值为0.2][{}]生成文件为：{}".format(s1, output_filename)
            print("3.2.2 Data inserted successfully!")
            return result_str
        except Exception as error:
            print("3.2.2 Error while inserting data:", error)

    def get_sb_advertise_323(self, market, startdate, enddate):
        """广告优化分析：-3-2.3  3.在2024.04.01 至 2024.04.14 这段时间内，  法国SB广告中ACOS值在22%到24%区间的 campaign 广告活动。将这些信息生成csv文件，里面记录这些关键词的以下信息，CPC， ACOS,  Clicks，campaignid，spend.（+10% +20%）"""
        try:
            conn = self.conn

            query = """
            SELECT
        campaignId,
        campaignName,
        campaignBudgetAmount,
        SUM( clicks ) AS totalClicks,
        SUM( cost ) AS totalCost,
        SUM( sales ) AS totalSales,
        ( SUM( cost ) / SUM( sales ) )  AS ACOS,
        ( SUM( cost ) / SUM( clicks ) ) AS CPC
FROM
        amazon_campaign_reports_sb
WHERE
        date BETWEEN '{}'
        AND '{}'
        AND market = '{}'
GROUP BY
        campaignId,
        campaignName,
        campaignBudgetAmount
HAVING
        ACOS < 0.24 and ACOS > 0.22
ORDER BY
        ACOS ASC

            """.format(startdate, enddate, market)
            df = pd.read_sql(query, con=conn)
            # return df
            output_filename = get_timestamp() + '_SB_advertise_2_3.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            s1 = f"关键词数量：{df[['campaignId', 'campaignName']].drop_duplicates().shape[0]}"
            result_str = "[ACOS取值为0.2][{}]生成文件为：{}".format(s1, output_filename)
            print("3.2.3 Data inserted successfully!")
            return result_str
        except Exception as error:
            print("3.2.3 Error while inserting data:", error)




# if __name__ == '__main__':
#     print("====amazon ====")
#     startdate = '2024-04-01'
#     endate = '2024-04-14'
#     market = 'US'
#     dwx = AmazonMysqlNewSBRagUitl(db_info)
#     daily_cost_rate = dwx.get_sp_searchterm_keyword_info(market, startdate,endate)
#     print(daily_cost_rate)





