import pymysql
import pandas as pd
import warnings

warnings.filterwarnings("ignore", category=UserWarning)

db_info = {'host': '******', 'user': '******', 'passwd': '******', 'port': 3308, 'db': '******',
           'charset': 'utf8mb4', 'use_unicode': True, }


bussname_list = ['江苏淮安电玩猩-淮汽店', '江苏南京冲锋熊猫-百家湖店', '安徽合肥电玩猩-合肥店', '江苏南京电玩猩总部', '江苏徐州电玩猩（云龙店）', '江苏南京电玩猩-义乌店',
                 '江苏南通电玩猩-南通店', '江苏镇江电玩猩-江大店', '江苏淮安电玩猩-淮安万达店', '江苏徐州电玩猩-彭城店']


class DwxMysqlRagUitl:

    def __init__(self, db_info):
        self.conn = self.connect(db_info)

    def connect(self, db_info):
        try:
            conn = pymysql.connect(**db_info)
            print("Connected to dwx_mysql database!")
            return conn
        except Exception as error:
            print("Error while connecting to dwx_mysql:", error)
            return None

    def connect_close(self):
        try:
            self.conn.close()
        except Exception as error:
            print("Error while connecting to dwx_mysql:", error)
            return None

# -----------------------------以下是根据自动化周报新加fc--------------------------------------------
    def get_nt_total_sellMoney(self, startdate, enddate, BusinessName):
        """根据给定的日期范围和店铺名获取销售额信息"""
        try:
            conn = self.conn

            query = """
             SELECT ChannelName, SUM(SellMoney) AS Sales 
             FROM view_cmsalelogdetail 
             WHERE ChannelName IN ('抖音团购', '美团大众', '盈客宝') 
             AND PeriodTime BETWEEN '{}' AND '{}' 
             AND OwnedBusiness IN (SELECT ID FROM mall_business WHERE BusinessName = '{}') 
             GROUP BY ChannelName
             """.format(startdate, enddate, BusinessName)
            result_str = ""

            df = pd.read_sql(query, con=conn)

            for index, row in df.iterrows():
                channelName = row['ChannelName']
                total_sellMoney = row['Sales']
                result_str += "渠道名:{}  - 销售额:{}元\n".format(channelName, total_sellMoney)

            print("1 1.1 Data inserted successfully!")
            return result_str



        except Exception as error:
            print("Error while retrieving sales info:", error)

    def calculate_channel_sellMoney_proportion(self, startdate, enddate, store_name):
        """1.2计算给定日期范围内店铺各个渠道套餐销售额占比"""
        try:
            conn = self.conn

            query = """
            SELECT
                ChannelName,
                SUM(SellMoney) AS Sales,
                ROUND(100*(SUM(SellMoney) / total_sales) ,2)AS SalesPercentage
            FROM
                view_cmsalelogdetail
                JOIN (
                    SELECT
                        SUM(vcsld.SellMoney) AS total_sales
                    FROM
                        view_cmsalelogdetail vcsld
                    WHERE
                        vcsld.ChannelName IN ('抖音团购', '美团大众', '盈客宝')
                        AND vcsld.Period BETWEEN '{}' AND '{}'
                        AND OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '{}' )

                ) AS total ON 1=1
            WHERE
                ChannelName IN ('抖音团购', '美团大众', '盈客宝')
                AND PeriodTime BETWEEN '{}' AND '{}'
                AND OwnedBusiness IN (SELECT ID FROM mall_business WHERE BusinessName = '{}')
            GROUP BY
                ChannelName
            """.format(startdate, enddate, store_name, startdate, enddate, store_name)
            result_str = ""
            df = pd.read_sql(query, con=conn)

            for index, row in df.iterrows():
                channelName = row['ChannelName']
                proportion = row['SalesPercentage']
                result_str += "渠道名:{}  - 销售额占比:{}%\n".format(channelName, proportion)

            print("1 1.2 Data inserted successfully!")

            return result_str


        except Exception as error:
            print("Error while calculating channel sellMoney proportion:", error)

    def calculate_store_channel_sellMoney_proportion(self, startdate, enddate):
        """1.3计算给定日期范围内各个店铺的各渠道套餐销售额占比"""
        try:
            conn = self.conn

            query = """
            SELECT
                ChannelName,
                SUM(SellMoney) AS Sales,
                ROUND(100*(SUM(SellMoney) / total_sales) ,2)AS SalesPercentage
            FROM
                view_cmsalelogdetail
                JOIN (
                    SELECT
                        SUM(vcsld.SellMoney) AS total_sales
                    FROM
                        view_cmsalelogdetail vcsld
                    WHERE
                        vcsld.ChannelName IN ('抖音团购', '美团大众', '盈客宝')
                        AND vcsld.Period BETWEEN '{}' AND '{}'
                ) AS total ON 1=1
            WHERE
                ChannelName IN ('抖音团购', '美团大众', '盈客宝')
                AND PeriodTime BETWEEN '{}' AND '{}'
            GROUP BY
                ChannelName
            """.format(startdate, enddate, startdate, enddate)

            result_str = ""

            df = pd.read_sql(query, con=conn)

            for index, row in df.iterrows():
                channelName = row['ChannelName']
                total_sellMoney = row['SalesPercentage']
                result_str += "渠道名:{}  - 销售额占比:{}%\n".format(channelName, total_sellMoney)

            print("1 1.3 Data inserted successfully!")

            return result_str

        except Exception as error:
            print("Error while calculating store channel sellMoney proportion:", error)

    def find_low_sellMoney_channel(self, startdate, enddate, store_name):
        """1.4找出给定日期范围内南通店销售额占比低于总占比 80% 的销售渠道"""
        try:
            conn = self.conn

            query = """
            WITH TotalSales AS (
                SELECT
                    ChannelName,
                    SUM(SellMoney) AS Sales,
                    100*SUM(SellMoney) / total_sales AS SalesPercentage
                FROM
                    view_cmsalelogdetail
                JOIN (
                    SELECT
                        SUM(vcsld.SellMoney) AS total_sales
                    FROM
                        view_cmsalelogdetail vcsld
                    WHERE
                        vcsld.ChannelName IN ('抖音团购', '美团大众', '盈客宝')
                        AND vcsld.Period BETWEEN '{}' AND '{}'
                ) AS total ON 1=1
                WHERE
                    ChannelName IN ('抖音团购', '美团大众', '盈客宝')
                    AND PeriodTime BETWEEN '{}' AND '{}'
                GROUP BY
                    ChannelName
            ),
            -- CTE for channel sales
            ChannelSales AS (
                SELECT
                    ChannelName,
                    SUM(SellMoney) AS Sales,
                   100*SUM(SellMoney) / total_sales AS SalesPercentage
                FROM
                    view_cmsalelogdetail
                JOIN (
                    SELECT
                        SUM(vcsld.SellMoney) AS total_sales
                    FROM
                        view_cmsalelogdetail vcsld
                    WHERE
                        vcsld.ChannelName IN ('抖音团购', '美团大众', '盈客宝')
                        AND vcsld.Period BETWEEN '{}' AND '{}'
                        AND OwnedBusiness IN (SELECT ID FROM mall_business WHERE BusinessName = '{}')
                ) AS total ON 1=1
                WHERE
                    ChannelName IN ('抖音团购', '美团大众', '盈客宝')
                    AND PeriodTime BETWEEN '{}' AND '{}'
                    AND OwnedBusiness IN (SELECT ID FROM mall_business WHERE BusinessName = '{}')
                GROUP BY
                    ChannelName
            )
            SELECT
                cs.ChannelName
            FROM
                ChannelSales cs
            JOIN
                TotalSales ts ON cs.ChannelName = ts.ChannelName
            WHERE
                cs.SalesPercentage < 0.8 * ts.SalesPercentage
            """.format(startdate, enddate, startdate, enddate, startdate, enddate, store_name,startdate, enddate, store_name)

            result_str = ""

            df = pd.read_sql(query, con=conn)

            for index, row in df.iterrows():
                channelName = row['ChannelName']

                result_str += "渠道名:{}  \n".format(channelName)

            print("1 1.4 Data inserted successfully!")

            return result_str


        except Exception as error:
            print("Error while finding low sellMoney channel:", error)

    def calculate_optimizable_channel_sellProportion(self, startdate, enddate, store_name):
        """2.1 计算给定日期范围内店铺可优化渠道的各套餐销售额占比"""
        try:
            conn = self.conn

            query = """
            SELECT
                CoinSetMealName,
                SUM( SellMoney ) AS TotalSellMoney,
                ROUND(100.0 * SUM(SellMoney) / total_sales ,2)AS SalesPercentage
            FROM
                view_cmsalelogdetail
                JOIN (
                    SELECT
                        SUM( vcsld.SellMoney ) AS total_sales
                    FROM
                        view_cmsalelogdetail vcsld
                    WHERE
                        vcsld.ChannelName = '盈客宝'
                        AND vcsld.Period BETWEEN '{}' AND '{}'
                        AND OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '{}' ) 
                ) AS total ON 1=1
            WHERE
                ChannelName = '盈客宝'
                AND PeriodTime BETWEEN '{}' AND '{}'
                AND OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '{}' ) 
            GROUP BY 
                CoinSetMealName;
            """.format(startdate, enddate, store_name, startdate, enddate, store_name)

            result_str = ""
            df = pd.read_sql(query, con=conn)

            for index, row in df.iterrows():
                CoinSetMealName = row['CoinSetMealName']
                SalesPercentage = row['SalesPercentage']
                result_str += "套餐名:{}  - 销售额占比:{}%\n \n".format(CoinSetMealName, SalesPercentage)

            print("1 2.1 Data inserted successfully!")

            return result_str

        except Exception as error:
            print("Error while calculating channel sellPercentage:", error)

    def calculate_average_sellProportion(self, startdate, enddate):
        """1 2.2 分析2024.04.01-2024.04.07这段时间内在可优化渠道（1.4结论）中各套餐销量平均占比"""
        try:
            conn = self.conn

            query = """
            WITH AvgSalesPerMeal AS (
                SELECT
                    vcl.CoinSetMealName,
                    COUNT(DISTINCT vcl.OwnedBusiness) AS NumBusinesses,
                    SUM(vcl.SellMoney) AS TotalSellMoney
                FROM
                    view_cmsalelogdetail vcl
                JOIN mall_business mb ON mb.ID = vcl.OwnedBusiness
                WHERE
                    vcl.ChannelName = '盈客宝'
                    AND vcl.PeriodTime BETWEEN '{}' AND '{}'
                    AND vcl.CoinSetMealName IN ( '0元=20币 签到币','195元1000游戏币','299元1600游戏币','99元500游戏币','39.9元100游戏币','1500元8000游戏币','（私域）有奖竞答福利币')
                GROUP BY
                    vcl.CoinSetMealName
            )
            SELECT
                asm.CoinSetMealName,
                SUM(asm.TotalSellMoney) / SUM(asm.NumBusinesses) AS AvgSalesPerBusiness,
                100.0 * (SUM(asm.TotalSellMoney) / SUM(asm.NumBusinesses)) / SUM(SUM(asm.TotalSellMoney) / SUM(asm.NumBusinesses)) OVER () AS SalesPercentage
            FROM
                AvgSalesPerMeal asm
            GROUP BY
                asm.CoinSetMealName;
            """.format(startdate, enddate)

            result_str = ""
            df = pd.read_sql(query, con=conn)

            for index, row in df.iterrows():
                CoinSetMealName = row['CoinSetMealName']
                SalesPercentage = row['SalesPercentage']
                result_str += "套餐名:{}  - 销售额占比:{}\n".format(CoinSetMealName, SalesPercentage)

            print("1 2.2 Data inserted successfully!")

            return result_str

        except Exception as error:
            print("Error while calculating average sellProportion:", error)

    def analyze_underperforming_packages(self, startdate, enddate, store_name):
        """1 2.3 分析2024.04.01-2024.04.07这段时间内南通店在可优化渠道（1.4结论）滞销套餐名（销量占比低于均值的80%）"""
        try:
            conn = self.conn

            query = """
            WITH ChannelSales AS (
                SELECT
                    CoinSetMealName,
                    SUM( SellMoney ) AS TotalSellMoney ,
                    100.0 *SUM(SellMoney) / total_sales AS SalesPercentage
                FROM
                    view_cmsalelogdetail
                    JOIN (
                    SELECT
                        SUM( vcsld.SellMoney ) AS total_sales
                    FROM
                        view_cmsalelogdetail vcsld
                    WHERE
                        vcsld.ChannelName = '盈客宝'
                        AND vcsld.Period BETWEEN '{}' AND '{}'
                        AND OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '{}' )
                    ) AS total ON 1=1
                WHERE
                    ChannelName = '盈客宝'
                    AND PeriodTime BETWEEN '{}' AND '{}'
                    AND OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '{}' )
                GROUP BY
                    CoinSetMealName ),

            TotalSale AS (
                WITH AvgSalesPerMeal AS (
                    SELECT
                        vcl.CoinSetMealName,
                        COUNT(DISTINCT vcl.OwnedBusiness) AS NumBusinesses,
                        SUM(vcl.SellMoney) AS TotalSellMoney
                    FROM
                        view_cmsalelogdetail vcl
                    JOIN mall_business mb ON mb.ID = vcl.OwnedBusiness
                    WHERE
                        vcl.ChannelName = '盈客宝'
                        AND vcl.PeriodTime BETWEEN '{}' AND '{}'
                        AND vcl.CoinSetMealName IN ( '0元=20币 签到币','195元1000游戏币','299元1600游戏币','99元500游戏币','39.9元100游戏币','1500元8000游戏币','（私域）有奖竞答福利币')
                    GROUP BY
                        vcl.CoinSetMealName
                )
                SELECT
                    asm.CoinSetMealName,
                    SUM(asm.TotalSellMoney) / SUM(asm.NumBusinesses) AS AvgSalesPerBusiness,
                    100.0 * (SUM(asm.TotalSellMoney) / SUM(asm.NumBusinesses)) / SUM(SUM(asm.TotalSellMoney) / SUM(asm.NumBusinesses)) OVER () AS SalesPercentage
                FROM
                    AvgSalesPerMeal asm
                GROUP BY
                    asm.CoinSetMealName
            )
            SELECT
                cs.CoinSetMealName
            FROM
                ChannelSales cs
            JOIN
                TotalSale ts ON cs.CoinSetMealName = ts.CoinSetMealName
            WHERE
                cs.SalesPercentage  < 0.8 * ts.SalesPercentage;
            """.format(startdate, enddate, store_name, startdate, enddate, store_name, startdate, enddate)

            result_str = ""
            df = pd.read_sql(query, con=conn)

            for index, row in df.iterrows():
                CoinSetMealName = row['CoinSetMealName']

                result_str += "滞销套餐: {}\n".format(CoinSetMealName)

            print("1 2.3 Data inserted successfully!")

            return result_str

        except Exception as error:
            print("Error while analyzing underperforming packages:", error)

    def analyze_daily_revenue_fluctuation(self, startdate, enddate, store_name):
        """2 1. 1分析2024.04.01-2024.04.07这段时间内南通店每日营收金额波动情况"""
        try:
            conn = self.conn

            query = """
            SELECT
                mp.ClassTime AS date,
                SUM( mo.TotleMoney ) AS daily_revenue 
            FROM
                mall_order mo
                JOIN mall_period mp ON mo.InPeriod = mp.ID 
            WHERE
                mo.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '{}' )
                AND mp.ClassTime BETWEEN '{}' AND '{}' 
            GROUP BY
                mp.ClassTime 
            ORDER BY
                mp.ClassTime;
            """.format(store_name, startdate, enddate)

            result_str = ""
            df = pd.read_sql(query, con=conn)

            for index, row in df.iterrows():
                Date = row['date']
                Daily_revenue = row['daily_revenue']

                result_str += "日期:{}  - 营收额:{}\n，\n".format(Date,Daily_revenue)

            print("2 1.1 Data inserted successfully!")

            return result_str

        except Exception as error:
            print("Error while analyzing daily revenue fluctuation:", error)

    def analyze_weekly_revenue_anomaly(self, last_start,last_end,startdate, enddate, store_name):
        """2 1.2.分析24年第14周2024.04.01-2024.04.07这一周营收金额是否有异常情况，分析第14周最低营收是否低于第13周最低值的80%"""
        try:
            conn = self.conn

            query = """
                    WITH week13 AS (
            SELECT mp.ClassTime AS date,
                   SUM(mo.TotleMoney) AS daily_revenue
              FROM mall_order mo
              JOIN mall_period mp
                ON mo.InPeriod = mp.ID
             WHERE mo.OwnedBusiness IN (SELECT ID FROM mall_business WHERE BusinessName = '{}')
               AND mp.ClassTime BETWEEN '{}' AND '{}'
          GROUP BY mp.ClassTime
        ),
        week14 AS (
            SELECT mp.ClassTime AS date,
                   SUM(mo.TotleMoney) AS daily_revenue
              FROM mall_order mo
              JOIN mall_period mp
                ON mo.InPeriod = mp.ID
             WHERE mo.OwnedBusiness IN (SELECT ID FROM mall_business WHERE BusinessName = '{}')
               AND mp.ClassTime BETWEEN '{}' AND '{}'
          GROUP BY mp.ClassTime
        )
            SELECT week14.date AS date,
                   week14.daily_revenue AS week14_revenue,
                                         0.8 * (SELECT MIN(daily_revenue) FROM week13) AS threhold,
                                          CASE
                    WHEN  week14.daily_revenue < 0.8 * (SELECT MIN(daily_revenue) FROM week13) THEN '异常'
                    ELSE '正常'
                END AS revenue_status
              FROM week14
             ORDER BY week14.date;
            """.format(store_name,last_start,last_end, store_name,startdate, enddate)
            result_str = ""
            df = pd.read_sql(query, con=conn)

            for index, row in df.iterrows():
                Date = row['date']
                Threhold= row['threhold']
                State = row['revenue_status']

                result_str += "日期:{}  - 异常阈值:{}\n，- 异常状态:{}\n\n".format(Date, Threhold,State)

            print("2 1.2 Data inserted successfully!")
            return result_str
            # 处理数据并返回结果

        except Exception as error:
            print("Error while analyzing weekly revenue anomaly:", error)

    def calculate_cost_fluctuation_weekly(self, startdate, enddate, store_name):
        """3 1.1 分析给定日期范围内每周的成本波动情况"""
        try:
            conn = self.conn
            query = """
               SELECT
	Doll7Recycle.EDAY,(
		ROUND((((
					CASE
							
							WHEN Doll7Prize.prize_amount IS NULL THEN
							0 ELSE Doll7Prize.prize_amount 
						END 
						) - ( CASE WHEN Doll7Recycle.recycle_amount IS NULL THEN 0 ELSE Doll7Recycle.recycle_amount END ) 
						) * 2.55 +(
					CASE
							
							WHEN Doll7Recycle.recycle_amount IS NULL THEN
							0 ELSE Doll7Recycle.recycle_amount 
						END 
						) * 1.2 
						),
					2 
					) + ROUND((((
							CASE
									
									WHEN Doll8Prize.prize_amount IS NULL THEN
									0 ELSE Doll8Prize.prize_amount 
								END 
								)- ( CASE WHEN Doll8Recycle.recycle_amount IS NULL THEN 0 ELSE Doll8Recycle.recycle_amount END ) 
								) * 5.5 + ( CASE WHEN Doll8Recycle.recycle_amount IS NULL THEN 0 ELSE Doll8Recycle.recycle_amount END ) * 2.4 
							),
						2 
						) + ROUND((((
								CASE
										
										WHEN Doll15Prize.prize_amount IS NULL THEN
										0 ELSE Doll15Prize.prize_amount 
									END 
									) - ( CASE WHEN Doll15Recycle.recycle_amount IS NULL THEN 0 ELSE Doll15Recycle.recycle_amount END ) 
									) * 20 + ( CASE WHEN Doll15Recycle.recycle_amount IS NULL THEN 0 ELSE Doll15Recycle.recycle_amount END ) * 14.4 
								),
							2 
						) + ( CASE WHEN Lottery.TotalTicket IS NULL THEN 0 ELSE Lottery.TotalTicket END ) + ( CASE WHEN GiftMachine.total_cost IS NULL THEN 0 ELSE GiftMachine.total_cost END ) 
					) AS total_cost 
				FROM
					(
					SELECT
						mp.ClassName AS EDAY,
						COALESCE ( SUM( Amount ), 0 ) AS recycle_amount 
					FROM
						view_giftrecoverlogitem gf
						JOIN mall_period mp ON gf.InPeriod = mp.ID 
					WHERE
						( GoodName = '7寸娃娃' OR GoodName = '普通娃娃' ) 
						AND gf.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '{}' ) 
					GROUP BY
						mp.ClassName 
					) AS Doll7Recycle
					LEFT JOIN (
					SELECT
						vm.Period AS EDAY,
						COALESCE ( SUM( OutGift ), 0 ) AS prize_amount 
					FROM
						view_machinereturnrate vm 
					WHERE
						TypeName = '7寸娃娃机' 
						AND vm.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '{}' ) 
					GROUP BY
						vm.Period 
					) AS Doll7Prize ON Doll7Recycle.EDAY = Doll7Prize.EDAY
					LEFT JOIN (
					SELECT
						mp.ClassName AS EDAY,
						COALESCE ( SUM( Amount ), 0 ) AS recycle_amount 
					FROM
						view_giftrecoverlogitem gf
						JOIN mall_period mp ON gf.InPeriod = mp.ID 
					WHERE
						GoodName = '8寸娃娃' 
						AND gf.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '{}' ) 
					GROUP BY
						mp.ClassName 
					) AS Doll8Recycle ON Doll7Recycle.EDAY = Doll8Recycle.EDAY
					LEFT JOIN (
					SELECT
						vm.Period AS EDAY,
						COALESCE ( SUM( OutGift ), 0 ) AS prize_amount 
					FROM
						view_machinereturnrate vm 
					WHERE
						TypeName = '8寸娃娃机' 
						AND vm.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '{}' ) 
					GROUP BY
						vm.Period 
					) AS Doll8Prize ON Doll7Recycle.EDAY = Doll8Prize.EDAY
					LEFT JOIN (
					SELECT
						mp.ClassName AS EDAY,
						COALESCE ( SUM( Amount ), 0 ) AS recycle_amount 
					FROM
						view_giftrecoverlogitem gf
						JOIN mall_period mp ON gf.InPeriod = mp.ID 
					WHERE
						GoodName = '15寸娃娃' 
						AND gf.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '{}' ) 
					GROUP BY
						mp.ClassName 
					) AS Doll15Recycle ON Doll7Recycle.EDAY = Doll15Recycle.EDAY
					LEFT JOIN (
					SELECT
						vm.Period AS EDAY,
						COALESCE ( SUM( OutGift ), 0 ) AS prize_amount 
					FROM
						view_machinereturnrate vm 
					WHERE
						TypeName = '15寸娃娃机' 
						AND vm.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '{}' ) 
					GROUP BY
						vm.Period 
					) AS Doll15Prize ON Doll7Recycle.EDAY = Doll15Prize.EDAY
					LEFT JOIN (
					SELECT
						vm.Period AS EDAY,
						(
						SUM( OutPhTicket ) + SUM( OutTicket ) + SUM( OutPhTicket ))/ 1000 AS TotalTicket 
					FROM
						view_machinereturnrate vm 
					WHERE
						TypeName = '彩票机' 
						AND vm.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '{}' ) 
					GROUP BY
						vm.Period 
					) AS Lottery ON Doll7Recycle.EDAY = Lottery.EDAY
					LEFT JOIN (
					SELECT
						mp.ClassName AS EDAY,
						SUM(
						ABS( gf.TotleMoney )) AS total_cost 
					FROM
						mall_stockchangelog gf
						JOIN mall_period mp ON gf.InPeriod = mp.ID
						JOIN mall_goodbase gb ON gf.Goods = gb.ID 
					WHERE
						gf.OrderType = '2' 
						AND gb.GoodName NOT LIKE '7寸娃娃' 
						AND gb.GoodName NOT LIKE '8寸娃娃' 
						AND gb.GoodName NOT LIKE '普通娃娃' 
						AND gb.GoodName NOT LIKE '红票' 
						AND gf.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '{}' ) 
					GROUP BY
						mp.ClassName 
					) AS GiftMachine ON Doll7Recycle.EDAY = GiftMachine.EDAY 
				WHERE
					Doll7Recycle.EDAY BETWEEN '{}' 
					AND '{}' 
			GROUP BY
	Doll7Recycle.EDAY;
               """.format(store_name,store_name,store_name,store_name,store_name,store_name,store_name,store_name,startdate, enddate)

            result_str = ""
            df = pd.read_sql(query, con=conn)

            for index, row in df.iterrows():
                date = row['EDAY']
                InTotal = row['total_cost']

                result_str += "日期:{}  - 日成本:{}，\n".format(date, InTotal)

            print("3 1.1 Data inserted successfully!")

            return result_str

        except Exception as error:
            print("Error while analyzing spending fluctuation weekly:", error)

    def analyze_spending_fluctuation_weekly(self, startdate, enddate, store_name):
        """3 1.2 分析南通店第14周日消费波动情况"""
        try:
            conn = self.conn
            query = """
            SELECT
                date(ClassTime) AS cdate,
                SUM(InTotalCoin) - SUM(OutPhCoin + OutCoin) AS InTotalCoin,
                (SUM(InTotalCoin) - SUM(OutPhCoin + OutCoin)) * 0.2 AS TotalSum
            FROM
                view_machinereturnrate
            WHERE
                DATE(ClassTime) BETWEEN '{}' AND '{}'  AND OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '{}' )
            GROUP BY
                date(ClassTime);
            """.format(startdate, enddate, store_name)

            result_str = ""
            df = pd.read_sql(query, con=conn)

            for index, row in df.iterrows():
                cdate = row['cdate']
                InTotalCoin = row['InTotalCoin']
                TotalSum = row['TotalSum']
                result_str += "日期:{}  - 总投币量:{}，消费额:{}\n".format(cdate, InTotalCoin, TotalSum)

            print("3 1.2 Data inserted successfully!")

            return result_str

        except Exception as error:
            print("Error while analyzing spending fluctuation weekly:", error)

    def compare_cost_revenue_ratio(self, last_start, enddate, store_name):
        """3 1.3 比较南通店第13周和14周是否有当日成本超出当日营收50%的情况"""
        try:
            conn = self.conn
            query = """
           WITH DailyRevenue AS (
	SELECT
		DATE( ClassTime ) AS cdate,
		SUM( InTotalCoin ) - SUM( OutPhCoin + OutCoin ) AS revenue,
		(
		SUM( InTotalCoin ) - SUM( OutPhCoin + OutCoin )) * 0.2 AS total_sum 
	FROM
		view_machinereturnrate 
	WHERE
		DATE( ClassTime ) BETWEEN '{}' 
		AND '{}' 
		AND OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '{}' ) 
	GROUP BY
		DATE( ClassTime ) 
	),
	DailyCost AS (
	SELECT
		Doll7Recycle.EDAY AS EDAY,
		(
			ROUND(
				(
					(
						( CASE WHEN Doll7Prize.prize_amount IS NULL THEN 0 ELSE Doll7Prize.prize_amount END ) - ( CASE WHEN Doll7Recycle.recycle_amount IS NULL THEN 0 ELSE Doll7Recycle.recycle_amount END ) 
					) * 2.55 + ( CASE WHEN Doll7Recycle.recycle_amount IS NULL THEN 0 ELSE Doll7Recycle.recycle_amount END ) * 1.2 
				),
				2 
				) + ROUND(
				(
					( CASE WHEN Doll8Prize.prize_amount IS NULL THEN 0 ELSE Doll8Prize.prize_amount END ) - ( CASE WHEN Doll8Recycle.recycle_amount IS NULL THEN 0 ELSE Doll8Recycle.recycle_amount END ) 
				) * 5.5 + ( CASE WHEN Doll8Recycle.recycle_amount IS NULL THEN 0 ELSE Doll8Recycle.recycle_amount END ) * 2.4,
				2 
				) + ROUND(
				(
					( CASE WHEN Doll15Prize.prize_amount IS NULL THEN 0 ELSE Doll15Prize.prize_amount END ) - ( CASE WHEN Doll15Recycle.recycle_amount IS NULL THEN 0 ELSE Doll15Recycle.recycle_amount END ) 
				) * 20 + ( CASE WHEN Doll15Recycle.recycle_amount IS NULL THEN 0 ELSE Doll15Recycle.recycle_amount END ) * 14.4,
				2 
			) + ( CASE WHEN Lottery.TotalTicket IS NULL THEN 0 ELSE Lottery.TotalTicket END ) + ( CASE WHEN GiftMachine.total_cost IS NULL THEN 0 ELSE GiftMachine.total_cost END ) 
		) AS total_cost 
	FROM
		(
		SELECT
			mp.ClassName AS EDAY,
			COALESCE ( SUM( Amount ), 0 ) AS recycle_amount 
		FROM
			view_giftrecoverlogitem gf
			JOIN mall_period mp ON gf.InPeriod = mp.ID 
		WHERE
			( GoodName = '7寸娃娃' OR GoodName = '普通娃娃' ) 
			AND gf.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '{}' ) 
		GROUP BY
			mp.ClassName 
		) AS Doll7Recycle
		LEFT JOIN (
		SELECT
			vm.Period AS EDAY,
			COALESCE ( SUM( OutGift ), 0 ) AS prize_amount 
		FROM
			view_machinereturnrate vm 
		WHERE
			TypeName = '7寸娃娃机' 
			AND vm.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '{}' ) 
		GROUP BY
			vm.Period 
		) AS Doll7Prize ON Doll7Recycle.EDAY = Doll7Prize.EDAY
		LEFT JOIN (
		SELECT
			mp.ClassName AS EDAY,
			COALESCE ( SUM( Amount ), 0 ) AS recycle_amount 
		FROM
			view_giftrecoverlogitem gf
			JOIN mall_period mp ON gf.InPeriod = mp.ID 
		WHERE
			GoodName = '8寸娃娃' 
			AND gf.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '{}' ) 
		GROUP BY
			mp.ClassName 
		) AS Doll8Recycle ON Doll7Recycle.EDAY = Doll8Recycle.EDAY
		LEFT JOIN (
		SELECT
			vm.Period AS EDAY,
			COALESCE ( SUM( OutGift ), 0 ) AS prize_amount 
		FROM
			view_machinereturnrate vm 
		WHERE
			TypeName = '8寸娃娃机' 
			AND vm.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '{}' ) 
		GROUP BY
			vm.Period 
		) AS Doll8Prize ON Doll7Recycle.EDAY = Doll8Prize.EDAY
		LEFT JOIN (
		SELECT
			mp.ClassName AS EDAY,
			COALESCE ( SUM( Amount ), 0 ) AS recycle_amount 
		FROM
			view_giftrecoverlogitem gf
			JOIN mall_period mp ON gf.InPeriod = mp.ID 
		WHERE
			GoodName = '15寸娃娃' 
			AND gf.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '{}' ) 
		GROUP BY
			mp.ClassName 
		) AS Doll15Recycle ON Doll7Recycle.EDAY = Doll15Recycle.EDAY
		LEFT JOIN (
		SELECT
			vm.Period AS EDAY,
			COALESCE ( SUM( OutGift ), 0 ) AS prize_amount 
		FROM
			view_machinereturnrate vm 
		WHERE
			TypeName = '15寸娃娃机' 
			AND vm.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '{}' ) 
		GROUP BY
			vm.Period 
		) AS Doll15Prize ON Doll7Recycle.EDAY = Doll15Prize.EDAY
		LEFT JOIN (
		SELECT
			vm.Period AS EDAY,
			( SUM( OutPhTicket ) + SUM( OutTicket ) + SUM( OutPhTicket ) ) / 1000 AS TotalTicket 
		FROM
			view_machinereturnrate vm 
		WHERE
			TypeName = '彩票机' 
			AND vm.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '{}' ) 
		GROUP BY
			vm.Period 
		) AS Lottery ON Doll7Recycle.EDAY = Lottery.EDAY
		LEFT JOIN (
		SELECT
			mp.ClassName AS EDAY,
			SUM(
			ABS( gf.TotleMoney )) AS total_cost 
		FROM
			mall_stockchangelog gf
			JOIN mall_period mp ON gf.InPeriod = mp.ID
			JOIN mall_goodbase gb ON gf.Goods = gb.ID 
		WHERE
			gf.OrderType = '2' 
			AND gb.GoodName NOT LIKE '7寸娃娃' 
			AND gb.GoodName NOT LIKE '8寸娃娃' 
			AND gb.GoodName NOT LIKE '普通娃娃' 
			AND gb.GoodName NOT LIKE '红票' 
			AND gf.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '{}' ) 
		GROUP BY
			mp.ClassName 
		) AS GiftMachine ON Doll7Recycle.EDAY = GiftMachine.EDAY 
	WHERE
		Doll7Recycle.EDAY BETWEEN '{}' 
		AND '{}' 
	GROUP BY
		Doll7Recycle.EDAY 
	) SELECT
	DailyRevenue.cdate AS Date,
	DailyRevenue.total_sum AS Revenue,
	DailyCost.total_cost AS Cost 
FROM
	DailyRevenue
	JOIN DailyCost ON DailyRevenue.cdate = DailyCost.EDAY 
WHERE
	DailyCost.total_cost > 0.5 * DailyRevenue.total_sum 
            """.format( last_start,enddate, store_name, store_name, store_name, store_name, store_name, store_name, store_name, store_name, store_name,last_start,enddate)

            result_str = ""
            df = pd.read_sql(query, con=conn)

            for index, row in df.iterrows():
                date = row['Date']
                revenue = row['Revenue']
                cost = row['Cost']
                result_str += "异常日期:{}  - 收入:{}，成本:{}\n".format(date, revenue, cost)

            print("3 1.3 Data inserted successfully!")

            return result_str

        except Exception as error:
            print("Error while analyzing spending fluctuation weekly:", error)

    def analyze_weekly_average_cost(self, last_start, enddate, store_name):
        """3 2.1 分析南通店第13周一周成本均值"""
        try:
            conn = self.conn
            query = """
            WITH CostPerDay AS (
	SELECT
		Doll7Recycle.EDAY,(
			ROUND((((
						CASE
								
								WHEN Doll7Prize.prize_amount IS NULL THEN
								0 ELSE Doll7Prize.prize_amount 
							END 
							) - ( CASE WHEN Doll7Recycle.recycle_amount IS NULL THEN 0 ELSE Doll7Recycle.recycle_amount END ) 
							) * 2.55 +(
						CASE
								
								WHEN Doll7Recycle.recycle_amount IS NULL THEN
								0 ELSE Doll7Recycle.recycle_amount 
							END 
							) * 1.2 
							),
						2 
						) + ROUND((((
								CASE
										
										WHEN Doll8Prize.prize_amount IS NULL THEN
										0 ELSE Doll8Prize.prize_amount 
									END 
									)- ( CASE WHEN Doll8Recycle.recycle_amount IS NULL THEN 0 ELSE Doll8Recycle.recycle_amount END ) 
									) * 5.5 + ( CASE WHEN Doll8Recycle.recycle_amount IS NULL THEN 0 ELSE Doll8Recycle.recycle_amount END ) * 2.4 
								),
							2 
							) + ROUND((((
									CASE
											
											WHEN Doll15Prize.prize_amount IS NULL THEN
											0 ELSE Doll15Prize.prize_amount 
										END 
										) - ( CASE WHEN Doll15Recycle.recycle_amount IS NULL THEN 0 ELSE Doll15Recycle.recycle_amount END ) 
										) * 20 + ( CASE WHEN Doll15Recycle.recycle_amount IS NULL THEN 0 ELSE Doll15Recycle.recycle_amount END ) * 14.4 
									),
								2 
							) + ( CASE WHEN Lottery.TotalTicket IS NULL THEN 0 ELSE Lottery.TotalTicket END ) + ( CASE WHEN GiftMachine.total_cost IS NULL THEN 0 ELSE GiftMachine.total_cost END ) 
						) AS total_cost 
					FROM
						(
						SELECT
							mp.ClassName AS EDAY,
							COALESCE ( SUM( Amount ), 0 ) AS recycle_amount 
						FROM
							view_giftrecoverlogitem gf
							JOIN mall_period mp ON gf.InPeriod = mp.ID 
						WHERE
							( GoodName = '7寸娃娃' OR GoodName = '普通娃娃' ) 
							AND gf.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '{}' ) 
						GROUP BY
							mp.ClassName 
						) AS Doll7Recycle
						LEFT JOIN (
						SELECT
							vm.Period AS EDAY,
							COALESCE ( SUM( OutGift ), 0 ) AS prize_amount 
						FROM
							view_machinereturnrate vm 
						WHERE
							TypeName = '7寸娃娃机' 
							AND vm.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '{}' ) 
						GROUP BY
							vm.Period 
						) AS Doll7Prize ON Doll7Recycle.EDAY = Doll7Prize.EDAY
						LEFT JOIN (
						SELECT
							mp.ClassName AS EDAY,
							COALESCE ( SUM( Amount ), 0 ) AS recycle_amount 
						FROM
							view_giftrecoverlogitem gf
							JOIN mall_period mp ON gf.InPeriod = mp.ID 
						WHERE
							GoodName = '8寸娃娃' 
							AND gf.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '{}' ) 
						GROUP BY
							mp.ClassName 
						) AS Doll8Recycle ON Doll7Recycle.EDAY = Doll8Recycle.EDAY
						LEFT JOIN (
						SELECT
							vm.Period AS EDAY,
							COALESCE ( SUM( OutGift ), 0 ) AS prize_amount 
						FROM
							view_machinereturnrate vm 
						WHERE
							TypeName = '8寸娃娃机' 
							AND vm.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '{}' ) 
						GROUP BY
							vm.Period 
						) AS Doll8Prize ON Doll7Recycle.EDAY = Doll8Prize.EDAY
						LEFT JOIN (
						SELECT
							mp.ClassName AS EDAY,
							COALESCE ( SUM( Amount ), 0 ) AS recycle_amount 
						FROM
							view_giftrecoverlogitem gf
							JOIN mall_period mp ON gf.InPeriod = mp.ID 
						WHERE
							GoodName = '15寸娃娃' 
							AND gf.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '{}' ) 
						GROUP BY
							mp.ClassName 
						) AS Doll15Recycle ON Doll7Recycle.EDAY = Doll15Recycle.EDAY
						LEFT JOIN (
						SELECT
							vm.Period AS EDAY,
							COALESCE ( SUM( OutGift ), 0 ) AS prize_amount 
						FROM
							view_machinereturnrate vm 
						WHERE
							TypeName = '15寸娃娃机' 
							AND vm.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '{}' ) 
						GROUP BY
							vm.Period 
						) AS Doll15Prize ON Doll7Recycle.EDAY = Doll15Prize.EDAY
						LEFT JOIN (
						SELECT
							vm.Period AS EDAY,
							(
							SUM( OutPhTicket ) + SUM( OutTicket ) + SUM( OutPhTicket ))/ 1000 AS TotalTicket 
						FROM
							view_machinereturnrate vm 
						WHERE
							TypeName = '彩票机' 
							AND vm.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '{}' ) 
						GROUP BY
							vm.Period 
						) AS Lottery ON Doll7Recycle.EDAY = Lottery.EDAY
						LEFT JOIN (
						SELECT
							mp.ClassName AS EDAY,
							SUM(
							ABS( gf.TotleMoney )) AS total_cost 
						FROM
							mall_stockchangelog gf
							JOIN mall_period mp ON gf.InPeriod = mp.ID
							JOIN mall_goodbase gb ON gf.Goods = gb.ID 
						WHERE
							gf.OrderType = '2' 
							AND gb.GoodName NOT LIKE '7寸娃娃' 
							AND gb.GoodName NOT LIKE '8寸娃娃' 
							AND gb.GoodName NOT LIKE '普通娃娃' 
							AND gb.GoodName NOT LIKE '红票' 
							AND gf.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '{}' ) 
						GROUP BY
							mp.ClassName 
						) AS GiftMachine ON Doll7Recycle.EDAY = GiftMachine.EDAY 
					WHERE
						Doll7Recycle.EDAY BETWEEN '{}' 
						AND '{}' 
					GROUP BY
						Doll7Recycle.EDAY 
						) SELECT
					ROUND( AVG( total_cost ), 2 ) AS avg_weekly_cost 
				FROM
					CostPerDay 
				WHERE
				EDAY BETWEEN '{}' 
	AND '{}';
             """.format(store_name, store_name, store_name, store_name, store_name, store_name, store_name, store_name,last_start,enddate, last_start,enddate)

            result_str = ""
            df = pd.read_sql(query, con=conn)

            for index, row in df.iterrows():
                avg_cost = row['avg_weekly_cost']

                result_str += "平均周成本:{} \n".format(avg_cost)

            print("3 2.1 Data inserted successfully!")

            return result_str

        except Exception as error:
            print("Error while analyzing spending fluctuation weekly:", error)



    def analyze_abnormal_purchase(self, startdate, enddate, store_name):
        """3 2.2 分析南通店出现异常当日的采购礼品价格是否有超过成本均值40%的商品"""
        try:
            conn = self.conn
            query = """
                SELECT
        po.Num AS OrderNo,
        mg.GoodName AS GoodName,
        poi.Amount AS NUM,
        poi.Price AS PurchasePrice 
FROM
        mall_purchaseorder po
        JOIN mall_purchaseorderitem poi ON po.ID = poi.OwnOrder
        JOIN mall_period mp ON po.InPeriod = mp.ID
        JOIN mall_goodbase mg ON mg.ID=poi.Goods
WHERE
        po.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '{}' )
        AND mp.ClassTime BETWEEN '{}' AND '{}'
        AND poi.Price>2165.74*0.4;
                """.format( store_name,startdate, enddate)

            result_str = ""
            df = pd.read_sql(query, con=conn)

            for index, row in df.iterrows():
                orderNo = row['OrderNo']
                Goodname = row['GoodName']
                num = row['NUM']
                Price = row['PurchasePrice']
                result_str += "订单编号:{}  - 商品名:{}，数量:{}，商品价格:{}\n".format(orderNo, Goodname, num,Price)

            print("3 2.2 Data inserted successfully!")

            return result_str

        except Exception as error:
            print("Error while analyzing spending fluctuation weekly:", error)

    def calculate_coin_price_fluctuation(self, startdate, enddate, store_name):
        """4 1.1计算南通店第14周币单价波动"""
        try:
            conn = self.conn

            query = """
     WITH revenue AS (
    SELECT
        PeriodTime,
        SUM(SellMoney) AS total_revenue
    FROM view_cmsalelogdetail
    WHERE
        PeriodTime BETWEEN '{}' AND '{}'
        AND ChannelName NOT LIKE '本店'
        AND OwnedBusiness IN (SELECT ID FROM mall_business WHERE BusinessName = '{}')
    GROUP BY PeriodTime
),
coins AS (
    SELECT
        PeriodTime,
        SUM(sale.ECoinAmount + sale.PCoinAmount + sale.GoldCoinAmount) AS total_coins
    FROM view_cmsalelogdetail AS sale
    WHERE
        PeriodTime BETWEEN '{}' AND '{}'
        AND ChannelName NOT LIKE '本店'
        AND OwnedBusiness IN (SELECT ID FROM mall_business WHERE BusinessName = '{}')
    GROUP BY PeriodTime
)
            SELECT
                r.PeriodTime AS date,
                ROUND(r.total_revenue / c.total_coins ,2) AS coin_price
            FROM revenue r
            JOIN coins c ON r.PeriodTime = c.PeriodTime;
            """.format(startdate, enddate, store_name, startdate, enddate, store_name)

            result_str = ""
            df = pd.read_sql(query, con=conn)

            for index, row in df.iterrows():
                Date = row['date']
                Coin_price = row['coin_price']

                result_str += "日期:{}  - 币单价:{}\n，\n".format(Date, Coin_price)

            print("4 1.1 Data inserted successfully!")
            return result_str

        except Exception as error:
                print("Error while calculating coin price fluctuation:", error)

    def analyze_daily_coin_sales(self, startdate, enddate, store_name):
        """4 1.2分析南通店第14周日售币波动"""
        try:
            conn = self.conn

            query = """
            SELECT
                    Period AS EDAY,
                    SUM( sale.ECoinAmount + sale.PCoinAmount + sale.GoldCoinAmount ) AS count 
            FROM
                    view_cmsalelogdetail AS sale 
            WHERE
                    sale.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '{}' )
                    AND sale.ChannelName NOT LIKE '本店'
                    AND sale.PeriodTime BETWEEN '{}' AND '{}'
            GROUP BY
                    Period;
            """.format(store_name, startdate, enddate)

            result_str = ""
            df = pd.read_sql(query, con=conn)

            for index, row in df.iterrows():
                Date = row['EDAY']
                Coin_price = row['count']

                result_str += "日期:{}  - 日售币:{}\n，\n".format(Date, Coin_price)

            print("4 1.2 Data inserted successfully!")
            return result_str
            # 处理数据并返回结果

        except Exception as error:
            print("Error while analyzing daily coin sales:", error)

    def analyze_member_visits(self, startdate, enddate, store_name):
        """5 1.1 分析南通店第14周到店会员数波动情况"""
        try:
            conn = self.conn

            query = """
            SELECT
                DATE( mp.ClassName ) AS 'Date',
                COUNT( DISTINCT mo.Lg ) AS 'MemberCount' 
            FROM
                mall_order mo
                INNER JOIN mall_business mb ON mo.OwnedBusiness = mb.ID
                INNER JOIN mall_period mp ON mo.InPeriod = mp.ID 
            WHERE
                mb.BusinessName IN ( '{}' )
                AND mp.ClassName BETWEEN '{}' AND '{}' 
            GROUP BY
                DATE( mp.ClassName )
            ORDER BY
                DATE( mp.ClassName ),
                mb.BusinessName;
            """.format(store_name, startdate, enddate)

            result_str = ""
            df = pd.read_sql(query, con=conn)

            for index, row in df.iterrows():
                Date = row['Date']
                Members = row['MemberCount']

                result_str += "日期:{}  - 到店会员数:{}\n，\n".format(Date, Members)

            print("5 1.1 Data inserted successfully!")
            return result_str
            # 处理数据并返回结果

        except Exception as error:
            print("Error while analyzing member visits:", error)

    def analyze_RFM_all_member_proportion(self):
        """5 1.3分析所有店铺RFM会员占比情况"""
        try:
            conn = self.conn

            query = """
            WITH R AS (
                SELECT
                    v.LeaguerCode,
                    CASE
                        WHEN DATEDIFF(CURRENT_DATE(), MAX(p.ClassTime)) < AVG(DATEDIFF(CURRENT_DATE(), p.ClassTime)) OVER () THEN 1
                        ELSE 0
                    END AS R
                FROM
                    mall_order mo
                    JOIN view_leaguervaluebalance v ON mo.Lg = v.BaseInfo
                    JOIN mall_period p ON mo.InPeriod = p.ID
                GROUP BY
                    v.LeaguerCode
            ),
            F AS (
                SELECT
                    v.LeaguerCode,
                    CASE
                        WHEN COUNT(DISTINCT p.ClassTime) > AVG(COUNT(DISTINCT p.ClassTime)) OVER () THEN 1
                        ELSE 0
                    END AS F
                FROM
                    mall_order mo
                    JOIN view_leaguervaluebalance v ON mo.Lg = v.BaseInfo
                    JOIN mall_period p ON mo.InPeriod = p.ID
                GROUP BY
                    v.LeaguerCode
            ),
            M AS (
                SELECT
                    v.LeaguerCode,
                    CASE
                        WHEN SUM(mo.PaidMoney) > AVG(SUM(mo.PaidMoney)) OVER () THEN 1
                        ELSE 0
                    END AS M
                FROM
                    mall_order mo
                    JOIN view_leaguervaluebalance v ON mo.Lg = v.BaseInfo
                    GROUP BY
                    v.LeaguerCode
            )
            SELECT
                CONCAT(R.R, F.F, M.M) AS RFM,
                100*(COUNT(*) / SUM(COUNT(*)) OVER ()) AS percentage
            FROM
                R
                JOIN F ON R.LeaguerCode = F.LeaguerCode
                JOIN M ON R.LeaguerCode = M.LeaguerCode
            GROUP BY
                CONCAT(R.R, F.F, M.M);
            """
            result_str = ""
            df = pd.read_sql(query, con=conn)

            for index, row in df.iterrows():
                RFM_label= row['RFM']
                Members_percentage = row['percentage']

                result_str += "标签:{}  - 会员占比:{}\n，\n".format(RFM_label, Members_percentage)

            print("5 1.3 Data inserted successfully!")
            return result_str
            # 处理数据并返回结果

        except Exception as error:
            print("Error while analyzing all RFM member proportion:", error)

    def analyze_RFM_member_proportion(self, store_name):
        """5 1.2分析南通店的RFM会员占比情况"""
        try:
            conn = self.conn

            query = """
            WITH R AS (
                SELECT
                    v.LeaguerCode,
                    CASE
                        WHEN DATEDIFF(CURRENT_DATE(), MAX(p.ClassTime)) < AVG(DATEDIFF(CURRENT_DATE(), p.ClassTime)) OVER () THEN 1
                        ELSE 0
                    END AS R
                FROM
                    mall_order mo
                    JOIN view_leaguervaluebalance v ON mo.Lg = v.BaseInfo
                    JOIN mall_period p ON mo.InPeriod = p.ID
                WHERE
                    mo.OwnedBusiness IN (SELECT ID FROM mall_business WHERE BusinessName = '{}')
                GROUP BY
                    v.LeaguerCode
            ),
            F AS (
                SELECT
                    v.LeaguerCode,
                    CASE
                        WHEN COUNT(DISTINCT p.ClassTime) > AVG(COUNT(DISTINCT p.ClassTime)) OVER () THEN 1
                        ELSE 0
                    END AS F
                FROM
                    mall_order mo
                    JOIN view_leaguervaluebalance v ON mo.Lg = v.BaseInfo
                    JOIN mall_period p ON mo.InPeriod = p.ID
                WHERE
                    mo.OwnedBusiness IN (SELECT ID FROM mall_business WHERE BusinessName = '{}')
                GROUP BY
                    v.LeaguerCode
            ),
            M AS (
                SELECT
                    v.LeaguerCode,
                    CASE
                        WHEN SUM(mo.PaidMoney) > AVG(SUM(mo.PaidMoney)) OVER () THEN 1
                        ELSE 0
                    END AS M
                FROM
                    mall_order mo
                    JOIN view_leaguervaluebalance v ON mo.Lg = v.BaseInfo
                WHERE
                    mo.OwnedBusiness IN (SELECT ID FROM mall_business WHERE BusinessName = '{}')
                GROUP BY
                    v.LeaguerCode
            )
            SELECT
                CONCAT(R.R, F.F, M.M) AS RFM,
                COUNT(*) AS member_count,
                (COUNT(*) / (SELECT COUNT(DISTINCT LeaguerCode) FROM view_leaguervaluebalance WHERE OwnedBusiness IN (SELECT ID FROM mall_business WHERE BusinessName = '{}'))) * 100 AS percentage
            FROM
                R
                JOIN F ON R.LeaguerCode = F.LeaguerCode
                JOIN M ON R.LeaguerCode = M.LeaguerCode
            GROUP BY
                CONCAT(R.R, F.F, M.M);
            """.format(store_name, store_name, store_name, store_name)

            result_str = ""
            df = pd.read_sql(query, con=conn)

            for index, row in df.iterrows():
                RFM_label = row['RFM']
                Members_count = row['member_count']
                Members_percentage = row['percentage']

                result_str += "标签:{}   - 会员数:{}\n，- 会员占比:{}\n，\n".format(RFM_label, Members_count,Members_percentage)

            print("5 1.2 Data inserted successfully!")
            return result_str

        except Exception as error:
            print("Error while analyzing RFM member proportion for {}: {}".format(store_name, error))

    def calculate_coin_fluctuation(self, startdate, enddate, store_name):
        """6 1.1分析南通店第14周的日机台投币量波动"""
        try:
            conn = self.conn

            query = """
            SELECT
                mp.ClassName,
                SUM(gem.CoinsNum) AS DailyCoins
            FROM
                game_everydaymachineincoins gem
                JOIN mall_period mp ON gem.InPeriod = mp.ID
                JOIN mall_business mb ON gem.OwnedBusiness = mb.ID
            WHERE
                gem.IsDelete = 0
                AND mp.ClassName BETWEEN '{}' AND '{}'
                AND mb.BusinessName IN ('{}')
            GROUP BY
                mp.ClassName,
                mb.BusinessName
            ORDER BY
                mp.ClassName,
                mb.BusinessName;
            """.format(startdate, enddate, store_name)

            result_str = ""
            df = pd.read_sql(query, con=conn)

            for index, row in df.iterrows():
                ClassName = row['ClassName']
                DailyCoins = row['DailyCoins']
                result_str += "日期:{}  - 投币数量:{}\n".format(ClassName, DailyCoins)

            print("6 1.1 Data inserted successfully!")

            return result_str

        except Exception as error:
            print("Error while calculating coin fluctuation:", error)

    def calculate_coin_proportion(self, startdate, enddate, store_name):
        """6 1.2分析南通店第14周的各类机器机台投币量占比"""
        try:
            conn = self.conn

            query = """
            SELECT
                mmt.TypeName,
                SUM(gem.CoinsNum) AS TotalCoins,
               ROUND( 100*(SUM(gem.CoinsNum)/SUM(SUM(gem.CoinsNum)) OVER()) ,2)AS percentage
            FROM
                game_everydaymachineincoins gem
                INNER JOIN game_machine gm ON gem.Machine = gm.ID
                INNER JOIN game_machinetype mmt ON gm.GameType = mmt.ID
                INNER JOIN mall_period mp ON gem.InPeriod = mp.ID 
            WHERE
                mp.ClassTime BETWEEN '{}' AND '{}'
                AND gem.OwnedBusiness = (SELECT ID FROM mall_business WHERE BusinessName = '{}') 
            GROUP BY
                mmt.TypeName;
            """.format(startdate, enddate, store_name)

            result_str = ""
            df = pd.read_sql(query, con=conn)

            for index, row in df.iterrows():
                TypeName = row['TypeName']
                TotalCoins = row['TotalCoins']
                Percentage = row['percentage']
                result_str += "机器类型:{}  - 总投币量:{} - 占比:{}%\n".format(TypeName, TotalCoins, Percentage)

            print("6 1.2 Data inserted successfully!")

            return result_str

        except Exception as error:
            print("Error while calculating coin proportion:", error)

    def calculate_startup_proportion(self, startdate, enddate, store_name):
        """6 1.3分析南通店第14周的各类机器机台启动次数占比"""
        try:
            conn = self.conn

            query = """
            SELECT
                mt.TypeName,
                SUM(vm.StartUpcount) AS StartUpCount,
                ROUND( 100*(SUM(vm.StartUpcount) / SUM(SUM(vm.StartUpcount)) OVER ()) ,0)AS percentage
            FROM
                view_machinesalequerysum vm
                INNER JOIN game_machine gm ON vm.MachineID = gm.ID
                INNER JOIN game_machinetype mt ON gm.GameType = mt.ID
                INNER JOIN mall_period mp ON vm.InPeriod = mp.ID
                INNER JOIN mall_business mb ON vm.OwnedBusiness = mb.ID
            WHERE
                mb.BusinessName = '{}'
                AND mp.ClassTime BETWEEN '{}' AND '{}'
                AND mt.TypeName IN ('娱乐机', '彩票机', '8寸娃娃机', '7寸娃娃机', '礼品机', '模拟机')
            GROUP BY
                mt.TypeName;
            """.format(store_name, startdate, enddate)

            result_str = ""
            df = pd.read_sql(query, con=conn)

            for index, row in df.iterrows():
                TypeName = row['TypeName']
                StartUpCount = row['StartUpCount']
                Percentage = row['percentage']
                result_str += "机器类型:{}  - 启动次数:{} - 占比:{}%\n".format(TypeName, StartUpCount, Percentage)

            print("6 1.3 Data inserted successfully!")

            return result_str

        except Exception as error:
            print("Error while calculating startup proportion:", error)

    def compare_hot_machines(self, startdate, enddate, store_name):
        """6 2.1.分析南通店的第14周的热门机器类型和投币量"""
        try:
            conn = self.conn

            query = """
            SELECT
                MachineName,
                Period,
                SUM(InTotalCoin - OutTotalCoin) AS DailyNetCoin 
            FROM
                view_machinereturnrate 
            WHERE
                Period BETWEEN '{}' AND '{}'
                AND OwnedBusiness IN (SELECT ID FROM mall_business WHERE BusinessName = '{}') 
            GROUP BY
                Period 
            ORDER BY
                DailyNetCoin DESC
            LIMIT 10;
            """.format(startdate, enddate, store_name)

            result_str = ""
            df = pd.read_sql(query, con=conn)

            for index, row in df.iterrows():
                MachineName = row['MachineName']
                Period = row['Period']
                DailyNetCoin = row['DailyNetCoin']
                result_str += "机器名称:{}  - 日期:{} - 日净投币量:{}\n".format(MachineName, Period, DailyNetCoin)

            print("6 2.1 Data inserted successfully!")

            return result_str

        except Exception as error:
            print("Error while comparing hot machines:", error)

    def analyze_hot_machines(self, startdate, enddate, store_name):
        """6 2.2分析合肥店的第14周的热门机器类型和投币量"""
        try:
            conn = self.conn

            query = """
            SELECT
                MachineName,
                Period,
                SUM(InTotalCoin - OutTotalCoin) AS DailyNetCoin 
            FROM
                view_machinereturnrate 
            WHERE
                Period BETWEEN '{}' AND '{}'
                AND OwnedBusiness IN (SELECT ID FROM mall_business WHERE BusinessName = '{}') 
            GROUP BY
                Period 
            ORDER BY
                DailyNetCoin DESC
            LIMIT 10;
            """.format(startdate, enddate, store_name)

            result_str = ""
            df = pd.read_sql(query, con=conn)

            for index, row in df.iterrows():
                MachineName = row['MachineName']
                Period = row['Period']
                DailyNetCoin = row['DailyNetCoin']
                result_str += "机器名称:{}  - 日期:{} - 日净投币量:{}\n".format(MachineName, Period, DailyNetCoin)

            print("6 2.2 Data inserted successfully!")

            return result_str

        except Exception as error:
            print("Error while comparing hot machines:", error)

    def calculate_lottery_win_rate(self, startdate, enddate, store_name):
        """6 2.4 计算第14周的南通店热门彩票机（2.1结论）的出奖率"""
        try:
            conn = self.conn

            query = """
            SELECT
                v.MachineName,
                v.TypeName,
                SUM(v.OutTotalTicket) AS OutTotalTicket,
                SUM(v.InTotalCoin) AS InTotalCoin,
                ROUND(IF(SUM(v.InTotalCoin) > 0, SUM(v.OutTotalTicket)/SUM(v.InTotalCoin), NULL) ,2)AS PayoutRate 
            FROM
                view_machinereturnrate v
                INNER JOIN game_machine gm ON v.MachineId = gm.ID 
            WHERE
                v.MachineName IN ('欢乐水果', '小熊保龄球', '神秘海域', '火星计划')
                AND v.Period BETWEEN '{}' AND '{}'
                AND gm.OwnedBusiness IN (SELECT ID FROM mall_business WHERE BusinessName = '{}') 
            GROUP BY
                v.MachineName,
                v.TypeName;
            """.format(startdate, enddate, store_name)

            result_str = ""
            df = pd.read_sql(query, con=conn)

            for index, row in df.iterrows():
                MachineName = row['MachineName']
                TypeName = row['TypeName']
                OutTotalTicket = row['OutTotalTicket']
                InTotalCoin = row['InTotalCoin']
                PayoutRate = row['PayoutRate']

                result_str += "机器名称:{} - 机器类型:{} - 出票总数:{} - 投币总数:{} - 中奖率:{}%  \n".format(MachineName,
                                                                                                           TypeName,
                                                                                                           OutTotalTicket,
                                                                                                           InTotalCoin,
                                                                                                           PayoutRate)

            print("6 2.4 Data inserted successfully!")

            return result_str

        except Exception as error:
            print("Error while calculating lottery win rate:", error)

    def calculate_claw_machine_startup(self, startdate, enddate, store_name):
        """6 2.5 计算第14周的南通店娃娃机启动次数，找出前十热门机器"""
        try:
            conn = self.conn

            query = """
            SELECT
                mp.ClassName,
                ROUND(SUM(vmsq.startupcount) ,0) AS DailyStartupCount,
                vmsq.MachineName
            FROM
                view_machinesalequerysum vmsq
                JOIN mall_period mp ON vmsq.InPeriod = mp.ID
            WHERE
                mp.ClassTime BETWEEN '{}' AND '{}'
                AND vmsq.OwnedBusiness IN (SELECT ID FROM mall_business WHERE BusinessName = '{}')
                AND vmsq.TypeName IN ('7寸娃娃机', '8寸娃娃机', '15寸娃娃机')
            GROUP BY
                vmsq.MachineName, mp.ClassName
            ORDER BY
                DailyStartupCount DESC
            LIMIT 10;
            """.format(startdate, enddate, store_name)

            result_str = ""
            df = pd.read_sql(query, con=conn)

            for index, row in df.iterrows():
                ClassName = row['ClassName']
                DailyStartupCount = row['DailyStartupCount']
                MachineName = row['MachineName']
                result_str += "日期:{} - 每日启动次数:{} - 机器名称:{}\n".format(ClassName, DailyStartupCount,
                                                                                     MachineName)

            print("6 2.5 Data inserted successfully!")

            return result_str

        except Exception as error:
            print("Error while calculating claw machine startup:", error)

    def analyze_machine_coin_output_issue(self, startdate, enddate, store_name):
        """7 1.1分析南通店第14周各类机器的投币产出异常"""
        try:
            conn = self.conn

            query = """
            SELECT
                MachineName,
                TypeName,
                '投币为0' AS ExceptionType 
            FROM
                view_machinereturnrate 
            WHERE
                (
                    ( TypeName IN ( '礼品机', '15寸娃娃机', '8寸娃娃机', '7寸娃娃机', '单人娃娃机' ) AND ( InTotalCoin = 0 AND OutGift != 0 ) )
                    OR ( TypeName IN ( '彩票机', '彩票机2' ) AND ( InTotalCoin = 0 AND OutTotalTicket != 0 ) )
                    OR ( TypeName = '娱乐机' AND OutTotalCoin != 0 AND InTotalCoin = 0 )
                )
                AND ClassTime BETWEEN '{}' AND '{}'
                AND OwnedBusiness IN (
                    SELECT
                        ID
                    FROM
                        mall_business
                    WHERE
                        BusinessName = '{}'
                );
            """.format(startdate, enddate, store_name)

            result_str = ""
            df = pd.read_sql(query, con=conn)

            for index, row in df.iterrows():
                MachineName = row['MachineName']
                TypeName = row['TypeName']
                ExceptionType = row['ExceptionType']
                result_str += "机器名:{}  - 类型:{}  - 异常类型:{}\n".format(MachineName, TypeName, ExceptionType)

            print("7 1.1 Data inserted successfully!")

            return result_str

        except Exception as error:
            print("Error while analyzing machine coin output issue:", error)

    def calculate_machine_prize_issue(self,startdate, enddate, store_name):
        """7 1.2计算南通店第14周各类机器出奖异常次数"""
        try:
            conn = self.conn

            query = """
            SELECT
                (
                    SELECT
                        COUNT(*)
                    FROM
                        view_machinereturnrate
                    WHERE
                        TypeName IN ( '7寸娃娃机' )
                        AND ( InTotalCoin / OutGift < 12 OR InTotalCoin / OutGift > 16 )
                        AND NOT (InTotalCoin = 0 AND OutGift != 0)
                        AND ClassTime BETWEEN '{}' AND '{}'
                        AND OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '{}' )
                ) AS abnormal_7inch_doll,
                (
                    SELECT
                        COUNT(*)
                    FROM
                        view_machinereturnrate
                    WHERE
                        TypeName = '8寸娃娃机'
                        AND ( InTotalCoin / OutGift < 33 OR InTotalCoin / OutGift > 39 )
                        AND NOT (InTotalCoin = 0 AND OutGift != 0)
                        AND ClassTime BETWEEN '{}' AND '{}'
                        AND OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '{}' )
                ) AS abnormal_8inch_doll,
                (
                    SELECT
                        COUNT(*)
                    FROM
                        view_machinereturnrate
                    WHERE
                        TypeName IN ( '彩票机', '彩票机2' )
                        AND ( OutTotalTicket / InTotalCoin < 30 OR OutTotalTicket / InTotalCoin > 40 )
                        AND NOT (InTotalCoin = 0 AND OutTotalTicket != 0)
                       AND ClassTime BETWEEN '{}' AND '{}'
                        AND OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '{}' )
                ) AS abnormal_ticket_machine,
                (
                    SELECT
                        COUNT(*)
                    FROM
                        view_machinereturnrate
                    WHERE
                        TypeName = '娱乐机'
                        AND OutTotalCoin / InTotalCoin > 1
                        AND NOT ( OutTotalCoin != 0 AND InTotalCoin = 0)
                        AND ClassTime BETWEEN '{}' AND '{}'
                        AND OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '{}' )
                ) AS abnormal_entertainment_machine;
            """.format(startdate, enddate, store_name,startdate, enddate, store_name,startdate, enddate, store_name,startdate, enddate, store_name)

            result_str = ""
            df = pd.read_sql(query, con=conn)

            result_str += "异常7寸娃娃机数:{}\n".format(df['abnormal_7inch_doll'].iloc[0])
            result_str += "异常8寸娃娃机数:{}\n".format(df['abnormal_8inch_doll'].iloc[0])
            result_str += "异常彩票机数:{}\n".format(df['abnormal_ticket_machine'].iloc[0])
            result_str += "异常娱乐机数:{}\n".format(df['abnormal_entertainment_machine'].iloc[0])

            print("7 1.2 Data inserted successfully!")

            return result_str

        except Exception as error:
            print("Error while calculating machine prize issue:", error)

    def analyze_machine_fault_issue(self, startdate, enddate, store_name):
        """7 1.3分析南通店第14周故障次数异常的机器"""
        try:
            conn = self.conn

            query = """
            SELECT
                gm.MachineName AS MachineName,
                gm.MachineNO AS MachineNo,
                gmt.TypeName AS TypeName,
                COUNT(*) AS FaultCount
            FROM
                game_machine gm
                JOIN game_machinealarmlog gmal ON gm.Terminal = gmal.OperateTerminal
                JOIN mall_period mp ON gmal.InPeriod = mp.ID
                JOIN game_machinetype gmt ON gm.GameType = gmt.ID 
            WHERE
                mp.ClassName BETWEEN '{}' AND '{}'
                AND gmal.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '{}' ) 
            GROUP BY
                gm.MachineName, gmt.TypeName 
            ORDER BY
                FaultCount DESC;
            """.format(startdate, enddate, store_name)

            result_str = ""
            df = pd.read_sql(query, con=conn)

            for index, row in df.iterrows():
                MachineName = row['MachineName']
                MachineNo = row['MachineNo']
                TypeName = row['TypeName']
                FaultCount = row['FaultCount']
                result_str += "机器名:{}  - 机器编号:{}  - 类型:{}  - 故障次数:{}\n".format(MachineName, MachineNo,
                                                                                            TypeName, FaultCount)

            print("7 1.3 Data inserted successfully!")

            return result_str

        except Exception as error:
            print("Error while analyzing machine fault issue:", error)

    # -----------------------------以上是根据自动化周报新加fc-------------------------------------------------------------
    def check_businessname(self, BusinessName):

        for i in bussname_list:
            if i in str(BusinessName) or str(BusinessName) in str(i):
                return True, str(i)

        return False, (
            "The BusinessName is wrong, please check! The BusinessName should be one of the following list ：" + '\n' + str(
            bussname_list))

    def get_total_daily_cost(self, date, BusinessName):
        """按日计算某个店铺日成本"""
        try:
            conn = self.conn

            check_result, BusinessName = self.check_businessname(BusinessName)
            if not check_result:
                return BusinessName

            query = """SELECT
	Doll7Recycle.EDAY,(
		ROUND(
		(((CASE		WHEN Doll7Prize.prize_amount IS NULL THEN	0 ELSE Doll7Prize.prize_amount  END ) - ( CASE WHEN Doll7Recycle.recycle_amount IS NULL THEN 0 ELSE Doll7Recycle.recycle_amount END ) ) * 2.55 +
		(CASE WHEN Doll7Recycle.recycle_amount IS NULL THEN 0 ELSE Doll7Recycle.recycle_amount END 	) * 1.2 	),2 	) + 
		ROUND(
		(((CASE	WHEN Doll8Prize.prize_amount IS NULL THEN		0 ELSE Doll8Prize.prize_amount 		END 		)- ( CASE WHEN Doll8Recycle.recycle_amount IS NULL THEN 0 ELSE Doll8Recycle.recycle_amount END ))* 5.5 + 
		( CASE WHEN Doll8Recycle.recycle_amount IS NULL THEN 0 ELSE Doll8Recycle.recycle_amount END ) * 2.4 	),	2 	) +
		ROUND((((CASE WHEN Doll15Prize.prize_amount IS NULL THEN	0 ELSE Doll15Prize.prize_amount END ) - ( CASE WHEN Doll15Recycle.recycle_amount IS NULL THEN 0 ELSE Doll15Recycle.recycle_amount END ))* 20 + 		( CASE WHEN Doll15Recycle.recycle_amount IS NULL THEN 0 ELSE Doll15Recycle.recycle_amount END ) * 14.4 		),	2 	) + 
		( CASE WHEN Lottery.TotalTicket IS NULL THEN 0 ELSE Lottery.TotalTicket END ) + 
		( CASE WHEN GiftMachine.total_cost IS NULL THEN 0 ELSE GiftMachine.total_cost END ) 
) AS total_cost 
FROM(
					SELECT
							dates.Date AS EDAY,
							COALESCE(SUM(RecycledAmount), 0) AS recycle_amount 
					FROM
							(
									SELECT DISTINCT mp.ClassName AS Date
									FROM mall_period mp
							) dates
							LEFT JOIN
							(
									SELECT mp.ClassName, COALESCE(SUM(Amount), 0) AS RecycledAmount
									FROM view_giftrecoverlogitem gf
									JOIN mall_period mp ON gf.InPeriod = mp.ID 
									WHERE (GoodName = '7寸娃娃' OR GoodName = '普通娃娃') 
									AND gf.OwnedBusiness IN (SELECT ID FROM mall_business WHERE BusinessName = '%s')
									GROUP BY mp.ClassName
							) recycled ON dates.Date = recycled.ClassName
					GROUP BY dates.Date
) AS Doll7Recycle
LEFT JOIN (
					SELECT
						Period AS EDAY,
						COALESCE ( SUM( OutGift ), 0 ) AS prize_amount 
					FROM
						view_machinereturnrate 
					WHERE
						(TypeName = '7寸娃娃机'  OR TypeName ='单人娃娃机')
						AND OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' ) 
					GROUP BY
						Period 
) AS Doll7Prize ON Doll7Recycle.EDAY = Doll7Prize.EDAY
LEFT JOIN (
					SELECT
						mp.ClassName AS EDAY,
						COALESCE ( SUM( Amount ), 0 ) AS recycle_amount 
					FROM
						view_giftrecoverlogitem gf
						JOIN mall_period mp ON gf.InPeriod = mp.ID 
					WHERE
						GoodName = '8寸娃娃' 
						AND gf.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' ) 
					GROUP BY
						mp.ClassName 
) AS Doll8Recycle ON Doll7Recycle.EDAY = Doll8Recycle.EDAY
LEFT JOIN (
					SELECT
						vm.Period AS EDAY,
						COALESCE ( SUM( OutGift ), 0 ) AS prize_amount 
					FROM
						view_machinereturnrate vm 
					WHERE
						TypeName = '8寸娃娃机' 
						AND vm.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' ) 
					GROUP BY
						vm.Period 
) AS Doll8Prize ON Doll7Recycle.EDAY = Doll8Prize.EDAY
LEFT JOIN (
					SELECT
						mp.ClassName AS EDAY,
						COALESCE ( SUM( Amount ), 0 ) AS recycle_amount 
					FROM
						view_giftrecoverlogitem gf
						JOIN mall_period mp ON gf.InPeriod = mp.ID 
					WHERE
						GoodName = '15寸娃娃' 
						AND gf.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' ) 
					GROUP BY
						mp.ClassName 
) AS Doll15Recycle ON Doll7Recycle.EDAY = Doll15Recycle.EDAY
LEFT JOIN (
					SELECT
						vm.Period AS EDAY,
						COALESCE ( SUM( OutGift ), 0 ) AS prize_amount 
					FROM
						view_machinereturnrate vm 
					WHERE
						TypeName = '15寸娃娃机' 
						AND vm.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' ) 
					GROUP BY
						vm.Period 
) AS Doll15Prize ON Doll7Recycle.EDAY = Doll15Prize.EDAY
LEFT JOIN (
					SELECT
						vm.Period AS EDAY,
						(
						SUM( OutPhTicket ) + SUM( OutTicket ) + SUM( OutPhTicket ))/ 1000 AS TotalTicket 
					FROM
						view_machinereturnrate vm 
					WHERE
						TypeName = '彩票机' 
						AND vm.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' ) 
					GROUP BY
						vm.Period 
) AS Lottery ON Doll7Recycle.EDAY = Lottery.EDAY
LEFT JOIN (
					SELECT
						mp.ClassName AS EDAY,
						SUM(
						ABS( gf.TotleMoney )) AS total_cost 
					FROM
						mall_stockchangelog gf
						JOIN mall_period mp ON gf.InPeriod = mp.ID
						JOIN mall_goodbase gb ON gf.Goods = gb.ID 
					WHERE
						gf.OrderType = '2' 
						AND gb.GoodName NOT LIKE '7寸娃娃' 
						AND gb.GoodName NOT LIKE '8寸娃娃' 
						AND gb.GoodName NOT LIKE '普通娃娃' 
						AND gb.GoodName NOT LIKE '红票' 
						AND 	gb.GoodName NOT LIKE '实物币(实物币)' 
						AND gf.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' ) 
					GROUP BY
						mp.ClassName 
) AS GiftMachine ON Doll7Recycle.EDAY = GiftMachine.EDAY 
				
WHERE
	Doll7Recycle.EDAY = '%s' """ % (
                BusinessName, BusinessName, BusinessName, BusinessName, BusinessName, BusinessName, BusinessName,
                BusinessName, date)
            df = pd.read_sql(query, con=conn)
            return df

            print("Data inserted successfully!")
        except Exception as error:
            print("Error while inserting data:", error)

    def get_total_daily_cost_rate(self, date, BusinessName):
        """按日计算某个店铺日成本率"""
        try:
            conn = self.conn

            check_result, BusinessName = self.check_businessname(BusinessName)
            if not check_result:
                return BusinessName

            query = """WITH DailyCost AS (
	SELECT
		Doll7Recycle.EDAY,
		(
			ROUND((((
						CASE
								
								WHEN Doll7Prize.prize_amount IS NULL THEN
								0 ELSE Doll7Prize.prize_amount 
							END 
							) - ( CASE WHEN Doll7Recycle.recycle_amount IS NULL THEN 0 ELSE Doll7Recycle.recycle_amount END ) 
							) * 2.55 +(
						CASE
								
								WHEN Doll7Recycle.recycle_amount IS NULL THEN
								0 ELSE Doll7Recycle.recycle_amount 
							END 
							) * 1.2 
							),
						2 
						) + ROUND((((
								CASE
										
										WHEN Doll8Prize.prize_amount IS NULL THEN
										0 ELSE Doll8Prize.prize_amount 
									END 
									)- ( CASE WHEN Doll8Recycle.recycle_amount IS NULL THEN 0 ELSE Doll8Recycle.recycle_amount END ) 
									) * 5.5 + ( CASE WHEN Doll8Recycle.recycle_amount IS NULL THEN 0 ELSE Doll8Recycle.recycle_amount END ) * 2.4 
								),
							2 
							) + ROUND((((
									CASE
											
											WHEN Doll15Prize.prize_amount IS NULL THEN
											0 ELSE Doll15Prize.prize_amount 
										END 
										) - ( CASE WHEN Doll15Recycle.recycle_amount IS NULL THEN 0 ELSE Doll15Recycle.recycle_amount END ) 
										) * 20 + ( CASE WHEN Doll15Recycle.recycle_amount IS NULL THEN 0 ELSE Doll15Recycle.recycle_amount END ) * 14.4 
									),
								2 
							) + ( CASE WHEN Lottery.TotalTicket IS NULL THEN 0 ELSE Lottery.TotalTicket END ) + ( CASE WHEN GiftMachine.total_cost IS NULL THEN 0 ELSE GiftMachine.total_cost END ) 
						) AS total_cost 
FROM(
					SELECT
							dates.Date AS EDAY,
							COALESCE(SUM(RecycledAmount), 0) AS recycle_amount 
					FROM
							(
									SELECT DISTINCT mp.ClassName AS Date
									FROM mall_period mp
							) dates
							LEFT JOIN
							(
									SELECT mp.ClassName, COALESCE(SUM(Amount), 0) AS RecycledAmount
									FROM view_giftrecoverlogitem gf
									JOIN mall_period mp ON gf.InPeriod = mp.ID 
									WHERE (GoodName = '7寸娃娃' OR GoodName = '普通娃娃') 
									AND gf.OwnedBusiness IN (SELECT ID FROM mall_business WHERE BusinessName = '%s')
									GROUP BY mp.ClassName
							) recycled ON dates.Date = recycled.ClassName
					GROUP BY dates.Date
) AS Doll7Recycle
LEFT JOIN (
					SELECT
						Period AS EDAY,
						COALESCE ( SUM( OutGift ), 0 ) AS prize_amount 
					FROM
						view_machinereturnrate 
					WHERE
						(TypeName = '7寸娃娃机'  OR TypeName ='单人娃娃机')
						AND OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' ) 
					GROUP BY
						Period 
											
) AS Doll7Prize ON Doll7Recycle.EDAY = Doll7Prize.EDAY
LEFT JOIN (
						SELECT
							mp.ClassName AS EDAY,
							COALESCE ( SUM( Amount ), 0 ) AS recycle_amount 
						FROM
							view_giftrecoverlogitem gf
							JOIN mall_period mp ON gf.InPeriod = mp.ID 
						WHERE
							gf.GoodName = '8寸娃娃' 
							AND gf.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' ) 
						GROUP BY
							mp.ClassName 
) AS Doll8Recycle ON Doll7Recycle.EDAY = Doll8Recycle.EDAY
LEFT JOIN (
						SELECT
							vm.Period AS EDAY,
							COALESCE ( SUM( OutGift ), 0 ) AS prize_amount 
						FROM
							view_machinereturnrate vm 
						WHERE
							vm.TypeName = '8寸娃娃机' 
							AND vm.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' ) 
						GROUP BY
							vm.Period 
) AS Doll8Prize ON Doll7Recycle.EDAY = Doll8Prize.EDAY
LEFT JOIN (
						SELECT
							mp.ClassName AS EDAY,
							COALESCE ( SUM( Amount ), 0 ) AS recycle_amount 
						FROM
							view_giftrecoverlogitem gf
							JOIN mall_period mp ON gf.InPeriod = mp.ID 
						WHERE
							gf.GoodName = '15寸娃娃' 
							AND gf.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' ) 
						GROUP BY
							mp.ClassName 
						) AS Doll15Recycle ON Doll7Recycle.EDAY = Doll15Recycle.EDAY
LEFT JOIN (
						SELECT
							vm.Period AS EDAY,
							COALESCE ( SUM( OutGift ), 0 ) AS prize_amount 
						FROM
							view_machinereturnrate vm 
						WHERE
							vm.TypeName = '15寸娃娃机' 
							AND vm.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' ) 
						GROUP BY
							vm.Period 
) AS Doll15Prize ON Doll7Recycle.EDAY = Doll15Prize.EDAY
LEFT JOIN (
						SELECT
							vm.Period AS EDAY,
							(
							SUM( OutPhTicket ) + SUM( OutTicket ) + SUM( OutPhTicket ))/ 1000 AS TotalTicket 
						FROM
							view_machinereturnrate vm 
						WHERE
							vm.TypeName = '彩票机' 
							AND vm.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' ) 
						GROUP BY
							vm.Period 
) AS Lottery ON Doll7Recycle.EDAY = Lottery.EDAY
LEFT JOIN (
						SELECT
							mp.ClassName AS EDAY,
							SUM(
							ABS( gf.TotleMoney )) AS total_cost 
						FROM
							mall_stockchangelog gf
							JOIN mall_period mp ON gf.InPeriod = mp.ID
							JOIN mall_goodbase gb ON gf.Goods = gb.ID 
						WHERE
							gf.OrderType = '2' 
							AND gb.GoodName NOT LIKE '7寸娃娃' 
							AND gb.GoodName NOT LIKE '8寸娃娃' 
							AND gb.GoodName NOT LIKE '普通娃娃' 
							AND gb.GoodName NOT LIKE '红票' 
							AND gb.GoodName NOT LIKE '实物币(实物币)'  
							AND gf.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' ) 
						GROUP BY
							mp.ClassName 
) AS GiftMachine ON Doll7Recycle.EDAY = GiftMachine.EDAY 
GROUP BY
						Doll7Recycle.EDAY 
ORDER BY
						Doll7Recycle.EDAY 
),
						
DailyCoinPrice AS (
		WITH temp AS (
						SELECT
							Period AS EDAY,
							'total_revenue' AS rname,
							SUM( SellMoney ) AS count 
						FROM
							view_cmsalelogdetail 
						WHERE
							OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' ) 
							AND ChannelName NOT LIKE '本店'
						GROUP BY
							Period 
			UNION
						SELECT
							Period AS EDAY,
							'TotalCoin' AS rname,
							SUM( sale.ECoinAmount + sale.PCoinAmount + sale.GoldCoinAmount ) AS count 
						FROM
							view_cmsalelogdetail AS sale 
						WHERE
							sale.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' ) 
							AND sale.ChannelName NOT LIKE '本店'
						GROUP BY
							Period UNION
						SELECT
							giving.ClassName AS EDAY,
							'givingcoin' AS rname,
							SUM( giving.CoinNumber + giving.GoldCoinNumber + giving.PhysicalCoinNumber ) AS count 
						FROM
							view_givingcoinlogquery AS giving 
						WHERE
							giving.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' ) 
						GROUP BY
							giving.ClassName 
						) SELECT
						t0.EDAY,((
							SELECT
								count 
							FROM
								temp t1 
							WHERE
								rname = 'total_revenue' 
								AND t1.EDAY = t0.EDAY 
							) / ( SELECT SUM( count ) FROM temp t2 WHERE rname IN ( 'TotalCoin', 'givingcoin' ) AND t2.EDAY = t0.EDAY )) AS coin_price 
					FROM
						temp t0 
					GROUP BY
						t0.EDAY 
					),
DailyCoinTotal AS (
					SELECT
							DATE( Period ) AS EDAY,
							SUM( InTotalCoin ) AS InC,
							SUM( OutPhCoin + OutCoin ) AS OutC,
							(
							SUM( InTotalCoin ) - SUM( OutPhCoin + OutCoin )) AS InTotal 
						FROM
							view_machinereturnrate 
						WHERE
							OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' ) 
						GROUP BY
							DATE( Period )
) 
SELECT
					DailyCost.EDAY,
					COALESCE (
					 DailyCost.total_cost ) / ( DailyCoinTotal.InTotal * DailyCoinPrice.coin_price )  AS daily_cost_rate
				FROM
					DailyCost
					JOIN DailyCoinTotal ON DailyCost.EDAY = DailyCoinTotal.EDAY
					JOIN DailyCoinPrice ON DailyCost.EDAY = DailyCoinPrice.EDAY 
			WHERE
	DailyCost.EDAY = '%s' """ % (
                BusinessName, BusinessName, BusinessName, BusinessName, BusinessName, BusinessName, BusinessName,
                BusinessName, BusinessName, BusinessName, BusinessName, BusinessName, date)
            df = pd.read_sql(query, con=conn)
            return df

            print("Data inserted successfully!")
        except Exception as error:
            print("Error while inserting data:", error)

    def get_total_daily_profit_rate(self, date, BusinessName):
        """按日计算某个门店日利润率"""
        try:
            conn = self.conn

            check_result, BusinessName = self.check_businessname(BusinessName)
            if not check_result:
                return BusinessName

            query = """WITH DailyCost AS (
	SELECT
		Doll7Recycle.EDAY,
		(
			ROUND((((
						CASE
								
								WHEN Doll7Prize.prize_amount IS NULL THEN
								0 ELSE Doll7Prize.prize_amount 
							END 
							) - ( CASE WHEN Doll7Recycle.recycle_amount IS NULL THEN 0 ELSE Doll7Recycle.recycle_amount END ) 
							) * 2.55 +(
						CASE
								
								WHEN Doll7Recycle.recycle_amount IS NULL THEN
								0 ELSE Doll7Recycle.recycle_amount 
							END 
							) * 1.2 
							),
						2 
						) + ROUND((((
								CASE
										
										WHEN Doll8Prize.prize_amount IS NULL THEN
										0 ELSE Doll8Prize.prize_amount 
									END 
									)- ( CASE WHEN Doll8Recycle.recycle_amount IS NULL THEN 0 ELSE Doll8Recycle.recycle_amount END ) 
									) * 5.5 + ( CASE WHEN Doll8Recycle.recycle_amount IS NULL THEN 0 ELSE Doll8Recycle.recycle_amount END ) * 2.4 
								),
							2 
							) + ROUND((((
									CASE
											
											WHEN Doll15Prize.prize_amount IS NULL THEN
											0 ELSE Doll15Prize.prize_amount 
										END 
										) - ( CASE WHEN Doll15Recycle.recycle_amount IS NULL THEN 0 ELSE Doll15Recycle.recycle_amount END ) 
										) * 20 + ( CASE WHEN Doll15Recycle.recycle_amount IS NULL THEN 0 ELSE Doll15Recycle.recycle_amount END ) * 14.4 
									),
								2 
							) + ( CASE WHEN Lottery.TotalTicket IS NULL THEN 0 ELSE Lottery.TotalTicket END ) + ( CASE WHEN GiftMachine.total_cost IS NULL THEN 0 ELSE GiftMachine.total_cost END ) 
						) AS total_cost 
FROM(
					SELECT
							dates.Date AS EDAY,
							COALESCE(SUM(RecycledAmount), 0) AS recycle_amount 
					FROM
							(
									SELECT DISTINCT mp.ClassName AS Date
									FROM mall_period mp
							) dates
							LEFT JOIN
							(
									SELECT mp.ClassName, COALESCE(SUM(Amount), 0) AS RecycledAmount
									FROM view_giftrecoverlogitem gf
									JOIN mall_period mp ON gf.InPeriod = mp.ID 
									WHERE (GoodName = '7寸娃娃' OR GoodName = '普通娃娃') 
									AND gf.OwnedBusiness IN (SELECT ID FROM mall_business WHERE BusinessName = '%s')
									GROUP BY mp.ClassName
							) recycled ON dates.Date = recycled.ClassName
					GROUP BY dates.Date
) AS Doll7Recycle
LEFT JOIN (
					SELECT
						Period AS EDAY,
						COALESCE ( SUM( OutGift ), 0 ) AS prize_amount 
					FROM
						view_machinereturnrate 
					WHERE
						(TypeName = '7寸娃娃机'  OR TypeName ='单人娃娃机')
						AND OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' ) 
					GROUP BY
						Period 
											
) AS Doll7Prize ON Doll7Recycle.EDAY = Doll7Prize.EDAY
LEFT JOIN (
						SELECT
							mp.ClassName AS EDAY,
							COALESCE ( SUM( Amount ), 0 ) AS recycle_amount 
						FROM
							view_giftrecoverlogitem gf
							JOIN mall_period mp ON gf.InPeriod = mp.ID 
						WHERE
							gf.GoodName = '8寸娃娃' 
							AND gf.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' ) 
						GROUP BY
							mp.ClassName 
) AS Doll8Recycle ON Doll7Recycle.EDAY = Doll8Recycle.EDAY
LEFT JOIN (
						SELECT
							vm.Period AS EDAY,
							COALESCE ( SUM( OutGift ), 0 ) AS prize_amount 
						FROM
							view_machinereturnrate vm 
						WHERE
							vm.TypeName = '8寸娃娃机' 
							AND vm.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' ) 
						GROUP BY
							vm.Period 
) AS Doll8Prize ON Doll7Recycle.EDAY = Doll8Prize.EDAY
LEFT JOIN (
						SELECT
							mp.ClassName AS EDAY,
							COALESCE ( SUM( Amount ), 0 ) AS recycle_amount 
						FROM
							view_giftrecoverlogitem gf
							JOIN mall_period mp ON gf.InPeriod = mp.ID 
						WHERE
							gf.GoodName = '15寸娃娃' 
							AND gf.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' ) 
						GROUP BY
							mp.ClassName 
						) AS Doll15Recycle ON Doll7Recycle.EDAY = Doll15Recycle.EDAY
LEFT JOIN (
						SELECT
							vm.Period AS EDAY,
							COALESCE ( SUM( OutGift ), 0 ) AS prize_amount 
						FROM
							view_machinereturnrate vm 
						WHERE
							vm.TypeName = '15寸娃娃机' 
							AND vm.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' ) 
						GROUP BY
							vm.Period 
) AS Doll15Prize ON Doll7Recycle.EDAY = Doll15Prize.EDAY
LEFT JOIN (
						SELECT
							vm.Period AS EDAY,
							(
							SUM( OutPhTicket ) + SUM( OutTicket ) + SUM( OutPhTicket ))/ 1000 AS TotalTicket 
						FROM
							view_machinereturnrate vm 
						WHERE
							vm.TypeName = '彩票机' 
							AND vm.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' ) 
						GROUP BY
							vm.Period 
) AS Lottery ON Doll7Recycle.EDAY = Lottery.EDAY
LEFT JOIN (
						SELECT
							mp.ClassName AS EDAY,
							SUM(
							ABS( gf.TotleMoney )) AS total_cost 
						FROM
							mall_stockchangelog gf
							JOIN mall_period mp ON gf.InPeriod = mp.ID
							JOIN mall_goodbase gb ON gf.Goods = gb.ID 
						WHERE
							gf.OrderType = '2' 
							AND gb.GoodName NOT LIKE '7寸娃娃' 
							AND gb.GoodName NOT LIKE '8寸娃娃' 
							AND gb.GoodName NOT LIKE '普通娃娃' 
							AND gb.GoodName NOT LIKE '红票' 
							AND gb.GoodName NOT LIKE '实物币(实物币)'  
							AND gf.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' ) 
						GROUP BY
							mp.ClassName 
) AS GiftMachine ON Doll7Recycle.EDAY = GiftMachine.EDAY 
GROUP BY
						Doll7Recycle.EDAY 
ORDER BY
						Doll7Recycle.EDAY 
),
						
DailyCoinPrice AS (
		WITH temp AS (
						SELECT
							Period AS EDAY,
							'total_revenue' AS rname,
							SUM( SellMoney ) AS count 
						FROM
							view_cmsalelogdetail 
						WHERE
							OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' ) 
							AND ChannelName NOT LIKE '本店'
						GROUP BY
							Period 
			UNION
						SELECT
							Period AS EDAY,
							'TotalCoin' AS rname,
							SUM( sale.ECoinAmount + sale.PCoinAmount + sale.GoldCoinAmount ) AS count 
						FROM
							view_cmsalelogdetail AS sale 
						WHERE
							sale.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' ) 
							AND sale.ChannelName NOT LIKE '本店'
						GROUP BY
							Period UNION
						SELECT
							giving.ClassName AS EDAY,
							'givingcoin' AS rname,
							SUM( giving.CoinNumber + giving.GoldCoinNumber + giving.PhysicalCoinNumber ) AS count 
						FROM
							view_givingcoinlogquery AS giving 
						WHERE
							giving.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' ) 
						GROUP BY
							giving.ClassName 
						) SELECT
						t0.EDAY,((
							SELECT
								count 
							FROM
								temp t1 
							WHERE
								rname = 'total_revenue' 
								AND t1.EDAY = t0.EDAY 
							) / ( SELECT SUM( count ) FROM temp t2 WHERE rname IN ( 'TotalCoin', 'givingcoin' ) AND t2.EDAY = t0.EDAY )) AS coin_price 
					FROM
						temp t0 
					GROUP BY
						t0.EDAY 
					),
DailyCoinTotal AS (
					SELECT
							DATE( Period ) AS EDAY,
							SUM( InTotalCoin ) AS InC,
							SUM( OutPhCoin + OutCoin ) AS OutC,
							(
							SUM( InTotalCoin ) - SUM( OutPhCoin + OutCoin )) AS InTotal 
						FROM
							view_machinereturnrate 
						WHERE
							OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' ) 
						GROUP BY
							DATE( Period )
) 
					
	SELECT
					DailyCost.EDAY,
					COALESCE (
					1 - ( DailyCost.total_cost ) / ( DailyCoinTotal.InTotal * DailyCoinPrice.coin_price )) AS daily_profit_rate 
				FROM
					DailyCost
					JOIN DailyCoinTotal ON DailyCost.EDAY = DailyCoinTotal.EDAY
					JOIN DailyCoinPrice ON DailyCost.EDAY = DailyCoinPrice.EDAY 
			WHERE
	DailyCost.EDAY = '%s' """ % (
                BusinessName, BusinessName, BusinessName, BusinessName, BusinessName, BusinessName, BusinessName,
                BusinessName, BusinessName, BusinessName, BusinessName, BusinessName, date)
            df = pd.read_sql(query, con=conn)
            return df

            print("Data inserted successfully!")
        except Exception as error:
            print("Error while inserting data:", error)

    def get_total_daily_income(self, date, BusinessName):
        """按日计算某个门店的日消费"""
        try:
            conn = self.conn

            check_result, BusinessName = self.check_businessname(BusinessName)
            if not check_result:
                return BusinessName

            query = """ WITH DailyCoinPrice AS (
/* 日的币单价*/
	WITH temp AS (
		SELECT
			Period AS EDAY,
			'total_revenue' AS rname,
			SUM( SellMoney ) AS count 
		FROM
			view_cmsalelogdetail 
		WHERE
			OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' ) 
			AND ChannelName NOT LIKE '本店'
		GROUP BY
			Period
			
			UNION
			
		SELECT
			Period AS EDAY,
			'TotalCoin' AS rname,
			SUM( sale.ECoinAmount + sale.PCoinAmount + sale.GoldCoinAmount ) AS count 
		FROM
			view_cmsalelogdetail AS sale 
		WHERE
			sale.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' ) 
			AND sale.ChannelName NOT LIKE '本店'
		GROUP BY
			Period 
			
			UNION
			
		SELECT
			giving.ClassName AS EDAY,
			'givingcoin' AS rname,
			SUM( giving.CoinNumber + giving.GoldCoinNumber + giving.PhysicalCoinNumber ) AS count 
		FROM
			view_givingcoinlogquery AS giving 
		WHERE
			giving.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' ) 
		GROUP BY
			giving.ClassName 
		) 
		
		SELECT
		t0.EDAY,((
			SELECT
				count 
			FROM
				temp t1 
			WHERE
				rname = 'total_revenue' 
				AND t1.EDAY = t0.EDAY 
			) / ( SELECT SUM( count ) FROM temp t2 WHERE rname IN ( 'TotalCoin', 'givingcoin' ) AND t2.EDAY = t0.EDAY )) AS coin_price 
	FROM
		temp t0 
	GROUP BY
		t0.EDAY 
	),
	
DailyCoinTotal AS (
/*日投币量*/
	SELECT
		DATE( Period ) AS EDAY,
		SUM( InTotalCoin ) AS InC,
		SUM( OutPhCoin + OutCoin ) AS OutC,
		(	SUM( InTotalCoin ) - SUM( OutPhCoin + OutCoin )) AS InTotal 
	FROM
		view_machinereturnrate 
	WHERE
		OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' ) 
	GROUP BY
		DATE( Period )
	) /*计算日收入*/
	
SELECT
	DailyCoinTotal.EDAY,
	COALESCE ( ( DailyCoinTotal.InTotal * DailyCoinPrice.coin_price ), 0 ) AS daily_cost 
FROM
	DailyCoinPrice
	JOIN DailyCoinTotal ON DailyCoinPrice.EDAY = DailyCoinTotal.EDAY 
WHERE
	DailyCoinTotal.EDAY = '%s' 
	""" % (BusinessName, BusinessName, BusinessName, BusinessName, date)
            df = pd.read_sql(query, con=conn)
            return df

            print("Data inserted successfully!")
        except Exception as error:
            print("Error while inserting data:", error)

    def get_total_monthly_cost(self, date, BusinessName):
        """按月计算全店总成本"""
        try:
            conn = self.conn

            check_result, BusinessName = self.check_businessname(BusinessName)
            if not check_result:
                return BusinessName

            query = """WITH Monthly_Inventory_List AS (
	SELECT
		DATE_FORMAT( mp.ClassName, '%%Y-%%m' ) AS yuefen,
		SUM( ms.CheckTotle ) AS price
	FROM
		mall_stockcheck ms
		JOIN mall_period mp ON ms.InPeriod = mp.ID
	WHERE
		ms.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' )
	GROUP BY
		yuefen
	),
	Monthly_Purchase_Order AS (
	SELECT
		DATE_FORMAT( mp.ClassName, '%%Y-%%m' ) AS yuefen,
		SUM( ms.TotleMoney ) AS price
	FROM
		mall_purchaseorder ms
		JOIN mall_period mp ON ms.InPeriod = mp.ID
	WHERE
		ms.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' )
	GROUP BY
		yuefen
	),
	Last_Monthly_Inventory_List AS (
	SELECT
		DATE_FORMAT( DATE_ADD( mp.ClassName, INTERVAL 1 MONTH ), '%%Y-%%m' ) AS next_month,
		SUM( ms.CheckTotle ) AS price
	FROM
		mall_stockcheck ms
		JOIN mall_period mp ON ms.InPeriod = mp.ID
	WHERE
		ms.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' )
	GROUP BY
		next_month
	) SELECT
	mit1.yuefen,
	( IFNULL(- mit1.price, 0 ) + IFNULL( mpd.price, 0 ) + IFNULL( mit2.price, 0 ) ) AS price
FROM
	Monthly_Inventory_List mit1
	LEFT JOIN Monthly_Purchase_Order mpd ON mit1.yuefen = mpd.yuefen
	LEFT JOIN Last_Monthly_Inventory_List mit2 ON mit1.yuefen = mit2.next_month
WHERE
	mit1.yuefen LIKE '%s'
GROUP BY
	mit1.yuefen
ORDER BY
	mit1.yuefen""" % (BusinessName, BusinessName, BusinessName, (str(date) + '%'))
            df = pd.read_sql(query, con=conn)
            return df

            print("Data inserted successfully!")
        except Exception as error:
            print("Error while inserting data:", error)

    def get_total_monthly_cost_rate(self, date, BusinessName):
        """按月计算全店月成本率"""
        try:
            conn = self.conn

            check_result, BusinessName = self.check_businessname(BusinessName)
            if not check_result:
                return BusinessName

            query = """WITH Monthly_Inventory_List AS (
	SELECT
		DATE_FORMAT( mp.ClassName, '%%Y-%%m' ) AS yuefen,
		SUM( ms.CheckTotle ) AS price
	FROM
		mall_stockcheck ms
		JOIN mall_period mp ON ms.InPeriod = mp.ID
	WHERE
		ms.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' )
	GROUP BY
		yuefen
	),
	Monthly_Purchase_Order AS (
	SELECT
		DATE_FORMAT( mp.ClassName, '%%Y-%%m' ) AS yuefen,
		SUM( ms.TotleMoney ) AS price
	FROM
		mall_purchaseorder ms
		JOIN mall_period mp ON ms.InPeriod = mp.ID
	WHERE
		ms.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' )
	GROUP BY
		yuefen
	),
	Last_Monthly_Inventory_List AS (
	SELECT
		DATE_FORMAT( DATE_ADD( mp.ClassName, INTERVAL 1 MONTH ), '%%Y-%%m' ) AS next_month,
		SUM( ms.CheckTotle ) AS price
	FROM
		mall_stockcheck ms
		JOIN mall_period mp ON ms.InPeriod = mp.ID
	WHERE
		ms.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' )
	GROUP BY
		next_month
	),
	Monthly_cost AS (
	SELECT
		mit1.yuefen AS yuefen,
		(- mit1.price + IFNULL( mpd.price, 0 ) + IFNULL( mit2.price, 0 ) ) AS price
	FROM
		Monthly_Inventory_List mit1
		LEFT JOIN Monthly_Purchase_Order mpd ON mit1.yuefen = mpd.yuefen
		LEFT JOIN Last_Monthly_Inventory_List mit2 ON mit1.yuefen = mit2.next_month
	GROUP BY
		mit1.yuefen
	),
	Monthly_Order_revenue AS (
/* 月订单营收 */
	SELECT
		DATE_FORMAT( mp.ClassName, '%%Y-%%m' ) AS yuefen,
		SUM( mo.PaidMoney ) AS OrderRevenue
	FROM
		mall_order mo
		JOIN mall_period mp ON mo.InPeriod = mp.ID
	WHERE
		mo.PayState <= 5000
		AND mo.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' )
	GROUP BY
		yuefen
	),
	Integralcost AS (
/* 积分成本 */
	SELECT
		DATE_FORMAT( mp.ClassName, '%%Y-%%m' ) AS yuefen,
		'积分' AS ValueName,
		( ( SUM( me.ThisRemain ) - SUM( me.PreRemain )) * 1.2 ) AS price
	FROM
		mall_everydayvalue AS me
		JOIN mall_leaguervaluetype AS ml ON me.Project = ml.ID
		JOIN mall_period mp ON me.InPeriod = mp.ID
	WHERE
		ml.ValueName LIKE '积分'
		AND me.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' )
	GROUP BY
		yuefen
	),
	Lotterycost AS (
/* 彩票成本 */
	SELECT
		DATE_FORMAT( mp.ClassName, '%%Y-%%m' ) AS yuefen,
		'彩票' AS ValueName,
		( ( ( SUM( me.ThisRemain ) - SUM( me.PreRemain )) / 1000 ) * 1 ) AS price
	FROM
		mall_everydayvalue AS me
		JOIN mall_leaguervaluetype AS ml ON me.Project = ml.ID
		JOIN mall_period mp ON me.InPeriod = mp.ID
	WHERE
		ml.ValueName LIKE '彩票'
		AND me.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' )
	GROUP BY
		yuefen
	),
	Tokencost AS (
/* 代币成本 */
	SELECT
		DATE_FORMAT( mp.ClassName, '%%Y-%%m' ) AS yuefen,
		'代币' AS ValueName,
		( ( SUM( me.ThisRemain ) - SUM( me.PreRemain )) * 0.185 ) AS price
	FROM
		mall_everydayvalue AS me
		JOIN mall_leaguervaluetype AS ml ON me.Project = ml.ID
		JOIN mall_period mp ON me.InPeriod = mp.ID
	WHERE
		ml.ValueName LIKE '代币'
		AND me.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' )
	GROUP BY
		yuefen
	),
	Dollcost AS (
/* 娃娃成本 */
	SELECT
		DATE_FORMAT( mp.ClassName, '%%Y-%%m' ) AS yuefen,
		'娃娃' AS ValueName,
		( ( SUM( me.ThisRemain ) - SUM( me.PreRemain )) * 0.185 ) AS price
	FROM
		mall_everydayvalue AS me
		JOIN mall_leaguervaluetype AS ml ON me.Project = ml.ID
		JOIN mall_period mp ON me.InPeriod = mp.ID
	WHERE
		ml.ValueName LIKE '娃娃'
		AND me.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' )
	GROUP BY
		yuefen
	),
	Monthly_revenue AS (
/* 计算月营收 */
	SELECT
		mr.yuefen AS yuefen,
		(
		IFNULL( mr.OrderRevenue, 0 ) - IFNULL( ic.price, 0 ) - IFNULL( lc.price, 0 ) - IFNULL( tc.price, 0 ) - IFNULL( dc.price, 0 )) AS MonthlyRevenue
	FROM
		Monthly_Order_revenue mr
		JOIN Integralcost ic ON mr.yuefen = ic.yuefen
		JOIN Lotterycost lc ON mr.yuefen = lc.yuefen
		JOIN Tokencost tc ON mr.yuefen = tc.yuefen
		JOIN Dollcost dc ON mr.yuefen = dc.yuefen
	GROUP BY
		mr.yuefen
	) SELECT
	mc.yuefen,
	( mc.price / mr.MonthlyRevenue ) AS Monthly_cost_rate
FROM
	Monthly_cost mc
	JOIN Monthly_revenue mr ON mc.yuefen = mr.yuefen
WHERE
	mc.yuefen LIKE '%s'
GROUP BY
	mc.yuefen
ORDER BY
	mc.yuefen""" % (BusinessName, BusinessName, BusinessName, BusinessName, BusinessName, BusinessName, BusinessName,
                    BusinessName, (str(date) + '%'))
            df = pd.read_sql(query, con=conn)
            return df

            print("Data inserted successfully!")
        except Exception as error:
            print("Error while inserting data:", error)

    def get_total_monthly_profit_rate(self, date, BusinessName):
        """按月计算全店月利润率"""
        try:
            conn = self.conn

            check_result, BusinessName = self.check_businessname(BusinessName)
            if not check_result:
                return BusinessName

            query = """WITH Monthly_Inventory_List AS (
	SELECT
		DATE_FORMAT( mp.ClassName, '%%Y-%%m' ) AS yuefen,
		SUM( ms.CheckTotle ) AS price
	FROM
		mall_stockcheck ms
		JOIN mall_period mp ON ms.InPeriod = mp.ID
	WHERE
		ms.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' )
	GROUP BY
		yuefen
	),
	Monthly_Purchase_Order AS (
	SELECT
		DATE_FORMAT( mp.ClassName, '%%Y-%%m' ) AS yuefen,
		SUM( ms.TotleMoney ) AS price
	FROM
		mall_purchaseorder ms
		JOIN mall_period mp ON ms.InPeriod = mp.ID
	WHERE
		ms.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' )
	GROUP BY
		yuefen
	),
	Last_Monthly_Inventory_List AS (
	SELECT
		DATE_FORMAT( DATE_ADD( mp.ClassName, INTERVAL 1 MONTH ), '%%Y-%%m' ) AS next_month,
		SUM( ms.CheckTotle ) AS price
	FROM
		mall_stockcheck ms
		JOIN mall_period mp ON ms.InPeriod = mp.ID
	WHERE
		ms.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' )
	GROUP BY
		next_month
	),
	Monthly_cost AS (
	SELECT
		mit1.yuefen AS yuefen,
		(- mit1.price + IFNULL( mpd.price, 0 ) + IFNULL( mit2.price, 0 ) ) AS price
	FROM
		Monthly_Inventory_List mit1
		LEFT JOIN Monthly_Purchase_Order mpd ON mit1.yuefen = mpd.yuefen
		LEFT JOIN Last_Monthly_Inventory_List mit2 ON mit1.yuefen = mit2.next_month
	GROUP BY
		mit1.yuefen
	),
	Monthly_Order_revenue AS (
/* 月订单营收 */
	SELECT
		DATE_FORMAT( mp.ClassName, '%%Y-%%m' ) AS yuefen,
		SUM( mo.PaidMoney ) AS OrderRevenue
	FROM
		mall_order mo
		JOIN mall_period mp ON mo.InPeriod = mp.ID
	WHERE
		mo.PayState <= 5000
		AND mo.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' )
	GROUP BY
		yuefen
	),
	Integralcost AS (
/* 积分成本 */
	SELECT
		DATE_FORMAT( mp.ClassName, '%%Y-%%m' ) AS yuefen,
		'积分' AS ValueName,
		( ( SUM( me.ThisRemain ) - SUM( me.PreRemain )) * 1.2 ) AS price
	FROM
		mall_everydayvalue AS me
		JOIN mall_leaguervaluetype AS ml ON me.Project = ml.ID
		JOIN mall_period mp ON me.InPeriod = mp.ID
	WHERE
		ml.ValueName LIKE '积分'
		AND me.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' )
	GROUP BY
		yuefen
	),
	Lotterycost AS (
/* 彩票成本 */
	SELECT
		DATE_FORMAT( mp.ClassName, '%%Y-%%m' ) AS yuefen,
		'彩票' AS ValueName,
		( ( ( SUM( me.ThisRemain ) - SUM( me.PreRemain )) / 1000 ) * 1 ) AS price
	FROM
		mall_everydayvalue AS me
		JOIN mall_leaguervaluetype AS ml ON me.Project = ml.ID
		JOIN mall_period mp ON me.InPeriod = mp.ID
	WHERE
		ml.ValueName LIKE '彩票'
		AND me.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' )
	GROUP BY
		yuefen
	),
	Tokencost AS (
/* 代币成本 */
	SELECT
		DATE_FORMAT( mp.ClassName, '%%Y-%%m' ) AS yuefen,
		'代币' AS ValueName,
		( ( SUM( me.ThisRemain ) - SUM( me.PreRemain )) * 0.185 ) AS price
	FROM
		mall_everydayvalue AS me
		JOIN mall_leaguervaluetype AS ml ON me.Project = ml.ID
		JOIN mall_period mp ON me.InPeriod = mp.ID
	WHERE
		ml.ValueName LIKE '代币'
		AND me.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' )
	GROUP BY
		yuefen
	),
	Dollcost AS (
/* 娃娃成本 */
	SELECT
		DATE_FORMAT( mp.ClassName, '%%Y-%%m' ) AS yuefen,
		'娃娃' AS ValueName,
		( ( SUM( me.ThisRemain ) - SUM( me.PreRemain )) * 0.185 ) AS price
	FROM
		mall_everydayvalue AS me
		JOIN mall_leaguervaluetype AS ml ON me.Project = ml.ID
		JOIN mall_period mp ON me.InPeriod = mp.ID
	WHERE
		ml.ValueName LIKE '娃娃'
		AND me.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' )
	GROUP BY
		yuefen
	),
	Monthly_revenue AS (
/* 计算月营收 */
	SELECT
		mr.yuefen AS yuefen,
		(
		IFNULL( mr.OrderRevenue, 0 ) - IFNULL( ic.price, 0 ) - IFNULL( lc.price, 0 ) - IFNULL( tc.price, 0 ) - IFNULL( dc.price, 0 )) AS MonthlyRevenue
	FROM
		Monthly_Order_revenue mr
		JOIN Integralcost ic ON mr.yuefen = ic.yuefen
		JOIN Lotterycost lc ON mr.yuefen = lc.yuefen
		JOIN Tokencost tc ON mr.yuefen = tc.yuefen
		JOIN Dollcost dc ON mr.yuefen = dc.yuefen
	GROUP BY
		mr.yuefen
	) SELECT
	mc.yuefen,
	( 1 - ( mc.price / mr.MonthlyRevenue ) ) AS Monthly_profit_margin
FROM
	Monthly_cost mc
	JOIN Monthly_revenue mr ON mc.yuefen = mr.yuefen
WHERE
	mc.yuefen LIKE '%s'
GROUP BY
	mc.yuefen
ORDER BY
	mc.yuefen""" % (BusinessName, BusinessName, BusinessName, BusinessName, BusinessName, BusinessName, BusinessName,
                    BusinessName, (str(date) + '%'))
            df = pd.read_sql(query, con=conn)
            return df

            print("Data inserted successfully!")
        except Exception as error:
            print("Error while inserting data:", error)

    def get_total_monthly_income(self, date, BusinessName):
        """按月计算全店总营收或月收入"""
        try:
            conn = self.conn

            check_result, BusinessName = self.check_businessname(BusinessName)
            if not check_result:
                return BusinessName

            query = """WITH Monthly_Order_revenue AS (
/* 月订单营收 */
	SELECT
		DATE_FORMAT( mp.ClassName, '%%Y-%%m' ) AS yuefen,
		SUM( mo.PaidMoney ) AS OrderRevenue
	FROM
		mall_order mo
		JOIN mall_period mp ON mo.InPeriod = mp.ID
	WHERE
		mo.PayState <= 5000
		AND mo.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' )
	GROUP BY
		yuefen
	),
	Integralcost AS (
/* 积分成本 */
	SELECT
		DATE_FORMAT( mp.ClassName, '%%Y-%%m' ) AS yuefen,
		'积分' AS ValueName,
		( ( SUM( me.ThisRemain ) - SUM( me.PreRemain )) * 1.2 ) AS price
	FROM
		mall_everydayvalue AS me
		JOIN mall_leaguervaluetype AS ml ON me.Project = ml.ID
		JOIN mall_period mp ON me.InPeriod = mp.ID
	WHERE
		ml.ValueName LIKE '积分'
		AND me.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' )
	GROUP BY
		yuefen
	),
	Lotterycost AS (
/* 彩票成本 */
	SELECT
		DATE_FORMAT( mp.ClassName, '%%Y-%%m' ) AS yuefen,
		'彩票' AS ValueName,
		( ( ( SUM( me.ThisRemain ) - SUM( me.PreRemain )) / 1000 ) * 1 ) AS price
	FROM
		mall_everydayvalue AS me
		JOIN mall_leaguervaluetype AS ml ON me.Project = ml.ID
		JOIN mall_period mp ON me.InPeriod = mp.ID
	WHERE
		ml.ValueName LIKE '彩票'
		AND me.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' )
	GROUP BY
		yuefen
	),
	Tokencost AS (
/* 代币成本 */
	SELECT
		DATE_FORMAT( mp.ClassName, '%%Y-%%m' ) AS yuefen,
		'代币' AS ValueName,
		( ( SUM( me.ThisRemain ) - SUM( me.PreRemain )) * 0.185 ) AS price
	FROM
		mall_everydayvalue AS me
		JOIN mall_leaguervaluetype AS ml ON me.Project = ml.ID
		JOIN mall_period mp ON me.InPeriod = mp.ID
	WHERE
		ml.ValueName LIKE '代币'
		AND me.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' )
	GROUP BY
		yuefen
	),
	Dollcost AS (
/* 娃娃成本 */
	SELECT
		DATE_FORMAT( mp.ClassName, '%%Y-%%m' ) AS yuefen,
		'娃娃' AS ValueName,
		( ( SUM( me.ThisRemain ) - SUM( me.PreRemain )) * 0.185 ) AS price
	FROM
		mall_everydayvalue AS me
		JOIN mall_leaguervaluetype AS ml ON me.Project = ml.ID
		JOIN mall_period mp ON me.InPeriod = mp.ID
	WHERE
		ml.ValueName LIKE '娃娃'
		AND me.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' )
	GROUP BY
		yuefen
	) /* 计算月营收 */
SELECT
	mr.yuefen,
	(
	mr.OrderRevenue - COALESCE ( ic.price, 0 ) - COALESCE ( lc.price, 0 ) - COALESCE ( tc.price, 0 ) - COALESCE ( dc.price, 0 )) AS MonthlyRevenue
FROM
	Monthly_Order_revenue mr
	JOIN Integralcost ic ON mr.yuefen = ic.yuefen
	JOIN Lotterycost lc ON mr.yuefen = lc.yuefen
	JOIN Tokencost tc ON mr.yuefen = tc.yuefen
	JOIN Dollcost dc ON mr.yuefen = dc.yuefen
WHERE
	mr.yuefen LIKE '%s'
GROUP BY
	mr.yuefen
ORDER BY
	mr.yuefen""" % (BusinessName, BusinessName, BusinessName, BusinessName, BusinessName, (str(date) + '%'))
            df = pd.read_sql(query, con=conn)
            return df

            print("Data inserted successfully!")
        except Exception as error:
            print("Error while inserting data:", error)


if __name__ == '__main__':
    print(111)
    date = '2024-02-15'
    BusinessName = '安徽合肥电玩猩-合肥店'
    dwx = DwxMysqlRagUitl()
    daily_cost_rate = dwx.get_total_daily_cost(date, BusinessName)
    print(daily_cost_rate)
    #
    daily_cost = dwx.get_total_daily_cost_rate(date, BusinessName)
    print(daily_cost)
    #
    # #
    daily_profit_rate = dwx.get_total_daily_profit_rate(date, BusinessName)
    print(daily_profit_rate)
    #
    total_monthly_cost = dwx.get_total_monthly_cost('2024-02', BusinessName)
    print('total_monthly_cost :', total_monthly_cost)

    total_monthly_income = dwx.get_total_monthly_income('2024-02', BusinessName)
    print('get_total_monthly_income :', total_monthly_income)
