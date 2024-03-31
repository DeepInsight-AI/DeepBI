import pymysql
import pandas as pd

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
						AND gf.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' )
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
						AND vm.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' )
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
						AND gf.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' )
					GROUP BY
						mp.ClassName
					) AS GiftMachine ON Doll7Recycle.EDAY = GiftMachine.EDAY
				WHERE
					Doll7Recycle.EDAY = '%s'
			GROUP BY
	Doll7Recycle.EDAY """ % (
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
    					FROM
    						(
    						SELECT
    							mp.ClassName AS EDAY,
    							COALESCE ( SUM( Amount ), 0 ) AS recycle_amount
    						FROM
    							view_giftrecoverlogitem gf
    							JOIN mall_period mp ON gf.InPeriod = mp.ID
    						WHERE
    							( gf.GoodName = '7寸娃娃' OR gf.GoodName = '普通娃娃' )
    							AND gf.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' )
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
    							vm.TypeName = '7寸娃娃机'
    							AND vm.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' )
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
    							AND gf.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' )
    						GROUP BY
    							mp.ClassName
    						) AS GiftMachine ON Doll7Recycle.EDAY = GiftMachine.EDAY
    					GROUP BY
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
    						GROUP BY
    							Period UNION
    						SELECT
    							Period AS EDAY,
    							'TotalCoin' AS rname,
    							SUM( sale.ECoinAmount + sale.PCoinAmount + sale.GoldCoinAmount ) AS count
    						FROM
    							view_cmsalelogdetail AS sale
    						WHERE
    							sale.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' )
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
    						SUM( InTotalCoin ) AS total_coin
    					FROM
    						view_machinereturnrate
    					WHERE
    						OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' )
    					GROUP BY
    						DATE( Period )
    					) SELECT
    					DailyCost.EDAY,
    					COALESCE (
    					 ( DailyCost.total_cost ) / ( DailyCoinTotal.total_coin * DailyCoinPrice.coin_price )) AS daily_profit_rate
    				FROM
    					DailyCost
    					JOIN DailyCoinTotal ON DailyCost.EDAY = DailyCoinTotal.EDAY
    					JOIN DailyCoinPrice ON DailyCost.EDAY = DailyCoinPrice.EDAY
    			WHERE
    	DailyCost.EDAY = '%s'""" % (
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
					FROM
						(
						SELECT
							mp.ClassName AS EDAY,
							COALESCE ( SUM( Amount ), 0 ) AS recycle_amount
						FROM
							view_giftrecoverlogitem gf
							JOIN mall_period mp ON gf.InPeriod = mp.ID
						WHERE
							( gf.GoodName = '7寸娃娃' OR gf.GoodName = '普通娃娃' )
							AND gf.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' )
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
							vm.TypeName = '7寸娃娃机'
							AND vm.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' )
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
							AND gf.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' )
						GROUP BY
							mp.ClassName
						) AS GiftMachine ON Doll7Recycle.EDAY = GiftMachine.EDAY
					GROUP BY
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
						GROUP BY
							Period UNION
						SELECT
							Period AS EDAY,
							'TotalCoin' AS rname,
							SUM( sale.ECoinAmount + sale.PCoinAmount + sale.GoldCoinAmount ) AS count
						FROM
							view_cmsalelogdetail AS sale
						WHERE
							sale.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' )
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
						SUM( InTotalCoin ) AS total_coin
					FROM
						view_machinereturnrate
					WHERE
						OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' )
					GROUP BY
						DATE( Period )
					) SELECT
					DailyCost.EDAY,
					COALESCE (
					1 - ( DailyCost.total_cost ) / ( DailyCoinTotal.total_coin * DailyCoinPrice.coin_price )) AS daily_profit_rate
				FROM
					DailyCost
					JOIN DailyCoinTotal ON DailyCost.EDAY = DailyCoinTotal.EDAY
					JOIN DailyCoinPrice ON DailyCost.EDAY = DailyCoinPrice.EDAY
			WHERE
	DailyCost.EDAY = '%s'""" % (
                BusinessName, BusinessName, BusinessName, BusinessName, BusinessName, BusinessName, BusinessName,
                BusinessName, BusinessName, BusinessName, BusinessName, BusinessName, date)
            df = pd.read_sql(query, con=conn)
            return df

            print("Data inserted successfully!")
        except Exception as error:
            print("Error while inserting data:", error)

    def get_total_daily_income(self, date, BusinessName):
        """按日计算某个门店日收入"""
        try:
            conn = self.conn

            check_result, BusinessName = self.check_businessname(BusinessName)
            if not check_result:
                return BusinessName

            query = """WITH DailyCoinPrice AS (
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
		GROUP BY
			Period UNION
		SELECT
			Period AS EDAY,
			'TotalCoin' AS rname,
			SUM( sale.ECoinAmount + sale.PCoinAmount + sale.GoldCoinAmount ) AS count
		FROM
			view_cmsalelogdetail AS sale
		WHERE
			sale.OwnedBusiness IN ( SELECT ID FROM mall_business WHERE BusinessName = '%s' )
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
	DailyCoinTotal AS ( /*日投币量*/ SELECT DATE( Period ) AS EDAY, SUM( InTotalCoin ) AS total_coin FROM view_machinereturnrate GROUP BY DATE( Period ) ) /*计算日收入*/
SELECT
	DailyCoinTotal.EDAY,
	COALESCE ( ( DailyCoinTotal.total_coin * DailyCoinPrice.coin_price ), 0 ) AS daily_cost
FROM
	DailyCoinPrice
	JOIN DailyCoinTotal ON DailyCoinPrice.EDAY = DailyCoinTotal.EDAY
WHERE
	DailyCoinTotal.EDAY = '%s'
GROUP BY
	DailyCoinTotal.EDAY""" % (BusinessName, BusinessName, BusinessName, date)
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
        """按月计算全店总收入"""
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
