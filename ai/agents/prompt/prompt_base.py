MYSQL_BASE_TIPS_MESS = '''
Here are some examples of generating mysql code for a given problem.
Please generate new libraries based on the data and questions humans ask you, import the necessary libraries and make sure the code is correct.

IMPORTANT: You need to follow your coding style,
Pay attention to check whether the query statement in the execution code block can query the data correctly.

    Q: help me Query monthly sales data in 2018
    <code>
    import pymysql

    connection = pymysql.connect(
        host='your_host',
        user='your_user',
        password='your_password',
        database='your_database',
        port="your_port"
    )

    query = """select DATE_FORMAT(o.`date`, '%Y-%m') AS `month`, `sales`, `profit` FROM order_list  WHERE DATE_FORMAT(`date`, '%Y') = '2018' GROUP BY `month`"""

    df = pd.read_sql(query, con=connection)
    print(df)
    connection.close()
    </code>

'''


MYSQL_BASE_TIPS_MESS_02 = '''
问题中的 全店日总成本率 只能用以下SQL逻辑来计算，没有其他方式, 不要省略和修改SQL的逻辑条件，仅仅修改日期即可，给出代码时不要省略内容。
禁止出现以下这样的省略情况：
 query = """
 -- SQL 查询省略了，需要使用提供的SQL模板，替换EDAY的值为'2024-03-05'
"""

    Q: 1999年3月8日的全店日成本率是多少？
    <code>
    import pymysql

    connection = pymysql.connect(
        host='your_host',
        user='your_user',
        password='your_password',
        database='your_database',
        port="your_port"
    )

    query = query = """
WITH DailyCost AS (

    SELECT
        Doll7Recycle.EDAY,
        (ROUND(
            (
                (
                    case when Doll7Prize.prize_amount is null then 0 else Doll7Prize.prize_amount end
                ) - (
                    case when Doll7Recycle.recycle_amount is null then 0 else Doll7Recycle.recycle_amount end
                )
            ) * 2.55 + (
                case when Doll7Recycle.recycle_amount is null then 0 else Doll7Recycle.recycle_amount end
            ) * 1.2
        , 2) +
        ROUND(
            (
                (
                    case when Doll8Prize.prize_amount is null then 0 else Doll8Prize.prize_amount end
                ) - (
                    case when Doll8Recycle.recycle_amount is null then 0 else Doll8Recycle.recycle_amount end
                )
            ) * 5.5 + (
                case when Doll8Recycle.recycle_amount is null then 0 else Doll8Recycle.recycle_amount end
            ) * 2.4
        , 2) +
        ROUND(
            (
                (
                    case when Doll15Prize.prize_amount is null then 0 else Doll15Prize.prize_amount end
                ) - (
                    case when Doll15Recycle.recycle_amount is null then 0 else Doll15Recycle.recycle_amount end
                )
            ) * 20 + (
                case when Doll15Recycle.recycle_amount is null then 0 else Doll15Recycle.recycle_amount end
            ) * 14.4
        , 2) +
        (
            case when Lottery.TotalTicket is null then 0 else Lottery.TotalTicket end
        ) +
        (
            case when GiftMachine.total_cost is null then 0 else GiftMachine.total_cost end
        )
    ) AS total_cost
    FROM (
        SELECT mp.ClassName as EDAY, COALESCE(SUM(Amount), 0) AS recycle_amount
        FROM view_giftrecoverlogitem gf
        JOIN mall_period mp ON gf.InPeriod = mp.ID
        WHERE (GoodName = '7寸娃娃' OR GoodName = '普通娃娃')
        GROUP BY mp.ClassName
    ) AS Doll7Recycle
    LEFT JOIN (
        SELECT vm.Period as EDAY, COALESCE(SUM(OutGift), 0) AS prize_amount
        FROM view_machinereturnrate vm
        WHERE TypeName = '7寸娃娃机'
        GROUP BY vm.Period
    ) AS Doll7Prize ON Doll7Recycle.EDAY = Doll7Prize.EDAY
    LEFT JOIN (
        SELECT mp.ClassName as EDAY, COALESCE(SUM(Amount), 0) AS recycle_amount
        FROM view_giftrecoverlogitem gf
        JOIN mall_period mp ON gf.InPeriod = mp.ID
        WHERE GoodName = '8寸娃娃'
        GROUP BY mp.ClassName
    ) AS Doll8Recycle ON Doll7Recycle.EDAY = Doll8Recycle.EDAY
    LEFT JOIN (
        SELECT vm.Period as EDAY, COALESCE(SUM(OutGift), 0) AS prize_amount
        FROM view_machinereturnrate vm
        WHERE TypeName = '8寸娃娃机'
        GROUP BY vm.Period
    ) AS Doll8Prize ON Doll7Recycle.EDAY = Doll8Prize.EDAY
    LEFT JOIN (
        SELECT mp.ClassName as EDAY, COALESCE(SUM(Amount), 0) AS recycle_amount
        FROM view_giftrecoverlogitem gf
        JOIN mall_period mp ON gf.InPeriod = mp.ID
        WHERE GoodName = '15寸娃娃'
        GROUP BY mp.ClassName
    ) AS Doll15Recycle ON Doll7Recycle.EDAY = Doll15Recycle.EDAY
    LEFT JOIN (
        SELECT vm.Period as EDAY, COALESCE(SUM(OutGift), 0) AS prize_amount
        FROM view_machinereturnrate vm
        WHERE TypeName = '15寸娃娃机'
        GROUP BY vm.Period
    ) AS Doll15Prize ON Doll7Recycle.EDAY = Doll15Prize.EDAY
    LEFT JOIN (
        SELECT vm.Period as EDAY, (SUM(OutPhTicket) + SUM(OutTicket) + SUM(OutPhTicket)) / 1000 AS TotalTicket
        FROM view_machinereturnrate vm
        WHERE TypeName = '彩票机'
        GROUP BY vm.Period
    ) AS Lottery ON Doll7Recycle.EDAY = Lottery.EDAY
    LEFT JOIN (
        SELECT mp.ClassName as EDAY, SUM(ABS(gf.TotleMoney)) AS total_cost
        FROM mall_stockchangelog gf
        JOIN mall_period mp ON gf.InPeriod = mp.ID
        JOIN mall_goodbase gb ON gf.Goods = gb.ID
        WHERE gf.OrderType = '2' AND gb.GoodName NOT LIKE '7寸娃娃' AND gb.GoodName NOT LIKE '8寸娃娃'
        AND gb.GoodName NOT LIKE '普通娃娃' AND gb.GoodName NOT LIKE '红票'
        GROUP BY mp.ClassName
    ) AS GiftMachine ON Doll7Recycle.EDAY = GiftMachine.EDAY

),
DailyCoinPrice AS (

    WITH temp AS (
        SELECT Period AS EDAY, 'total_revenue' AS rname, SUM(SellMoney) AS count
        FROM view_cmsalelogdetail
        GROUP BY Period
        UNION
        SELECT Period AS EDAY, 'TotalCoin' AS rname, SUM(sale.ECoinAmount + sale.PCoinAmount + sale.GoldCoinAmount) AS count
        FROM view_cmsalelogdetail AS sale
        GROUP BY Period
        UNION
        SELECT giving.ClassName AS EDAY, 'givingcoin' AS rname, SUM(giving.CoinNumber + giving.GoldCoinNumber + giving.PhysicalCoinNumber) AS count
        FROM view_givingcoinlogquery AS giving
        GROUP BY giving.ClassName
    )
    SELECT t0.EDAY, ((SELECT count FROM temp t1 WHERE rname = 'total_revenue' and t1.EDAY = t0.EDAY) / (SELECT SUM(count) FROM temp t2 WHERE rname IN ('TotalCoin', 'givingcoin') and t2.EDAY = t0.EDAY)) AS coin_price
    FROM temp t0
    GROUP BY t0.EDAY
),
DailyCoinTotal AS (

    SELECT DATE(Period) as EDAY, SUM(InTotalCoin) AS total_coin
    FROM view_machinereturnrate
    GROUP BY DATE(Period)
)

SELECT
    DailyCost.EDAY,
    COALESCE(1 - (DailyCost.total_cost) / (DailyCoinTotal.total_coin * DailyCoinPrice.coin_price), 0) AS daily_profit_rate
FROM
    DailyCost
JOIN
    DailyCoinTotal ON DailyCost.EDAY = DailyCoinTotal.EDAY
JOIN
    DailyCoinPrice ON DailyCost.EDAY = DailyCoinPrice.EDAY
WHERE
    DailyCost.EDAY = '1999-03-08'

"""
    df = pd.read_sql(query, con=connection)
    print(df)
    connection.close()
    </code>

'''
