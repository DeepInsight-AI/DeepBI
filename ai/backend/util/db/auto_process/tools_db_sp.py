from datetime import datetime

import pandas as pd
import pymysql

db_info = {'host': '****', 'user': '****', 'passwd': '****', 'port': 3306,
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

class DbSpTools:
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

    def get_sp_SkuAdgroupCamapign(self, market,startdate,enddate,start_acos,end_acos,adjuest):
        # 低于 平均ACOS值 30% 以上的  campaign 广告活动
        # 建议执行的操作：预算提升30%
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            query = """
with tempacos as (
SELECT case when SUM(cost)/SUM(sales14d)<0.2 then 0.2  else SUM(cost)/SUM(sales14d) end AS avgacos
FROM amazon_campaign_reports_sp where (date between '{}' and '{}') and market='{}' and campaignStatus='ENABLED'
)
,temp2 as (
select
market,campaignName,campaignId,campaignBudgetAmount AS budget_old,campaignBudgetAmount*(1+{}) as budget_new,
(select avgacos from tempacos) as standards_acos,SUM(cost)/SUM(sales14d) as acos,
'camapign_调整:'+'{}' as beizhu,'todo' as status
from amazon_campaign_reports_sp
where (date between '{}' and '{}') and market='{}' and campaignStatus='ENABLED'
group by market,campaignName,campaignId
having SUM(cost)/SUM(sales14d) >(select avgacos from tempacos)*(1+{}) and SUM(cost)/SUM(sales14d) <(select avgacos from tempacos)*(1+{}) and SUM(sales14d)>0
)
select
*
from temp2
             """.format(startdate,enddate,market,adjuest,adjuest,startdate,enddate,market,start_acos,end_acos)
            df = pd.read_sql(query, con=conn)
            return df
            for index, row in df.iterrows():
                print(f"Index: {index}")
                print(row)
                print()  # 添加空行分隔每一行

            print("get_sp_SkuAdgroupCamapign Data query successfully!")

        except Exception as error:
            print("get_sp_SkuAdgroupCamapign Error while query data:", error)

    def get_sp_adgroup_update(self, market, startdate, enddate, start_acos, end_acos, adjuest):
        # 查找需要操作的广告组
        try:
            conn = self.conn

            # 暂时忽略了market转化 US
            query = """
with tempacos as (
SELECT case when SUM(cost)/SUM(sales14d)<0.2 then 0.2  else SUM(cost)/SUM(sales14d) end AS avgacos
FROM amazon_ad_group_reports_sp where (date between '{}' and '{}') and market='{}' and adStatus='ENABLED'
)
,temp2 as (
select
 market,adGroupName,adGroupId,
(select avgacos from tempacos) as standards_acos,SUM(cost)/SUM(sales14d) as acos,{} as adjuset,
'adgroup_调整:' as beizhu,'todo' as status
from amazon_ad_group_reports_sp
where (date between '{}' and '{}') and market='{}' and adStatus='ENABLED'
group by market,adGroupName,adGroupId
having SUM(cost)/SUM(sales14d) >(select avgacos from tempacos)*(1+{}) and SUM(cost)/SUM(sales14d) <(select avgacos from tempacos)*(1+{}) and SUM(sales14d)>0
)
select
*
from temp2
             """.format(startdate, enddate, market, adjuest, startdate, enddate, market, start_acos,
                        end_acos)
            pd.set_option('display.max_columns', None)
            pd.set_option('display.max_rows', None)
            df = pd.read_sql(query, con=conn)
            return df
            for index, row in df.iterrows():
                print(f"Index: {index}")
                print(row)
                print()

            print("get_sp_adgroup_update Data query successfully!")

        except Exception as error:
            print("get_sp_adgroup_update Error while query data:", error)
