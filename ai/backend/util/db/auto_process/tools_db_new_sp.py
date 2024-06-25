import pymysql
import pandas as pd


class DbNewSpTools:
    def __init__(self):
        db_info = {'host': '192.168.5.114', 'user': 'test_deepdata', 'passwd': 'test123!@#', 'port': 3308,
                   'db': 'test_amazon_log',
                   'charset': 'utf8mb4', 'use_unicode': True, }
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

    def update_sp_campaign(self,market,campaign_name,campaign_id,budget_old,budget_new,standards_acos,acos,beizhu,status,update_time):
        try:
            conn = self.conn
            cursor = conn.cursor()
            query = "INSERT INTO amazon_campaign_update (market, campaign_name, campaign_id, budget_old, budget_new, standards_acos, acos, beizhu, status, update_time) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            values = (market, campaign_name, campaign_id, budget_old, budget_new, standards_acos, acos, beizhu, status,update_time)
            cursor.execute(query, values)
            conn.commit()
            print("Record inserted successfully into amazon_campaign_update table")
        except Exception as e:
            print(f"Error occurred: {e}")

    # 新建广告活动
    def create_sp_campaigin(self,market,portfolioId,endDate,campaign_name,campaign_id,targetingType,state,startDate,budgetType,budget,operation_state,create_time,campaign_type,costType):
        try:
            conn = self.conn
            cursor = conn.cursor()
            query = "INSERT INTO amazon_campaign_create (market,portfolioId,endDate,campaign_name,campaign_id,targetingType,state,startDate,budgetType,budget,operation_state,create_time,campaign_type,costType) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            values = (market,portfolioId,endDate,campaign_name,campaign_id,targetingType,state,startDate,budgetType,budget,operation_state,create_time,campaign_type,costType)
            cursor.execute(query, values)
            conn.commit()
            print("Record inserted successfully into create_sp_campaigin table")
        except Exception as e:
            print(f"Error occurred when into create_sp_campaigin: {e}")

    # 新建广告组
    def create_sp_adgroups(self,market,campaignId,adGroupName,adGroupId,state,defaultBid,adGroupState,update_time,creativeType,adGroupType):
        try:
            conn = self.conn
            cursor = conn.cursor()
            query = "INSERT INTO amazon_adgroups_create (market,campaignId,adGroupName,adGroupId,state,defaultBid,adGroupState,update_time,creativeType,adGroupType) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            values = (market,campaignId,adGroupName,adGroupId,state,defaultBid,adGroupState,update_time,creativeType,adGroupType)
            cursor.execute(query, values)
            conn.commit()
            print("Record inserted successfully into amazon_adgroups_create table")
        except Exception as e:
            print(f"Error occurred when into amazon_adgroups_create table: {e}")

    def update_sp_adgroups(self,market,adGroupName,adGroupId,bid_old,bid_new,standards_acos,acos,beizhu,status,update_time):
        try:
            conn = self.conn
            cursor = conn.cursor()
            query = "INSERT INTO amazon_adgroups_update (market, adGroupName, adGroupId, bid_old, bid_new, standards_acos, acos, beizhu, status, update_time) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            values = (market,adGroupName,adGroupId,bid_old,bid_new, standards_acos, acos, beizhu, status,update_time)
            cursor.execute(query, values)
            conn.commit()
            print("Record inserted successfully into amazon_adgroups_update table")
        except Exception as e:
            print(f"Error occurred: {e}")

    # 对应新增品的记录log
    def create_sp_product(self,market,campaignId,asin,sku,adGroupId,adId,status,update_time,productType):
        try:
            conn = self.conn
            cursor = conn.cursor()
            query = "INSERT INTO amazon_product_create (market, campaignId, asin, sku, adGroupId, adId,status, update_time,productType) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            values = (market,campaignId,asin,sku,adGroupId, str(adId), status,update_time,productType)
            cursor.execute(query, values)
            conn.commit()
            print("Record inserted successfully into amazon_product_create table")
        except Exception as e:
            print(f"Error occurred when into amazon_product_create: {e}")

    # 对应修改品的记录log
    def update_sp_product(self, market, adId, state_new,status, update_time):
        try:
            conn = self.conn
            cursor = conn.cursor()
            query = "INSERT INTO amazon_product_update (market, adId, state_new, status, update_time) VALUES (%s, %s, %s, %s, %s)"
            values = (market, adId, state_new, status, update_time)
            cursor.execute(query, values)
            conn.commit()
            print("Record inserted successfully into amazon_product_update table")
        except Exception as e:
            print(f"Error occurred when into amazon_product_update: {e}")

    # sp广告组新增关键词
    def add_sp_keyword_toadGroup(self,market,keywordId,campaignId,matchType,state,bid,adGroupId,keywordText,keywordText_new,operation_state,create_time):
        try:
            conn = self.conn
            cursor = conn.cursor()
            query = "INSERT INTO amazon_keyword_create (market,keywordId,campaignId,matchType,state,bid,adGroupId,keywordText_old,keywordText_new,operation_state,create_time) VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            values = (market,keywordId,campaignId,matchType,state,bid,adGroupId,keywordText,keywordText_new,operation_state,create_time)
            cursor.execute(query, values)
            conn.commit()
            print("Record inserted successfully into amazon_keyword_create table")
        except Exception as e:
            print(f"Error occurred when into amazon_keyword_create: {e}")

    # sp广告组关键词调整
    def update_sp_keyword_toadGroup(self,market,keywordId,state,bid,operation_state,create_time):
        try:
            conn = self.conn
            cursor = conn.cursor()
            query = "INSERT INTO amazon_keyword_update (market,keywordId,state,bid,operation_state,create_time) VALUES (%s,%s, %s, %s, %s, %s)"
            values = (market,keywordId,state,bid,operation_state,create_time)
            cursor.execute(query, values)
            conn.commit()
            print("Record inserted successfully into update_sp_keyword_toadGroup table")
        except Exception as e:
            print(f"Error occurred when into update_sp_keyword_toadGroup: {e}")

    # sp广告系列的placement更新
    def update_sp_campaign_placement(self,market,campaignId,placement,percentage_old,percentage_new,status,update_time):
        try:
            conn = self.conn
            cursor = conn.cursor()
            query = "INSERT INTO amazon_campaign_placement_update (market,campaignId,placement,percentage_old,percentage_new,status,update_time) VALUES (%s,%s, %s, %s, %s, %s, %s)"
            values = (market,campaignId,placement,percentage_old,percentage_new,status,update_time)
            cursor.execute(query, values)
            conn.commit()
            print("Record inserted successfully into amazon_campaign_placement_update table")
        except Exception as e:
            print(f"Error occurred when into amazon_campaign_placement_update: {e}")


    # sp广告系列新增negativeKeyword
    def add_sp_campaign_negativeKeyword(self,market,adGroupName,adGroupId,campaignId,campaignName,matchType,keyword_state,keywordText,keywordText_new,campaignNegativeKeywordId,operation_state,update_time):
        try:
            conn = self.conn
            cursor = conn.cursor()
            query = "INSERT INTO amazon_negative_keyword_create (market,adGroupName,adGroupId,campaignId,campaignName,matchType,keyword_state,keywordText,keywordText_new,campaignNegativeKeywordId,operation,operation_state,update_time) VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s,%s, 'campaign_add', %s, %s)"
            values = (market,adGroupName,adGroupId,campaignId,campaignName,matchType,keyword_state,keywordText,keywordText_new,campaignNegativeKeywordId,operation_state,update_time)
            cursor.execute(query, values)
            conn.commit()
            print("Record inserted successfully into amazon_negative_keyword_create table")
        except Exception as e:
            print(f"Error occurred when into amazon_negative_keyword_create: {e}")

    # sp广告系列更改negativeKeyword
    def update_sp_campaign_negativeKeyword(self, market,keyword_state, keywordText, campaignNegativeKeywordId, operation_state,update_time):
        try:
            conn = self.conn
            cursor = conn.cursor()
            query = "INSERT INTO amazon_negative_keyword_update (market,keyword_state,keywordText,campaignNegativeKeywordId,operation,operation_state,update_time) VALUES (%s,%s, %s, %s, 'campaign_update', %s, %s)"
            values = (market,keyword_state,keywordText,campaignNegativeKeywordId,operation_state,update_time)
            cursor.execute(query, values)
            conn.commit()
            print("Record inserted successfully into amazon_negative_keyword_update table")
        except Exception as e:
            print(f"Error occurred when into amazon_negative_keyword_update: {e}")

    # sp广告组新增negativeKeyword
    def add_sp_adGroup_negativeKeyword(self, market, adGroupName, adGroupId, campaignId, campaignName, matchType,
                                        keyword_state, keywordText, operation_state,
                                        update_time):
        try:
            conn = self.conn
            cursor = conn.cursor()
            query = "INSERT INTO amazon_negative_keyword_create (market,adGroupName,adGroupId,campaignId,campaignName,matchType,keyword_state,keywordText,operation,operation_state,update_time) VALUES (%s, %s, %s, %s, %s,%s, %s,%s, 'addGroup_add', %s, %s)"
            values = (market, adGroupName, adGroupId, campaignId, campaignName, matchType, keyword_state, keywordText, operation_state, update_time)
            cursor.execute(query, values)
            conn.commit()
            print("Record inserted successfully into amazon_negative_keyword_create table")
        except Exception as e:
            print(f"Error occurred when into amazon_negative_keyword_create: {e}")

    # sp广告组更改negativeKeyword
    def update_sp_adGroup_negativeKeyword(self, market, keyword_state, keywordText, campaignNegativeKeywordId,
                                           operation_state, update_time):
        try:
            conn = self.conn
            cursor = conn.cursor()
            query = "INSERT INTO amazon_negative_keyword_update (market,keyword_state,keywordText,campaignNegativeKeywordId,operation,operation_state,update_time) VALUES (%s,%s, %s, %s, 'adGroup_update', %s, %s)"
            values = (market, keyword_state, keywordText, campaignNegativeKeywordId, operation_state, update_time)
            cursor.execute(query, values)
            conn.commit()
            print("Record inserted successfully into amazon_negative_keyword_update table")
        except Exception as e:
            print(f"Error occurred when into amazon_negative_keyword_update: {e}")

    def add_sd_adGroup_Targeting(self, market,adGroupId,bid,expression_type,state,expression,targetingType,targetingState, update_time):
        try:
            conn = self.conn
            cursor = conn.cursor()
            query = "INSERT INTO amazon_targeting_create (market,adGroupId,bid,expressionType,state,expression,targetingType,targetingState,update_time) VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s)"
            values = (market,adGroupId,bid,expression_type,state,expression,targetingType,targetingState, update_time)
            cursor.execute(query, values)
            conn.commit()
            print("Record inserted successfully into amazon_targeting_create table")
        except Exception as e:
            print(f"Error occurred when into amazon_targeting_create: {e}")

    def update_sd_adGroup_Targeting(self, market,adGroupId,bid,state,expression,targetingType,targetingState, update_time):
        try:
            conn = self.conn
            cursor = conn.cursor()
            query = "INSERT INTO amazon_targeting_update (market,adGroupId,bid,state,expression,targetingType,targetingState,update_time) VALUES (%s,%s, %s, %s, %s, %s, %s, %s)"
            values = (market,adGroupId,bid,state,expression,targetingType,targetingState, update_time)
            cursor.execute(query, values)
            conn.commit()
            print("Record inserted successfully into amazon_targeting_create table")
        except Exception as e:
            print(f"Error occurred when into amazon_targeting_create: {e}")



