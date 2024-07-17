import pymysql
import pandas as pd


class DbTools:
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

    def update_sku_price(self,market,sku,asin,old_price,new_price,operation_state,update_time,check_price,check_time):
        try:
            conn = self.conn
            cursor = conn.cursor()
            query = "INSERT INTO amazon_sku_price_update (market, sku, asin, old_price, new_price, operation_state, update_time, check_price, check_time) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            values = (market, sku, asin, old_price, new_price, operation_state, update_time, check_price, check_time)
            cursor.execute(query, values)
            conn.commit()
            # 获取自增 id
            inserted_id = cursor.lastrowid
            print(f"Record inserted successfully into amazon_sku_price_update table with id: {inserted_id}")
            return inserted_id
        except Exception as e:
            print(f"Error occurred: {e}")
            return None

    def check_sku_price(self,check_price,check_time,id):
        try:
            conn = self.conn
            cursor = conn.cursor()
            query = "UPDATE  amazon_sku_price_update SET check_price = %s, check_time = %s WHERE id = %s"
            values = (check_price, check_time, id)
            cursor.execute(query, values)
            conn.commit()
            print("Record update successfully into amazon_sku_price_update table")
        except Exception as e:
            print(f"Error occurred: {e}")
