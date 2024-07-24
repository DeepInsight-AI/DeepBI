import os
import psycopg2

# 创建数据库连接引擎，URL 形式
DEEPBI_DATABASE_URL = os.environ.get("DEEPBI_DATABASE_URL", "postgresql://postgres@postgres/postgres")
print('DEEPBI_DATABASE_URL : ', DEEPBI_DATABASE_URL)


class PsgReport:
    def connect(self):
        try:
            conn = psycopg2.connect(DEEPBI_DATABASE_URL)
            print("Connected to PostgreSQL database!")
            return conn
        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL:", error)
            return None

    def select_data(self, data_id):
        try:
            conn = self.connect()
            cursor = conn.cursor()
            cursor.execute(
                """SELECT * FROM data_dashboard_file  WHERE id = """ + str(data_id) + """ and is_generate = 0 """)
            rows = cursor.fetchall()
            cursor.close()
            conn.close()
            return rows
        except (Exception, psycopg2.Error) as error:
            print("Error while fetching data:", error)
            return None

    # 更新数据
    def update_data(self, data):
        try:
            conn = self.connect()
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE data_dashboard_file
                SET is_generate = %s
                WHERE id = %s
            """, data)
            conn.commit()
            print("Data updated successfully!")
            cursor.close()
            conn.close()
            return True
        except (Exception, psycopg2.Error) as error:
            print("Error while updating data:", error)
            return False

    def update_html_name(self, data):
        try:
            conn = self.connect()
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE data_dashboard_file
                SET html_name = %s
                WHERE id = %s
            """, data)
            conn.commit()
            print("Data updated successfully!")
            cursor.close()
            conn.close()
            return True
        except (Exception, psycopg2.Error) as error:
            print("Error while updating data:", error)
            return False
