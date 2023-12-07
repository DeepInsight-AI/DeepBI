import os
import psycopg2

# 创建数据库连接引擎，URL 形式
HOLMES_DATABASE_URL = os.environ.get("HOLMES_DATABASE_URL", None)
print('HOLMES_DATABASE_URL : ', HOLMES_DATABASE_URL)


class PsgReport:
    def connect(self):
        try:
            conn = psycopg2.connect(HOLMES_DATABASE_URL)
            print("Connected to PostgreSQL database!")
            return conn
        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL:", error)
            return None

    # 插入数据
    def insert_data(self, data):
        try:
            conn = self.connect()
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO date_report_file (column1, column2, column3)
                VALUES (%s, %s, %s)
            """, data)
            conn.commit()
            print("Data inserted successfully!")
            cursor.close()
        except (Exception, psycopg2.Error) as error:
            print("Error while inserting data:", error)

    # 查询数据
    def select_data(self):
        try:
            conn = self.connect()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM your_table_name")
            rows = cursor.fetchall()
            cursor.close()
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
                UPDATE date_report_file
                SET is_generate = %s
                WHERE id = %s
            """, data)
            conn.commit()
            print("Data updated successfully!")
            cursor.close()
            conn.close()
        except (Exception, psycopg2.Error) as error:
            print("Error while updating data:", error)


# 示例用法
if __name__ == "__main__":
    connection = connect()

    # 假设你有要插入的数据
    data_to_insert = ('value1', 'value2', 'value3')
    insert_data(connection, data_to_insert)

    # 查询数据
    rows = select_data(connection)
    if rows:
        for row in rows:
            print(row)

    # 更新数据
    data_to_update = ('updated_value1', 'updated_value2', 'updated_value3', 'condition_value')
    update_data(connection, data_to_update)

    # 删除数据
    delete_data(connection, 'condition_value')

    connection.close()  # 关闭数据库连接
