import pymysql
import csv
from ai.backend.util.db.auto_yzj.utils.current_time import current_time
from datetime import datetime



class MySQLQueryExecutor:
    def __init__(self):
        db_info = {'host': '192.168.5.114', 'user': 'test_deepdata', 'passwd': 'test123!@#', 'port': 3308,
                   'db': 'amazon_ads',
                   'charset': 'utf8mb4', 'use_unicode': True, }
        self.conn = self.connect(db_info)
        self.cnx = None
        self.cursor = self.conn.cursor()

    def connect(self, db_info):
        """连接到MySQL数据库"""
        try:
            conn = pymysql.connect(**db_info)
            print("Connected to amazon_mysql database!")
            return conn
        except Exception as error:
            print("Error while connecting to amazon_mysql:", error)
            return None

    def disconnect(self):
        """断开数据库连接"""
        try:
            self.conn.close()
        except Exception as error:
            print("Error while connecting to amazon_mysql:", error)
            return None

    def execute_query_from_file(self, country, cur_time):
        """从文件中读取SQL查询并执行"""
        try:
            conn = self.conn
            c_time = self.cur_time()  # 如果未提供时间，则使用当前时间

            print("查询开始时间:", c_time)  # 打印查询开始时间
            # 读取SQL文件
            amr = AmazonMysqlRagUitl()
            amr.preprocessing_sku(country,cur_time)
            # # 执行SQL查询
            # df1 = pd.read_sql(query, con=conn)
            # # return df
            # output_filename = '预处理1.csv'
            # df1.to_csv(output_filename, index=False, encoding='utf-8-sig')

            c_time = self.cur_time()  # 如果未提供时间，则使用当前时间
            print("查询结束时间:", c_time)  # 打印查询结束时间
        except FileNotFoundError:
            print("SQL文件未找到")


    def export_to_csv(self, results, csv_file_path):
        """将查询结果导出为CSV文件"""
        try:
            # 打开CSV文件
            with open(csv_file_path, mode='w', newline='', encoding='utf-8') as file:
                # 创建csv.writer对象
                writer = csv.writer(file, delimiter=',')  # 设置,为分隔符

                # 写入标题行（即列名）
                writer.writerow([i[0] for i in self.cursor.description])

                # 写入数据行
                for row in results:
                    # 将字节序列变成utf8
                    str_row = tuple(item.decode('utf-8') if isinstance(item, bytearray) else item for item in row)
                    writer.writerow(str_row)
            print(f"数据已导出到 {csv_file_path}")
        except Exception as e:
            print(f"导出CSV时发生错误: {e}")

    def cur_time(self):
        """返回当前时间的字符串表示"""
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
