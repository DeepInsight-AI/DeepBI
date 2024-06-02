import mysql.connector
import csv
from ai.backend.util.db.auto_yzj.utils.current_time import current_time


class MySQLQueryExecutor:
    def __init__(self):
        self.config = {
            'user': '————-',
            'password': '————',
            'host': '————',
            'database': '————',
            'port': '————',
            'charset': '————',
            'raise_on_warnings': True
        }
        self.cnx = None
        self.cursor = None

    def connect(self):
        """连接到MySQL数据库"""
        try:
            self.cnx = mysql.connector.connect(**self.config)
            self.cursor = self.cnx.cursor()
            print("成功连接到数据库")
        except mysql.connector.Error as err:
            print(f"连接错误: {err}")

    def disconnect(self):
        """断开数据库连接"""
        if self.cnx.is_connected():
            self.cursor.close()
            self.cnx.close()
            print("数据库连接已关闭")

    def execute_query_from_file(self, sql_file_path, cur_time=current_time(), country='ES'):
        """从文件中读取SQL查询并执行"""
        try:
            # 读取SQL文件
            with open(sql_file_path, 'r', encoding='utf-8') as file:
                sql_query = file.read()
            # 设置排序规则:ai 表示不区分大小写（as）和重音（ai）。
            self.cursor.execute("SET NAMES 'utf8mb4' COLLATE 'utf8mb4_0900_ai_ci';")
            # 设置会话变量@session_fixed_date，设置时间
            self.cursor.execute("set @session_fixed_date = %s;", (cur_time,))
            # 国家也应该作为一个参数,后续写
            self.cursor.execute("set @session_fixed_country = %s;", (country,))
            # 执行SQL查询
            self.cursor.execute(sql_query)
            return self.cursor.fetchall()
        except FileNotFoundError:
            print("SQL文件未找到")
        except mysql.connector.Error as err:
            print(f"查询错误: {err}")

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
