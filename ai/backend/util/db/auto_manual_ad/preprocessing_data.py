from MySQLQueryExecutor import MySQLQueryExecutor
from utils.find import find_files
from utils.trans_to import csv_to_json


def preprocess_data(cur_time: str, country: str):
    # 实例化类
    executor = MySQLQueryExecutor()

    # 连接数据库
    executor.connect()

    # 替换为你的项目目录路径
    sql_files = find_files(directory='./', suffix='.sql')

    # 保存所有后缀为sql的文件名
    dic = {}
    # 打印所有找到的.sql文件路径
    for sql_file in sql_files:
        dic[sql_file] = sql_file[0:-3:] + "csv"

    try:
        for sql_file_path, csv_file_path in dic.items():
            results = executor.execute_query_from_file(sql_file_path, cur_time, country)
            # print(results)
            # 导出结果到CSV
            executor.export_to_csv(results, csv_file_path)
            # 需要生成相应的json
            csv_to_json(csv_file_path)
    finally:
        # 断开数据库连接
        executor.disconnect()
