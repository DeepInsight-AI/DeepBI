import json
import os
from dotenv import load_dotenv
from pathlib import Path


def is_json(myjson):
    try:
        json_object = json.loads(myjson)
    except ValueError as e:
        return False
    return True


def get_upload_path():
    # 加载 .env 文件中的环境变量
    load_dotenv('../.env')
    # 读取环境变量的值
    data_source_file_dir = os.getenv("DATA_SOURCE_FILE_DIR", None)
    if data_source_file_dir and len(str(data_source_file_dir)) > 0:
        return str(data_source_file_dir) + '/'
    else:
        # 获取当前工作目录的路径
        current_directory = Path.cwd()

        # 获取当前工作目录的父级目录
        parent_directory = current_directory.parent
        data_source_file_dir = str(parent_directory) + '/user_upload_files/'
        return data_source_file_dir


def get_web_server_ip():
    # 加载 .env 文件中的环境变量
    load_dotenv('../.env')
    # 读取环境变量的值
    web_server_ip = os.getenv("WEB_SERVER", None)
    if web_server_ip and len(str(web_server_ip)) > 0:
        return str(web_server_ip)
    else:
        return None


def deal_database_info(db_str):
    """ 检查数据库基础信息是否超限  """

    max_token = 10000
    len_db_str = len(db_str)
    if len_db_str < max_token:
        return db_str
    else:
        if db_str.get('table_desc'):
            # 第一轮，去除 字段空白注释
            for tb in db_str.get('table_desc'):
                if tb.get('field_desc'):
                    for fd in tb.get('field_desc'):
                        # 默认值
                        if fd.get('comment') == '':
                            # fd['comment'] = fd.get('name')
                            del fd['comment']
                if len(db_str) < max_token:
                    return db_str

            # 第二轮，去除 字段注释
            for tb in db_str.get('table_desc'):
                # 删除字段前，先补全table注释
                if len(tb.get('table_comment')) == 0:
                    tb['table_comment'] = '获取tables'

                if tb.get('field_desc'):
                    for fd in tb.get('field_desc'):
                        if fd.get('comment'):
                            del fd['comment']
                if len(db_str) < max_token:
                    return db_str

            pass
            # 第三轮，去除 字段
            for tb in db_str.get('table_desc'):
                if tb.get('field_desc'):
                    for fd in tb.get('field_desc'):
                        if fd.get('name'):
                            del fd['name']
                if len(db_str) < max_token:
                    return db_str

            if len(db_str) < max_token:
                print('所选数据超过最大长度 %s' % max_token, '请重新输入')


if __name__ == '__main__':
    pass
