import json
import os
from dotenv import load_dotenv
from pathlib import Path

# from bi.settings import DATA_SOURCE_FILE_DIR as docker_data_source_file_dir
docker_data_source_file_dir = "/app/user_upload_files"

host_secret = 'tNGoVq0KpQ4LKr5WMIZM'
db_secret = 'aCyBIffJv2OSW5dOvREL'
user_secret = 'kdgtPvEnzGKjE44d38M3'
passwd_secret = 'D3uGSjdaHbFL1ZprkIJD'


def is_json(myjson):
    try:
        json_object = json.loads(myjson)
    except ValueError as e:
        return False
    return True


def get_upload_path():
    # 加载 .env 文件中的环境变量
    data_source_file_dir = os.environ.get("DATA_SOURCE_FILE_DIR", docker_data_source_file_dir)
    if data_source_file_dir and len(str(data_source_file_dir)) > 0:
        return str(data_source_file_dir) + '/'
    else:
        # 获取当前工作目录的路径
        current_directory = Path.cwd()
        # 获取当前工作目录的父级目录
        data_source_file_dir = str(current_directory) + '/user_upload_files/'
        return data_source_file_dir


def get_web_server_ip():
    web_server_ip = os.environ.get("WEB_SERVER", None)
    if web_server_ip and len(str(web_server_ip)) > 0:
        return str(web_server_ip)
    else:
        return None


def get_web_language():
    web_language = os.environ.get("WEB_LANGUAGE", None)
    if web_language and len(str(web_language)) > 0:
        return str(web_language)
    else:
        return 'CN'


def dbinfo_encode(json_data):
    if json_data.get('user'):
        json_data['user'] = user_secret

    if json_data.get('passwd'):
        json_data['passwd'] = passwd_secret

    if json_data.get('password'):
        json_data['password'] = passwd_secret

    if json_data.get('host'):
        json_data['host'] = host_secret

    if json_data.get('db'):
        json_data['db'] = db_secret

    if json_data.get('dbname'):
        json_data['dbname'] = db_secret

    return json_data


def dbinfo_decode(data, sql_mess):
    if sql_mess.get('user'):
        data = data.replace(user_secret, sql_mess['user'])

    if sql_mess.get('passwd'):
        data = data.replace(passwd_secret, sql_mess['passwd'])

    if sql_mess.get('password'):
        data = data.replace(passwd_secret, sql_mess['password'])

    if sql_mess.get('host'):
        data = data.replace(host_secret, sql_mess['host'])

    if sql_mess.get('db'):
        data = data.replace(db_secret, sql_mess['db'])

    if sql_mess.get('dbname'):
        data = data.replace(db_secret, sql_mess['dbname'])

    return data
