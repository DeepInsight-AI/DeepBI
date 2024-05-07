import json
import os
# from dotenv import load_dotenv
from pathlib import Path

# from bi.settings import DATA_SOURCE_FILE_DIR as docker_data_source_file_dir

docker_data_source_file_dir = "./user_upload_files"

host_secret = '****host_secret_****'
db_secret = '*****db_secret_*****'
user_secret = '****user_secret_****'
passwd_secret = '***passwd_secret_***'


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
        # parent_directory = current_directory.parent
        data_source_file_dir = str(current_directory) + '/user_upload_files/'

        # data_source_file_dir = '/app/user_upload_files/'
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
        print(sql_mess['user'])
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


def read_json_values(data):
    if isinstance(data, dict):
        for value in data.values():
            yield from read_json_values(value)
    elif isinstance(data, list):
        for item in data:
            yield from read_json_values(item)
    else:
        yield data


def read_json_keys(data, keys=[]):
    keys = []
    # 提取值不是对象的键值对
    if isinstance(data, dict):
        for key, value in data.items():
            if not isinstance(value, (dict, list)):
                keys.append(key)
            else:
                read_json_keys(value, keys)
    elif isinstance(data, list):
        for item in data:
            read_json_keys(item, keys)
    return keys


def read_target_keyvalue(ragdoc_json_data, select_rag_list, basic_knowledge=[]):
    basic_knowledge = []
    for key, value in ragdoc_json_data.items():
        if key in select_rag_list:
            rag_name = {key: value}
            basic_knowledge.append(rag_name)
    return basic_knowledge

