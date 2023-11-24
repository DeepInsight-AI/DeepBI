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
    data_source_file_dir = os.environ.get("DATA_SOURCE_FILE_DIR", None)
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


