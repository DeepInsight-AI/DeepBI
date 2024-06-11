# -*- coding: utf-8 -*-
"""
@ author:
@ info:
@ date: 2023/10/15 16:05
"""
import json
import os
import time
import requests
from dotenv import load_dotenv
import hashlib
import base64
from ai.backend.base_config import CONFIG
from ai.backend.util.base_util import dbinfo_encode


DB_API_SECRET_KEY = os.environ.get("DB_API_SECRET_KEY", None)
# print('DB_API_SECRET_KEY : ', DB_API_SECRET_KEY)


def encrypt(text, key):
    key_hash = hashlib.sha256(key.encode()).digest()
    text_bytes = text.encode()
    encrypted_bytes = bytearray()

    for i in range(len(text_bytes)):
        encrypted_bytes.append(text_bytes[i] ^ key_hash[i % len(key_hash)])

    return base64.b64encode(encrypted_bytes).decode().replace("+", ".").replace("/", "+").replace("=", "_")


def decrypt(encrypted_text, key):
    encrypted_text = encrypted_text.replace("+", "/").replace(".", "+").replace("_", "=")
    key_hash = hashlib.sha256(key.encode()).digest()
    encrypted_bytes = base64.b64decode(encrypted_text.encode())
    decrypted_bytes = bytearray()

    for i in range(len(encrypted_bytes)):
        decrypted_bytes.append(encrypted_bytes[i] ^ key_hash[i % len(key_hash)])

    return decrypted_bytes.decode()


def make_secret(text):
    """ By default, it is the data id when it comes in, and it is the json serialized string when it goes back."""
    now_int_time = int(time.time())
    code_str = str(now_int_time) + "$$" + str(text) + "$$" + str(DB_API_SECRET_KEY)
    # print(code_str)
    return encrypt(code_str, DB_API_SECRET_KEY)
    pass


def check_secret(data_id, secret):
    try:
        code_str = decrypt(secret, DB_API_SECRET_KEY)
        # print("decode from se ", code_str)
        code_arr = code_str.split("$$")
        code_int_time = code_arr[0]
        code_data_id = code_arr[1]
        code_secret_key = code_arr[2]
        if DB_API_SECRET_KEY != code_secret_key:
            return False, 'Secret Error'
        now_int_time = time.time()
        if now_int_time - int(code_int_time) > 60:
            return False, 'Secret Overtime'
        if int(data_id) != int(code_data_id):
            return False, 'Database Error'
        return True, 'Success'
    except Exception as e:
        return False, "Error"
    pass


def decode_data_info(code):
    """Decrypt to obtain database information """
    try:
        code_str = decrypt(code, DB_API_SECRET_KEY)
        code_arr = code_str.split("$$")
        if DB_API_SECRET_KEY != code_arr[2]:
            return None
        else:
            json_data = json.loads(code_arr[1])
            return json_data
    except Exception as e:
        return None
    pass


class Main:
    def __init__(self, db_id: str):
        self.db_id = db_id
        pass

    def run(self):
        # db = "14"  # 这里是数据库id
        db = self.db_id  # 这里是数据库id
        # 生成 se 用于获取 接口权限
        from_se = make_secret(db)
        print("生成 获取 secret", from_se)
        # url = "http://127.0.0.1:4999/data_source_info/" + db + "/" + from_se

        if CONFIG.web_server_ip is not None:
            url = "http://" + CONFIG.web_server_ip + "/data_source_info/" + db + "/" + from_se
            print(url)
        else:
            return False, ' error: Not found CONFIG.web_server_ip '

        # print(url)
        url_data = requests.get(url).text
        # print('url_data :', url_data)
        json_data = json.loads(url_data)
        # print('json_data :', json_data)
        if 200 == json_data['code']:
            decode_json = decode_data_info(json_data['data'])
            # print("decode", decode_json)

            # 敏感信息隐藏
            decode_json = dbinfo_encode(decode_json)
            return True, decode_json
        else:
            # print(json_data['msg'])
            return False, json_data['msg']

    def run_decode(self):
        db = self.db_id  # database id
        # Generate se to obtain interface permissions
        from_se = make_secret(db)
        # print("Generate secret", from_se)
        # url = "http://127.0.0.1:4999/data_source_info/" + db + "/" + from_se

        if CONFIG.web_server_ip is not None:
            url = "http://" + CONFIG.web_server_ip + "/data_source_info/" + db + "/" + from_se

        else:
            print('error: Not found CONFIG.web_server_ip ')
            return False, ' error: Not found CONFIG.web_server_ip '

        # print(url)
        url_data = requests.get(url).text
        # print('url_data :', url_data)
        json_data = json.loads(url_data)
        # print('json_data :', json_data)
        if 200 == json_data['code']:
            decode_json = decode_data_info(json_data['data'])
            # print("decode : ", decode_json)
            return True, decode_json
        else:
            # print(json_data['msg'])
            return False, json_data['msg']


if __name__ == "__main__":
    db_id = str(18)
    obj = Main(db_id)
    obj.run()
